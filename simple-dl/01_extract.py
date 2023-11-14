import argparse
from datetime import datetime
import logging
import sqlite3
import time

import orjson
from isal import isal_zlib
from tqdm import tqdm

from export_config import OWNER_BLACKLIST, REGEX_BLACKLIST


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract IDs from dl.py DB")
    parser.add_argument("input_db", help="DB from dl.py")
    parser.add_argument("output_db", help="DB to write extracted data to")
    args = parser.parse_args()

    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger_filename = f"simple_dl_errors_{now}.log"
    print(f"Logging to {logger_filename}\n")
    logging.basicConfig(filename=logger_filename)
    logger = logging.getLogger(__name__)


class Db:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.c = self.conn.cursor()

    def close(self):
        self.c.close()
        self.conn.close()


class InputDb(Db):
    def get_true_parent_folders(self):
        # Find folders which are not the children of any other folders
        print("Finding parent folders (this could take a while)")
        now = time.monotonic()
        res = self.c.execute("""
            SELECT parent_id FROM hierarchy
            EXCEPT
            SELECT child_id FROM hierarchy
        """)
        print(f"  Done executing query: {time.monotonic() - now:.3f} seconds")
        return [r[0] for r in res]   # Don't use fetchall to avoid unnecessary list creation

    def load_metadata(self, id):
        m = self.c.execute(f"SELECT metadata FROM metadata WHERE id = '{id}' LIMIT 1")
        return orjson.loads(isal_zlib.decompress(m.fetchone()[0]))

    def load_children(self, id):
        c = self.c.execute(f"SELECT child_id FROM hierarchy WHERE parent_id = '{id}'")
        return [e[0] for e in c]


class OutputDb(Db):
    def __init__(self, filename, batch_size=10000):
        super().__init__(filename)

        self.queue = []
        self.batch_size = batch_size

        self.c.execute("""
            CREATE TABLE IF NOT EXISTS data(
              id           TEXT PRIMARY KEY NOT NULL,
              name         TEXT,
              extension    TEXT,
              mime_type    TEXT,
              size         TEXT,
              resource_key TEXT,
              version      TEXT
            );
        """)

    def add(self, data):
        self.queue.append(data)
        if len(self.queue) >= self.batch_size:
            self.flush()

    def flush(self):
        self.c.executemany("INSERT OR REPLACE INTO data VALUES (?,?,?,?,?,?,?)", self.queue)
        self.conn.commit()
        self.queue = []

    def close(self):
        if self.queue:
            print(f"  Output DB: flushing {len(self.queue)} item(s) before close")
            self.flush()
        super().close()


class Visitor:
    def __init__(self, in_db, out_db):
        self.in_db = in_db
        self.out_db = out_db
        self.stack_set = set()  # Track IDs seen so far in this traversal to avoid loops

    def visit(self, item):
        for owner in item["owners"]:
            if "emailAddress" in owner:
                owner_email_address = owner["emailAddress"]
                if owner_email_address in OWNER_BLACKLIST:
                    return
                for regex in REGEX_BLACKLIST:
                    if regex.search(owner_email_address):
                        return

        item_id = item["id"]
        mime_type = item["mimeType"]
        if mime_type == "application/vnd.google-apps.folder":
            if item_id in self.stack_set:   # The folder we're about to visit the children of was already visited before
                logger.warning(f"Loop detected for {item_id}")
                return

            self.stack_set.add(item_id)

            for child_id in self.in_db.load_children(item_id):
                child = self.in_db.load_metadata(child_id)
                self.visit(child)

            self.stack_set.remove(item_id)
        elif mime_type == "application/vnd.google-apps.shortcut":
            # Shortcuts aren't files that can be downloaded, I think
            # Even if they were, we already have all the metadata so there shouldn't be anything to download
            pass
        else:
            self.out_db.add((
                item_id,
                item["name"],
                item.get("fullFileExtension"),
                mime_type,
                item.get("size"),
                item.get("resourceKey"),
                item["version"],
            ))


if __name__ == "__main__":
    in_db  = InputDb(args.input_db)
    out_db = OutputDb(args.output_db)
    visitor = Visitor(in_db, out_db)
    true_parent_folders = in_db.get_true_parent_folders()
    orphans = []

    print("Processing DB")
    print("  Progress bar counts the number of top-level folders, so each item may take a long time")
    print("  Detailed progress can be tracked by `SELECT COUNT(*)`ing the number of rows in the `data` table of the output DB")
    print("  Progress bar description shows current folder/ID being processed\n")

    while true_parent_folders:
        with tqdm(total=len(true_parent_folders), unit="id") as pbar:
            for id in true_parent_folders:
                item = None
                try:
                    item = in_db.load_metadata(id)
                except:
                    children = in_db.load_children(id)
                    orphans.extend(children)
                    logger.error(f"Failed to load metadata for parent folder: {id}, orphans: {len(children)}")
                    pbar.set_description_str(f"Failed: {item['name']} ({item['id']})", refresh=False)
                    pbar.update(1)
                    continue

                pbar.set_description_str(f"{item['name']} ({item['id']})")      # Hopefully doesn't cause too much refreshing
                visitor.visit(item)
                pbar.update(1)

        print("Done, orphans to process: ", len(orphans))

        true_parent_folders = orphans
        orphans = []

    print("\nDone, closing DBs")
    in_db.close()
    out_db.close()

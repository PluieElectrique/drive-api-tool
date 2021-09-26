import argparse
import json
import sqlite3
import zlib

from tqdm import tqdm

from export_config import OWNER_BLACKLIST


class DB:
    def __init__(self, db_path):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()

    def total_ids(self):
        self.cur.execute("SELECT COUNT(*) FROM metadata")
        return self.cur.fetchone()[0]

    def total_ids_0B(self):
        self.cur.execute("SELECT COUNT(*) FROM metadata WHERE id >= '0B' and id < '0C'")
        return self.cur.fetchone()[0]

    def get_all_ids(self, batch_size=50000):
        id_total = self.total_ids()
        for offset in range(0, id_total, batch_size):
            self.cur.execute(
                f"SELECT id FROM metadata LIMIT {batch_size} OFFSET {offset}"
            )
            # fetchall returns a list, so it's safe to yield and allow other queries to happen
            for id in self.cur.fetchall():
                yield id[0]

    def load_metadata(self, id):
        self.cur.execute(f"SELECT metadata FROM metadata WHERE id = '{id}' LIMIT 1")
        return json.loads(zlib.decompress(self.cur.fetchone()[0]))

    def load_children(self, id):
        self.cur.execute(f"SELECT child_id FROM hierarchy WHERE parent_id = '{id}'")
        return [e[0] for e in self.cur.fetchall()]

    def is_child(self, id):
        self.cur.execute(f"SELECT 1 FROM hierarchy WHERE child_id = '{id}' LIMIT 1")
        return self.cur.fetchone() is not None


class Recurse:
    def __init__(self, db):
        self.db = db

        self.processed_id = set()
        self.processed_id_0B = set()
        self.skipped_id = set()
        self.skipped_id_0B = set()

    def __enter__(self):
        id_total = self.db.total_ids()
        self.pbar = tqdm(total=id_total, desc="Visit ID", unit="id")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pbar.close()

    def export(self):
        processed_all = self.processed_id | self.processed_id_0B
        return (
            len(self.processed_id),
            len(self.processed_id_0B),
            len(self.skipped_id - processed_all),
            len(self.skipped_id_0B - processed_all),
        )

    def recurse(self, id, metadata, children, blacklist=False):
        self.pbar.update(1)

        for owner in metadata["owners"]:
            if "emailAddress" in owner and owner["emailAddress"] in OWNER_BLACKLIST:
                blacklist = True
                break

        if blacklist:
            if id.startswith("0B"):
                self.skipped_id_0B.add(id)
            else:
                self.skipped_id.add(id)
        else:
            if id.startswith("0B"):
                self.processed_id_0B.add(id)
            else:
                self.processed_id.add(id)

        if metadata["mimeType"] == "application/vnd.google-apps.folder":
            for child_id in children:
                self.recurse(
                    child_id,
                    self.db.load_metadata(child_id),
                    self.db.load_children(child_id),
                    blacklist,
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count IDs by type")
    parser.add_argument("db", type=str, help="Path to database")
    args = parser.parse_args()

    with DB(args.db) as db, Recurse(db) as r:
        id_total = db.total_ids()
        id_total_0B = db.total_ids_0B()

        for id in db.get_all_ids():
            if not db.is_child(id):
                r.recurse(id, db.load_metadata(id), db.load_children(id))

        processed_id, processed_id_0B, skipped_id, skipped_id_0B = r.export()

    print()
    print("Total IDs")
    print("  Number of IDs       :", id_total)
    print("  Number of non-0B IDs:", id_total - id_total_0B)
    print("  Number of 0B IDs    :", id_total_0B)
    print("Processed IDs")
    print("  Number of non-0B IDs:", processed_id)
    print("  Number of 0B IDs    :", processed_id_0B)
    print("Blacklisted and not otherwise processed IDs")
    print("  Number of non-0B IDs:", skipped_id)
    print("  Number of 0B IDs    :", skipped_id_0B)

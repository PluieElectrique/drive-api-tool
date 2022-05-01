import argparse
import heapq
import json
import sqlite3
import zlib

from tqdm import tqdm


class OwnerStats:
    def __init__(self, name, email, top_n=10):
        self.name = name
        self.email = email
        self.top_n = top_n

        self.num_files = 0
        self.num_files_with_size = 0
        self.num_folders = 0
        self.total_size = 0
        self.top = []

    def add(self, item):
        if item["mimeType"] == "application/vnd.google-apps.folder":
            self.num_folders += 1
        else:
            self.num_files += 1

        if "size" not in item:
            return

        self.num_files_with_size += 1
        size = int(item["size"])
        self.total_size += size
        # We don't really care what happens in the case of a tie, so break it
        # by ID (which is somewhat random).
        heap_item = (size, item["id"], item["name"])
        if len(self.top) < self.top_n:
            heapq.heappush(self.top, heap_item)
        else:
            _ = heapq.heappushpop(self.top, heap_item)

    def export(self):
        top = []
        for t in sorted(self.top, reverse=True):
            top.append(
                {
                    "size": t[0],
                    "name": t[2],
                    "id": t[1],
                }
            )

        return {
            "displayName": self.name,
            "emailAddress": self.email,
            "numFiles": self.num_files,
            "numFilesWithSize": self.num_files_with_size,
            "numFolders": self.num_folders,
            "totalFileSize": self.total_size,
            "topFiles": top,
        }


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

    def get_all_metadata(self, batch_size=50000):
        id_total = self.total_ids()
        for offset in range(0, id_total, batch_size):
            self.cur.execute(
                f"SELECT metadata FROM metadata LIMIT {batch_size} OFFSET {offset}"
            )
            # fetchall returns a list, so it's safe to yield and allow other queries to happen
            for result in self.cur.fetchall():
                yield json.loads(zlib.decompress(result[0]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Report owner statistics")
    parser.add_argument("db", type=str, help="Path to database")
    parser.add_argument("output", type=str, help="Output JSON file")
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Return the top N largest files for each owner",
    )
    args = parser.parse_args()

    stats = {}

    with DB(args.db) as db:
        total_ids = db.total_ids()
        pbar = tqdm(total=total_ids, desc="Read metadata", unit="id")

        for item in db.get_all_metadata():
            # If an item has multiple owners, it will count for each owner's stats.
            for owner in item["owners"]:
                name = owner["displayName"]
                # Use permission ID as fallback
                email = owner.get("emailAddress", owner["permissionId"])

                if email not in stats:
                    stats[email] = OwnerStats(name, email, top_n=args.top)

                stats[email].add(item)

            pbar.update(1)

    stats = [s.export() for s in stats.values()]
    stats = sorted(stats, key=lambda s: s["totalFileSize"], reverse=True)
    with open(args.output, "w") as f:
        json.dump(stats, f)

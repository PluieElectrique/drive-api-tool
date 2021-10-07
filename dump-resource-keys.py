import argparse
import json
import sqlite3
import zlib

from tqdm import tqdm


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

    def total_ids_0B(self):
        self.cur.execute("SELECT COUNT(*) FROM metadata WHERE id >= '0B' and id < '0C'")
        return self.cur.fetchone()[0]

    def get_all_resource_keys(self, batch_size=25000):
        id_total = self.total_ids_0B()
        pbar = tqdm(desc="Getting resource keys", total=id_total, unit="id")
        for offset in range(0, id_total, batch_size):
            self.cur.execute(
                "SELECT id, metadata FROM metadata WHERE id >= '0B' and id < '0C'"
                f"LIMIT {batch_size} OFFSET {offset}"
            )
            data = []
            for id, metadata in self.cur.fetchall():
                metadata = json.loads(zlib.decompress(metadata))
                if "resourceKey" in metadata:
                    data.append((id, metadata["resourceKey"]))
                pbar.update(1)

            if data:
                yield data

        pbar.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export all resource keys")
    parser.add_argument("db", type=str, help="Path to database")
    parser.add_argument("output", type=str, help="Output CSV file")
    args = parser.parse_args()

    with DB(args.db) as db, open(args.output, "w") as f:
        f.write("id,resourcekey\n")
        for data in db.get_all_resource_keys():
            data_csv = "\n".join(i + "," + k for i, k in data)
            f.write(data_csv + "\n")

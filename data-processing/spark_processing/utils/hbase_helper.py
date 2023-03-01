from happybase import Connection, ConnectionPool
import os

HBASE_HOST = os.environ.get("HBASE_HOST")
HBASE_TABLE_NAME = os.environ.get("HBASE_TABLE_NAME")

class HBaseHelper:
    def __init__(self):
        self.connection_pool = ConnectionPool(size=3, host=HBASE_HOST)

    def write_to_hbase(self, rows):
        with self.connection_pool.connection() as conn:
            table = conn.table(HBASE_TABLE_NAME)
            for row in rows:
                table.put(row['row_key'], row)

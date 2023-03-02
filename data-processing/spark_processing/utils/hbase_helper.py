from happybase import Connection, ConnectionPool
import yaml

with open('config/config.yml') as f:
    config = yaml.safe_load(f)

HBASE_HOST = config['hbase']['host']
HBASE_PORT = config['hbase']['port']
HBASE_TABLE_NAME = config['hbase']['table_name']

class HBaseHelper:
    def __init__(self):
        self.connection_pool = ConnectionPool(size=3, host=HBASE_HOST)

    def write_to_hbase(self, rows):
        try:
            with self.connection_pool.connection() as conn:
                table = conn.table(HBASE_TABLE_NAME)
                with table.batch() as batch:
                    for row in rows:
                        batch.put(row['row_key'], row)
        except Exception as e:
            # Handle the exception here, e.g. by logging the error or raising a custom exception
            print(f"Error writing to HBase: {str(e)}")

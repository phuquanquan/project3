import yaml
import happybase


def read_config():
    with open('data_processing/config/config.yml', 'r') as f:
        config = yaml.safe_load(f)
    return config

class HBaseConnection:
    def __init__(self, host, port):
        hbase_config = read_config()['hbase']

        self.host = hbase_config['host']
        self.port = hbase_config['port']
        self.table_name = hbase_config['table_name']
        self.connection = None

    def connect(self):
        self.connection = happybase.Connection(self.host, self.port)
        self.connection.open()

    def close(self):
        self.connection.close()

    def get_table(self):
        return self.connection.table(self.table_name)

    def get_data(self):
        table = self.get_table()
        data = []
        for key, value in table.scan():
            data.append({'key': key, 'value': value})
        return data

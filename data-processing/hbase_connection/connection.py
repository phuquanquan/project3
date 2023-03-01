import happybase
import yaml

def read_config():
    with open("../config/config.yml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return cfg

def connect_to_hbase():
    # read configuration file
    cfg = read_config()

    # connect to HBase
    connection = happybase.Connection(cfg['hbase_host'], cfg['hbase_port'])
    connection.open()

    # create table if not exists
    table_name = cfg['hbase_table']
    families = {'log': dict()}
    if table_name not in connection.tables():
        connection.create_table(table_name, families)

    # return table instance
    return connection.table(table_name)

def save_to_hbase(table, data):
    # save data to HBase
    pass

import happybase
import yaml

def read_config():
    with open("data_processing/config/config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg

def connect_to_hbase():
    # read configuration file
    cfg = read_config()

    # connect to HBase
    connection = happybase.Connection(cfg['hbase']['host'], port=cfg['hbase']['port'])
    connection.open()

    # create table if not exists
    table_name = cfg['hbase']['table_name']
    families = {'log': dict()}
    if table_name.encode() not in connection.tables():
        connection.create_table(table_name, families)

    # return table instance
    return connection.table(table_name)

def save_to_hbase(table, data):
    # save data to HBase
    row_key = data[0]
    values = data[1]
    table.put(row_key.encode(), values)  # add encode() to row_key
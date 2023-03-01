#!/usr/bin/env python

import sys
import happybase
import yaml

# Load configuration from config.yml
with open('/data_processing/config/config.yml', 'r') as f:
    config = yaml.safe_load(f)

# Connection information for HBase
HBASE_HOST = config['hbase']['host']
HBASE_PORT = config['hbase']['port']
HBASE_TABLE_NAME = config['hbase']['table_name']

# Connect to HBase table
connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
table = connection.table(HBASE_TABLE_NAME)

for line in sys.stdin:
    # Parse log line
    parts = line.strip().split('\t')
    ip = parts[0]
    client = parts[1]
    user = parts[2]
    datetime_obj = parts[3]
    request = parts[4]
    status = parts[5]
    size = parts[6]
    referer = parts[7]
    user_agent = parts[8]

    # Create row key
    row_key = '{0}-{1}'.format(ip, datetime_obj)

    # Create dictionary of values to insert
    values = {
        'log:client': client,
        'log:user': user,
        'log:request': request,
        'log:status': status,
        'log:size': size,
        'log:referer': referer,
        'log:user_agent': user_agent
    }

    # Insert row into HBase table
    table.put(row_key, values)

# Close connection to HBase table
connection.close()

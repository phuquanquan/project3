#!/usr/bin/env python

import sys
import happybase
import yaml

# Load configuration from config.yml
with open('data_processing/config/config.yml', 'r') as f:
    config = yaml.safe_load(f)

# Connection information for HBase
HBASE_HOST = config['hbase']['host']
HBASE_PORT = config['hbase']['port']
HBASE_TABLE_NAME = config['hbase']['table_name']

# Connect to HBase table
with happybase.Connection(HBASE_HOST, port=HBASE_PORT) as connection:
    table = connection.table(HBASE_TABLE_NAME)

    for line in sys.stdin:
        # Parse log line
        parts = line.strip().split('\t')
        host = parts[0]
        client_identd = parts[1]
        user_id = parts[2]
        date_time = parts[3]
        method = parts[4]
        endpoint = parts[5]
        protocol = parts[6]
        response_code = parts[7]
        content_size = parts[8]

        # Create row key
        row_key = '{0}-{1}'.format(host, date_time)

        # Create dictionary of values to insert
        values = {
            'log:client_identd': client_identd,
            'log:user_id': user_id,
            'log:method': method,
            'log:endpoint': endpoint,
            'log:protocol': protocol,
            'log:response_code': response_code,
            'log:content_size': content_size
        }

        # Insert row into HBase table
        table.put(row_key, values)

# Close connection to HBase table (not necessary since the context manager is used)

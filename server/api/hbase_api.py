from flask import Blueprint, jsonify, request
from backend.models.hbase import HBaseConnection

hbase_api = Blueprint('hbase_api', __name__)

@hbase_api.route('/data', methods=['GET'])
def get_data():
    connection = HBaseConnection('localhost', 9090)
    connection.connect()
    table = connection.connection.table('table_name')
    data = []
    for key, value in table.scan():
        data.append({'key': key, 'value': value})
    connection.close()
    return jsonify(data)

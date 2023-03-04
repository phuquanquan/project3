from flask import Flask
from backend.api.hbase_api import hbase_api

app = Flask(__name__)
app.register_blueprint(hbase_api)

if __name__ == '__main__':
    app.run(debug=True)

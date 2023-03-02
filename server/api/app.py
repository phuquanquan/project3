from flask import Flask, jsonify
from flask_restful import Resource, Api
from flask_cors import CORS

from models.models import db, Data

app = Flask(__name__)
app.config.from_object("config.Config")
CORS(app)

api = Api(app)

db.init_app(app)


class DataAPI(Resource):
    def get(self):
        data = Data.query.all()
        return jsonify([d.serialize() for d in data])

    def post(self):
        # TODO: add logic to process incoming data
        return {"message": "Data received successfully"}, 200


api.add_resource(DataAPI, "/data")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")

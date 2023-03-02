from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Data(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(50))
    endpoint = db.Column(db.String(200))
    method = db.Column(db.String(10))
    status = db.Column(db.Integer)

    def __init__(self, timestamp, endpoint, method, status):
        self.timestamp = timestamp
        self.endpoint = endpoint
        self.method = method
        self.status = status

    def __repr__(self):
        return f"<Data {self.timestamp} {self.endpoint} {self.method} {self.status}>"

    def serialize(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "endpoint": self.endpoint,
            "method": self.method,
            "status": self.status,
        }

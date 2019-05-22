import pymongo
import motor.motor_asyncio


class MongoDBConnection(object):
    SECRETS = {
        "MONGO_HOST": "***REMOVED***",
        "MONGO_PORT": 27019,
        "MONGO_DBNAME": "staging",
        "MONGO_AUTH_SOURCE": "admin",
        "MONGO_RO_USERNAME": "***REMOVED***",
        "MONGO_RO_PASSWORD": "***REMOVED***",
        "MONGO_USERNAME": "***REMOVED***",
        "MONGO_PW": "***REMOVED***"

    }
    uri = "mongodb://{username}:{password}@{hostname}:{port}/{db}?authSource=admin&replicaSet=rs0"
    read_only = None
    db = None
    client = None

    def __init__(self, read_only=True, uri=None, db=None):
        self.read_only = read_only
        self.db = db if db is not None else self.SECRETS['MONGO_DBNAME']
        if uri is not None:
            self.uri = uri

    def __enter__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            self.uri.format(username=self.SECRETS["MONGO_USERNAME"],
                            password=self.SECRETS["MONGO_PW"],
                            hostname=self.SECRETS["MONGO_HOST"],
                            port=self.SECRETS["MONGO_PORT"],
                            db=self.db))
        return self.client[self.db]

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

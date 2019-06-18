from pymongo import MongoClient
import motor.motor_asyncio


class MongoDBConnection(object):
    SECRETS = {
        "MONGO_HOST": "***REMOVED***",
        "MONGO_PORT": 27019,
        "MONGO_DBNAME": "matchengine_test",
        # "MONGO_DBNAME": "staging",
        "MONGO_AUTH_SOURCE": "admin",
        "MONGO_RO_USERNAME": "***REMOVED***",
        "MONGO_RO_PASSWORD": "***REMOVED***",
        "MONGO_USERNAME": "***REMOVED***",
        "MONGO_PW": "***REMOVED***"

    }
    uri = "mongodb://{username}:{password}@{hostname}:{port}/{db}?authSource=admin&replicaSet=rs0&maxPoolSize=1000"
    read_only = None
    db = None
    client = None

    def __init__(self, read_only=True, uri=None, db=None, async_init=True):
        """
        Default params to use values from an external SECRETS.JSON configuration file,

        Override SECRETS_JSON values if arguments are passed via CLI
        :param read_only:
        :param uri:
        :param db:
        """
        self.read_only = read_only
        self.async_init = async_init
        self.db = db if db is not None else self.SECRETS['MONGO_DBNAME']
        if uri is not None:
            self.uri = uri

    def __enter__(self):
        username = self.SECRETS["MONGO_RO_USERNAME"] if self.read_only else self.SECRETS["MONGO_USERNAME"]
        password = self.SECRETS["MONGO_RO_PASSWORD"] if self.read_only else self.SECRETS["MONGO_PW"]
        if self.async_init:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                self.uri.format(username=username,
                                password=password,
                                hostname=self.SECRETS["MONGO_HOST"],
                                port=self.SECRETS["MONGO_PORT"],
                                db=self.db))
        else:
            self.client = MongoClient(
                self.uri.format(username=username if self.read_only else self.SECRETS['MONGO_USERNAME'],
                                password=password if self.read_only else self.SECRETS['MONGO_PW'],
                                hostname=self.SECRETS["MONGO_HOST"],
                                port=self.SECRETS["MONGO_PORT"],
                                db=self.db))
        return self.client[self.db]

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.client.close()

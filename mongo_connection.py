from typing import Union, Dict

import motor.motor_asyncio
import pymongo.database

from matchengine_types import Secrets
from plugin_stub import DBSecrets


class DefaultDBSecrets(DBSecrets):
    _secrets: Dict

    def __init__(self):
        import os, json
        secrets_json = os.getenv('SECRETS_JSON', None)
        if secrets_json is None:
            raise Exception("SECRETS_JSON not set; exiting")
        try:
            if os.path.exists(secrets_json):
                with open(secrets_json) as _f:
                    self._secrets = json.load(_f)
            else:
                self._secrets = json.loads(secrets_json)
        except Exception as e:
            print(e)
            raise Exception("SECRETS_JSON not valid json; exiting")

    def get_secrets(self) -> Secrets:
        return Secrets(HOST=self._secrets["MONGO_HOST"],
                       PORT=self._secrets["MONGO_PORT"],
                       DB=self._secrets["MONGO_DBNAME"],
                       AUTH_DB=self._secrets["MONGO_AUTH_SOURCE"],
                       RO_USERNAME=self._secrets["MONGO_RO_USERNAME"],
                       RO_PASSWORD=self._secrets["MONGO_RO_PASSWORD"],
                       RW_USERNAME=self._secrets["MONGO_USERNAME"],
                       RW_PASSWORD=self._secrets["MONGO_PASSWORD"])


class MongoDBConnection(object):
    uri = "mongodb://{username}:{password}@{hostname}:{port}/{db}?authSource=admin&replicaSet=rs0&maxPoolSize=1000"
    read_only: bool
    secrets: Secrets
    db: Union[pymongo.database.Database, motor.motor_asyncio.AsyncIOMotorDatabase]
    client = Union[pymongo.MongoClient, motor.motor_asyncio.AsyncIOMotorClient]

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
        if uri is not None:
            self.uri = uri

        if not hasattr(self, 'secrets'):
            self.secrets = DefaultDBSecrets().get_secrets()
        self.db = db if db is not None else self.secrets.DB

    def __enter__(self):
        username = self.secrets.RO_USERNAME if self.read_only else self.secrets.RW_USERNAME
        password = self.secrets.RO_PASSWORD if self.read_only else self.secrets.RW_PASSWORD
        if self.async_init:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                self.uri.format(username=username,
                                password=password,
                                hostname=self.secrets.HOST,
                                port=self.secrets.PORT,
                                db=self.secrets.DB))
        else:
            self.client = pymongo.MongoClient(
                self.uri.format(username=username,
                                password=password,
                                hostname=self.secrets.HOST,
                                port=self.secrets.PORT,
                                db=self.db))
        return self.client[self.db]

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.client.close()

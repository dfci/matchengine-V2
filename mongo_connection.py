from typing import Union

import pymongo.database
import motor.motor_asyncio

from matchengine_types import Secrets


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
        self.db = db if db is not None else self.secrets.DB
        if uri is not None:
            self.uri = uri

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

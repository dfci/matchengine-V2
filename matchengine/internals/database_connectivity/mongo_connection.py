from __future__ import annotations

from typing import TYPE_CHECKING

import motor.motor_asyncio
import pymongo.database

from matchengine.internals.plugin_helpers.plugin_stub import DBSecrets
from matchengine.internals.typing.matchengine_types import Secrets

if TYPE_CHECKING:
    from typing import (
        Union,
        Dict
    )


class DefaultDBSecrets(DBSecrets):
    _secrets: Dict

    def __init__(self):
        import os
        import json
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
        return Secrets(host=self._secrets["MONGO_HOST"],
                       port=self._secrets["MONGO_PORT"],
                       db=self._secrets["MONGO_DBNAME"],
                       auth_db=self._secrets.get("MONGO_AUTH_SOURCE", False),
                       ro_username=self._secrets.get("MONGO_RO_USERNAME", False),
                       ro_password=self._secrets.get("MONGO_RO_PASSWORD", False),
                       rw_username=self._secrets.get("MONGO_USERNAME", False),
                       rw_password=self._secrets.get("MONGO_PASSWORD", False),
                       replica_set=self._secrets.get("MONGO_REPLICASET", False),
                       max_pool_size=self._secrets.get("MONGO_MAX_POOL_SIZE", False, ),
                       min_pool_size=self._secrets.get("MONGO_MIN_POOL_SIZE", False))


class MongoDBConnection(object):
    uri = ""
    read_only: bool
    secrets: Secrets
    db: Union[pymongo.database.Database, motor.motor_asyncio.AsyncIOMotorDatabase]
    client: Union[pymongo.MongoClient, motor.motor_asyncio.AsyncIOMotorClient]

    def __init__(self, read_only=True, db=None, async_init=True):
        """
        Default params to use values from an external SECRETS.JSON configuration file,

        Override SECRETS_JSON values if arguments are passed via CLI
        :param read_only:
        :param db:
        """
        self.read_only = read_only
        self.async_init = async_init

        if not hasattr(self, 'secrets'):
            self.secrets = DefaultDBSecrets().get_secrets()
        self.db = db if db is not None else self.secrets.DB

    def __enter__(self):
        username = self.secrets.RO_USERNAME if self.read_only else self.secrets.RW_USERNAME
        password = self.secrets.RO_PASSWORD if self.read_only else self.secrets.RW_PASSWORD
        uri_params = list()
        if self.secrets.AUTH_DB:
            uri_params.append(f"authSource={self.secrets.AUTH_DB}")
        if self.secrets.REPLICA_SET:
            uri_params.append(f"replicaSet={self.secrets.REPLICA_SET}")
        if self.secrets.MAX_POOL_SIZE:
            uri_params.append(f"maxPoolSize={self.secrets.MAX_POOL_SIZE}")
        if self.secrets.MIN_POOL_SIZE:
            uri_params.append(f"minPoolSize={self.secrets.MIN_POOL_SIZE}")
        username_password_param = (f"{username if username else str()}"
                                   f"{':' if username and password else str()}"
                                   f"{password if password else str()}"
                                   f"{'@' if username or password else str()}")

        uri = (f"mongodb://{username_password_param}{self.secrets.HOST}:{self.secrets.PORT}/{self.db}"
               f"{'?' if uri_params else str()}{'&'.join(uri_params)}")
        if self.async_init:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        else:
            self.client = pymongo.MongoClient(uri)
        return self.client[self.db]

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.client.close()

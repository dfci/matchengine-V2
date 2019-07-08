import json
import os
from typing import Dict

from matchengine_types import Secrets
from plugin_stub import DBSecrets


class DFCIDBSecrets(DBSecrets):
    _secrets: Dict

    def __init__(self):
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


__export__ = ["DFCIDBSecrets"]

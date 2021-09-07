from .client import Client
from .consts import *


class OracleAPI(Client):

    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, test=False):
        Client.__init__(self, api_key, api_secret_key, passphrase, use_server_time, test)

    def oracle(self):
        return self._request_without_params(GET, GET_ORACLE)

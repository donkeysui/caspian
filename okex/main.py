from okex.websocket import subscribe_without_login, subscribe
from const import url, api_key, passphrase, secret_key
import asyncio

channels = ["swap/depth5:BTC-USD-SWAP"]

loop = asyncio.get_event_loop()
loop.run_until_complete(subscribe(url, api_key, passphrase, secret_key, channels))
loop.close()

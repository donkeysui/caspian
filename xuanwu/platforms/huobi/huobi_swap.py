# -*- coding: utf-8 -*-
"""
  @ Author:   Turkey
  @ Email:    suiminyan@gmail.com
  @ Date:     2021/9/22 10:40
  @ Description: 
  @ History:
"""
import gzip
import copy
import hmac
import base64
import time
import urllib
import hashlib
import datetime
from urllib.parse import urljoin
from xuanwu.const import USER_AGENT
from xuanwu.const import MARKET_TYPE_KLINE
from xuanwu.model.market import Orderbook, Kline, Trade
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position
from xuanwu.model.order import *
from urllib.parse import urljoin
from xuanwu.error import Error
from xuanwu.utils import tools, logger
from xuanwu.tasks import SingleTask
from xuanwu.const import HUOBI_SWAP
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.utils.decorator import async_method_locker

__all__ = ("HuobiSwapMarket", "HuobiSwapTrade",)


class HuobiSwapRestAPI:
    """ Huobi Swap REST API Client.

    Attributes:
        :param host: HTTP request host.
        :param access_key: Account's ACCESS KEY.
        :param secret_key: Account's SECRET KEY.
    """

    def __init__(self, host, access_key, secret_key):
        """ initialize REST API client. """
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_swap_info(self, contract_code=None):
        """ Get Swap Info

        Attributes:
            :param contract_code:  such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        * Note: 1. If input `contract_code`, only matching this contract code.
                2. If not input 'contract_code', matching all contract_codes.
        """
        uri = "/swap-api/v1/swap_contract_info"
        params = {}
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_price_limit(self, contract_code=None):
        """ Get swap price limit.

        Attributes:
            :param contract_code:  such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input 'contract_code', matching all contract_codes.
        """
        uri = "/swap-api/v1/swap_price_limit"
        params = {}
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_orderbook(self, contract_code):
        """ Get orderbook information.

        Attributes:
            :param contract_code:  such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-ex/market/depth"
        params = {
            "contract_code": contract_code,
            "type": "step0"
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_klines(self, contract_code, period, size=None, sfrom=None, to=None):
        """ Get kline information.

        Attributes:
            :param contract_code:  such as "BTC-USD".
            :param period: 1min, 5min, 15min, 30min, 60min,4hour,1day, 1mon
            :param size: [1,2000]
            :param sfrom: start time
            :param to: end time

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-ex/market/history/kline"
        params = {
            "contract_code": contract_code,
            "period": period
        }
        if size:
            params["size"] = size
        if sfrom:
            params["from"] = sfrom
        if to:
            params["to"] = to
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_asset_info(self, contract_code=None):
        """ Get account asset information.

        Attributes:
            :param contract_code: such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_account_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_position(self, contract_code=None):
        """ Get position information.

        Attributes:
            :param contract_code: such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_position_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_account_position(self, contract_code):
        """ Get position and account information.

        Attributes:
            :param contract_code: Currency name, e.g. BTC-USD.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_account_position_info"
        body = {"contract_code": contract_code}
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_order(self, contract_code, price, quantity, direction, offset, lever_rate, order_price_type,
                           client_order_id=None):
        """ Create an new order.

        Attributes:
            :param contract_code: such as "BTC-USD".
            :param price: Order price.
            :param quantity: Order amount.
            :param direction: Transaction direction, `buy` / `sell`.
            :param offset: `open` / `close`.
            :param lever_rate: Leverage rate, 10 or 20.
            :param order_price_type: Order type, `limit` - limit order, `opponent` - market order.
            :param client_order_id: client order id

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_order"
        body = {
            "contract_code": contract_code,
            "price": price,
            "volume": quantity,
            "direction": direction,
            "offset": offset,
            "lever_rate": lever_rate,
            "order_price_type": order_price_type
        }
        if client_order_id:
            body.update({"client_order_id": client_order_id})
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_orders(self, orders_data):
        """ Batch Create orders.
            :param orders_data:
                    {'orders_data': [
                       {
                        'contract_code':'BTC-USD',  'client_order_id':'',
                        'price':1, 'volume':1, 'direction':'buy', 'offset':'open',
                        'leverRate':20, 'orderPriceType':'limit'},
                       {
                        'contract_code':'BTC-USD', 'client_order_id':'',
                        'price':2, 'volume':2, 'direction':'buy', 'offset':'open',
                        'leverRate':20, 'orderPriceType':'limit'}
                    ]}
        """
        uri = "/swap-api/v1/swap_batchorder"
        body = orders_data
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, contract_code, order_id=None, client_order_id=None):
        """ Revoke an order.

        Attributes:
            :param contract_code: such as "BTC-USD".
            :param order_id: Order ID.
            :param client_order_id: client order id

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_cancel"
        body = {
            "contract_code": contract_code
        }
        if order_id:
            body["order_id"] = order_id
        if client_order_id:
            body["client_order_id"] = client_order_id

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_orders(self, contract_code, order_ids=None, client_order_ids=None):
        """ Revoke multiple orders.

        Attributes:
            :param contract_code: such as "BTC-USD".
            :param order_ids: Order ID list.
            :param client_order_ids: Client Order List

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_cancel"
        body = {
            "contract_code": contract_code
        }
        if order_ids:
            body["order_id"] = ",".join(order_ids)
        if client_order_ids:
            body["client_order_id"] = ",".join(client_order_ids)

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order_all(self, contract_code):
        """ Revoke all orders.

        Attributes:
            :param contract_code: such as "BTC-USD".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/swap-api/v1/swap_cancelall"
        body = {
            "contract_code": contract_code,
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_info(self, contract_code, order_ids=None, client_order_ids=None):
        """ Get order information.

        Attributes:
            :param contract_code: such as "BTC-USD".
            :param order_ids: Order ID list.
                              (different IDs are separated by ",", maximum 20 orders can be requested at one time.)
            :param client_order_ids: Client Order ID list.
                              (different IDs are separated by ",", maximum 20 orders can be requested at one time.)

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_order_info"
        body = {
            "contract_code": contract_code
        }
        if order_ids:
            body.update({"order_id": ",".join(order_ids)})
        if client_order_ids:
            body.update({"client_order_id": ",".join(client_order_ids)})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, contract_code, index=1, size=50):
        """ Get open order information.

        Attributes:
            :param contract_code: such as "BTC-USD".
            :param index: Page index, default 1st page.
            :param size: Page size, Default 20，no more than 50.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/swap-api/v1/swap_openorders"
        body = {
            "contract_code": contract_code,
            "page_index": index,
            "page_size": size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_history_orders(self, contract_code, trade_type, stype, status, create_date,
                                 page_index=0, page_size=50):
        """ Get history orders information.

        Args:
            :param contract_code: such as "BTC-USD".
            :param trade_type:
                              0:all,
                              1: buy long,
                              2: sell short,
                              3: buy short,
                              4: sell long,
                              5: sell liquidation,
                              6: buy liquidation,
                              7:Delivery long,
                              8: Delivery short
            :param stype:
                              1:All Orders,
                              2:Order in Finished Status
            :param status: status:
                              1. Ready to submit the orders;
                              2. Ready to submit the orders;
                              3. Have sumbmitted the orders;
                              4. Orders partially matched;
                              5. Orders cancelled with partially matched;
                              6. Orders fully matched;
                              7. Orders cancelled;
                              11. Orders cancelling.
            :param create_date: any positive integer available. Requesting data beyond 90 will not be supported,
                                otherwise, system will return trigger history data within the last 90 days by default.
            :param page_index: default 1st page
            :param page_size: default page size 20. 50 max.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        """
        uri = "/swap-api/v1/swap_hisorders"
        body = {
            "contract_code": contract_code,
            "trade_type": trade_type,
            "type": stype,
            "status": status,
            "create_date": create_date,
            "page_index": page_index,
            "page_size": page_size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def transfer_between_spot_swap(self,  symbol, amount, from_, to):
        """ Do transfer between spot and future.
        Attributes:
            :param symbol: currency,such as btc,eth,etc.
            :param amount: transfer amount.pls note the precision digit is 8.
            :param from_: 'spot' or 'swap'
            :param to: 'spot' or 'swap'
        """
        body = {
            "currency": symbol,
            "amount": amount,
            "from": from_,
            "to": to
        }

        uri = "https://api.huobi.pro/v2/account/transfer"
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
           params method: HTTP request method. `GET` / `POST` / `DELETE` / `PUT`.
           params uri: HTTP request uri.
           params params: HTTP query params.
           params body: HTTP request body.
           params headers: HTTP request headers.
           params auth: If this request requires authentication.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        if uri.startswith("http://") or uri.startswith("https://"):
            url = uri
        else:
            url = urljoin(self._host, uri)

        if auth:
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            params = params if params else {}
            params.update({"AccessKeyId": self._access_key,
                           "SignatureMethod": "HmacSHA256",
                           "SignatureVersion": "2",
                           "Timestamp": timestamp})

            params["Signature"] = self.generate_signature(method, params, uri)

        if not headers:
            headers = {}
        if method == "GET":
            headers["Content-type"] = "application/x-www-form-urlencoded"
            headers["User-Agent"] = USER_AGENT
            _, success, error = await AsyncHttpRequests.fetch("GET", url, params=params, headers=headers, timeout=10)
        else:
            headers["Accept"] = "application/json"
            headers["Content-type"] = "application/json"
            headers["User-Agent"] = USER_AGENT
            _, success, error = await AsyncHttpRequests.fetch("POST", url, params=params, data=body, headers=headers,
                                                              timeout=10)
        if error:
            return None, error
        if not isinstance(success, dict):
            result = json.loads(success)
        else:
            result = success
        if result.get("status") != "ok":
            return None, result
        return result, None

    def generate_signature(self, method, params, request_path):
        if request_path.startswith("http://") or request_path.startswith("https://"):
            host_url = urllib.parse.urlparse(request_path).hostname.lower()
            request_path = '/' + '/'.join(request_path.split('/')[3:])
        else:
            host_url = urllib.parse.urlparse(self._host).hostname.lower()
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class HuobiSwapMarket(Websocket):
    """ Huobi Swap Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `huobi_swap`.
            wss: Exchange Websocket host address.
            symbols: Trade pair list, e.g. ["BTC-CQ"].
            channels: channel list, only `orderbook`, `kline` and `trade` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """
    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://www.hbdm.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbooks_length = kwargs.get("orderbooks_length", 100)
        self._klines_length = kwargs.get("klines_length", 100)
        self._trades_length = kwargs.get("trades_length", 100)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")

        self._c_to_s = {}  # {"channel": "symbol"}

        url = self._wss + "/swap-ws"
        super(HuobiSwapMarket, self).__init__(url, send_hb_interval=5)
        self.initialize()

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        data = {"pong": int(time.time()*1000)}
        try:
            await self.ws.send_json(data)
        except ConnectionResetError:
            SingleTask.run(self._reconnect)

    async def connected_callback(self):
        """ After create Websocket connection successfully, we will subscribing orderbook/trade events.
        """
        for ch in self._channels:
            if ch == "kline":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "kline")
                    if not channel:
                        continue
                    kline = {
                        "sub": channel
                    }
                    await self.ws.send_json(kline)
            elif ch == "orderbook":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "depth")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            elif ch == "trade":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process(self, msg):
        """只继承不实现"""
        pass

    async def process_binary(self, msg):
        """ Process binary message that received from Websocket connection.
        """
        data = json.loads(gzip.decompress(msg).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self.ws.send_json(hb_msg)
            return

        if channel.find("kline") != -1:
            await self.process_kline(data)

        elif channel.find("depth") != -1:
            await self.process_orderbook(data)

        elif channel.find("trade") != -1:
            await self.process_trade(data)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        """ Convert symbol to channel.

        Args:
            symbol: Trade pair name.such as BTC-USD
            channel_type: channel name, kline / ticker / depth.
        """
        if channel_type == "kline":
            channel = "market.{s}.kline.1min".format(s=symbol.upper())
        elif channel_type == "depth":
            channel = "market.{s}.depth.step6".format(s=symbol.upper())
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol.upper())
        else:
            logger.error("channel type error! channel type:", channel_type, caller=self)
            return None
        self._c_to_s[channel] = symbol
        return channel

    async def process_kline(self, data):
        """ process kline data
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        d = data.get("tick")
        info = {
            "platform": self._platform,
            "symbol": symbol,
            "open": "%.8f" % d["open"],
            "high": "%.8f" % d["high"],
            "low": "%.8f" % d["low"],
            "close": "%.8f" % d["close"],
            "volume": "%.8f" % d["amount"],
            "timestamp": int(data.get("ts")),
            "kline_type": MARKET_TYPE_KLINE
        }
        kline = Kline(**info)
        SingleTask.run(self._kline_update_callback, copy.copy(kline))

        logger.debug("symbol:", symbol, "kline:", kline, caller=self)

    async def process_orderbook(self, data):
        """ process orderbook data
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        d = data.get("tick")
        info = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": d.get("asks")[:self._orderbook_length],
            "bids": d.get("bids")[:self._orderbook_length],
            "timestamp": d.get("ts")
        }
        orderbook = Orderbook(**info)
        SingleTask.run(self._orderbook_update_callback, copy.copy(orderbook))

    async def process_trade(self, data):
        """ process trade
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        ticks = data.get("tick")
        for tick in ticks["data"]:
            direction = tick.get("direction")
            price = tick.get("price")
            quantity = tick.get("amount")
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "action": ORDER_ACTION_BUY if direction == "buy" else ORDER_ACTION_SELL,
                "price": "%.8f" % price,
                "quantity": "%.8f" % quantity,
                "timestamp": tick.get("ts")
            }
            trade = Trade(**info)
            SingleTask.run(self._trade_update_callback, copy.copy(trade))


class HuobiSwapTrade(Websocket):
    """ Huobi Swap Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. default `https://api.hbdm.com"`.
        wss: Websocket address. default `wss://www.hbdm.com`.
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
        asset_update_callback: You can use this param to specific a async callback function when you initializing
                               Trade object. `asset_update_callback` is like `async def
                               on_asset_update_callback(asset: Asset): pass` and this callback function will be
                               executed asynchronous when received AssetEvent.
        order_update_callback: You can use this param to specific a async callback function when you initializing
                               Trade object. `order_update_callback` is like `async
                               def on_order_update_callback(order: Order): pass` and this callback function will be
                               executed asynchronous when some order state updated.
        position_update_callback: You can use this param to specific a async callback function when you initializing
                                  Trade object. `position_update_callback` is like `async def
                                  on_position_update_callback(order: Position): pass` and this callback function will
                                  be executed asynchronous when some position state updated.
        init_success_callback: You can use this param to specific a async callback function when you initializing
                               Trade object. `init_success_callback` is like `async def
                               on_init_success_callback(success: bool, error: Error, **kwargs): pass` and this
                               callback function will be executed asynchronous after Trade module object
                               initialized successfully.
    """
    def __init__(self, **kwargs):
        """Initialize."""
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("contract_type"):
            e = Error("param contract_type miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.hbdm.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://api.hbdm.com"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = HUOBI_SWAP
        self._symbol = kwargs["symbol"]
        self._contract_type = kwargs["contract_type"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        url = self._wss + "/swap-notification"
        super(HuobiSwapTrade, self).__init__(url, send_hb_interval=5)

        self._assets = {}  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(self._platform, self._account, self._strategy, self._symbol+'/'+self._contract_type)

        self._order_channel = "orders.{symbol}".format(symbol=self._symbol)
        self._position_channel = "positions.{symbol}".format(symbol=self._symbol)
        self._asset_channel = "accounts.{symbol}".format(symbol=self._symbol)

        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        self._rest_api = HuobiSwapRestAPI(self._host, self._access_key, self._secret_key)

        self.initialize()

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def position(self):
        return copy.copy(self._position)

    @property
    def rest_api(self):
        return self._rest_api

    async def _send_heartbeat_msg(self, *args, **kwargs):
        data = {"op": "pong", "ts": str(int(time.time()*1000))}
        if not self.ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self.ws.send_json(data)

    async def connected_callback(self):
        """After connect to Websocket server successfully, send a auth message to server."""
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        data = {
            "AccessKeyId": self._access_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        }
        sign = self._rest_api.generate_signature("GET", data, "/swap-notification")
        data["op"] = "auth"
        data["type"] = "api"
        data["Signature"] = sign
        await self.ws.send_json(data)

    async def process(self, msg):
        """只继承不实现"""
        pass

    async def auth_callback(self, data):
        if data["err-code"] != 0:
            e = Error("Websocket connection authorized failed: {}".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        # subscribe order
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._order_channel
        }
        await self.ws.send_json(data)

        # subscribe position
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._position_channel
        }
        await self.ws.send_json(data)

        # subscribe asset
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._asset_channel
        }
        await self.ws.send_json(data)

    async def sub_callback(self, data):
        if data["err-code"] != 0:
            e = Error("subscribe {} failed!".format(data["topic"]))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        if data["topic"] == self._order_channel:
            self._subscribe_order_ok = True
        elif data["topic"] == self._position_channel:
            self._subscribe_position_ok = True
        elif data["topic"] == self._asset_channel:
            self._subscribe_asset_ok = True
        if self._subscribe_order_ok and self._subscribe_position_ok \
                and self._subscribe_asset_ok:
            success, error = await self._rest_api.get_open_orders(self._symbol)
            if error:
                e = Error("get open orders failed!")
                SingleTask.run(self._init_success_callback, False, e)
            elif "data" in success and "orders" in success["data"]:
                for order_info in success["data"]["orders"]:
                    order_info["ts"] = order_info["created_at"]
                    self._update_order(order_info)
                SingleTask.run(self._init_success_callback, True, None)
            else:
                logger.warn("get open orders:", success, caller=self)
                e = Error("Get Open Orders Unknown error")
                SingleTask.run(self._init_success_callback, False, e)

    @async_method_locker("HuobiSwapTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        data = json.loads(gzip.decompress(raw).decode())
        logger.debug("data:", data, caller=self)

        op = data.get("op")
        if op == "ping":
            hb_msg = {"op": "pong", "ts": data.get("ts")}
            await self.ws.send_json(hb_msg)

        elif op == "auth":
            await self.auth_callback(data)

        elif op == "sub":
            await self.sub_callback(data)

        elif op == "notify":
            if data["topic"].startswith("orders"):
                self._update_order(data)
            elif data["topic"].startswith("positions"):
                self._update_position(data)
            elif data["topic"].startswith("accounts"):
                self._update_asset(data)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, client_order_id=None, **kwargs):
        """ Create an order.

        Attributes:
            :param action: Trade direction, BUY or SELL.
            :param price: Price of each contract.
            :param quantity: The buying or selling quantity.
            :param order_type: Order type, LIMIT or MARKET.
            :param client_order_id:
            kwargs:
                lever_rate: Leverage rate, 10 or 20.

        :returns:
            :return order_no: Order ID if created successfully, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        if int(quantity) > 0:
            if action == ORDER_ACTION_BUY:
                direction = "buy"
                offset = "open"
            elif action == ORDER_ACTION_SELL:
                direction = "sell"
                offset = "close"
            else:
                return None, "action error"
        else:
            if action == ORDER_ACTION_BUY:
                direction = "buy"
                offset = "close"
            elif action == ORDER_ACTION_SELL:
                direction = "sell"
                offset = "open"
            else:
                return None, "action error"

        lever_rate = kwargs.get("lever_rate", 20)
        if order_type == ORDER_TYPE_LIMIT:
            order_price_type = "limit"
        elif order_type == ORDER_TYPE_MARKET:
            order_price_type = "optimal_20"
        elif order_type == ORDER_TYPE_MAKER:
            order_price_type = "post_only"
        elif order_type == ORDER_TYPE_FOK:
            order_price_type = "fok"
        elif order_type == ORDER_TYPE_IOC:
            order_price_type = "ioc"

        else:
            return None, "order type error"

        quantity = abs(int(quantity))
        result, error = await self._rest_api.create_order(self._symbol,
                                                          price, quantity, direction, offset, lever_rate,
                                                          order_price_type, client_order_id)
        if error:
            return None, error
        return str(result["data"]["order_id"]), None

    async def create_orders(self, orders, *args, **kwargs):
        """ batch create orders

        Attributes:
            orders: []
            list item:
                action: Trade direction, BUY or SELL.
                price: Price of each contract.
                quantity: The buying or selling quantity.
                order_type: Order type, LIMIT or MARKET.
                lever_rate: leverage.
            kwargs:

        :returns:
            :return success: order info  if created successfully.
            :return error: erros information.
        """
        orders_data = []
        for order in orders:
            if int(order["quantity"]) > 0:
                if order["action"] == ORDER_ACTION_BUY:
                    direction = "buy"
                    offset = "open"
                elif order["action"] == ORDER_ACTION_SELL:
                    direction = "sell"
                    offset = "close"
                else:
                    return None, "action error"
            else:
                if order["action"] == ORDER_ACTION_BUY:
                    direction = "buy"
                    offset = "close"
                elif order["action"] == ORDER_ACTION_SELL:
                    direction = "sell"
                    offset = "open"
                else:
                    return None, "action error"

            lever_rate = order["lever_rate"]
            if order["order_type"] == ORDER_TYPE_LIMIT:
                order_price_type = "limit"
            elif order["order_type"] == ORDER_TYPE_MARKET:
                order_price_type = "optimal_20"
            elif order["order_type"] == ORDER_TYPE_MAKER:
                order_price_type = "post_only"
            elif order["order_type"] == ORDER_TYPE_FOK:
                order_price_type = "fok"
            elif order["order_type"] == ORDER_TYPE_IOC:
                order_price_type = "ioc"
            else:
                return None, "order type error"

            quantity = abs(int(order["quantity"]))

            client_order_id = order.get("client_order_id", "")

            orders_data.append({"contract_code": self._symbol,
                                "client_order_id": client_order_id,
                                "price": order["price"],
                                "volume": quantity,
                                "direction": direction,
                                "offset": offset,
                                "leverRate": lever_rate,
                                "orderPriceType":  order_price_type
                                })

        result, error = await self._rest_api.create_orders({"orders_data": orders_data})
        if error:
            return None, error
        order_nos = [order["order_id"] for order in result.get("data").get("success")]
        return order_nos, result.get("data").get("errors")

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Attributes:
            :param order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param,
                              you can cancel all orders for this symbol(initialized in Trade object).
                              If you set 1 param, you can cancel an order. If you set multiple param,
                              you can cancel multiple orders. Do not set param length more than 100.

        :returns:
            Success or error, see bellow.
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            success, error = await self._rest_api.revoke_order_all(self._symbol)
            if error:
                return False, error
            if success.get("errors"):
                return False, success["errors"]
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(self._symbol, order_nos[0])
            if error:
                return order_nos[0], error
            if success.get("errors"):
                return False, success["errors"]
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            success, error = await self._rest_api.revoke_orders(self._symbol, order_nos)
            if error:
                return order_nos[0], error
            if success.get("errors"):
                return False, success["errors"]
            return success, error

    async def get_open_order_nos(self):
        """ Get open order id list.

        Attributes:
            None.

        :returns:
            :return order_nos: Open order id list, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(self._symbol)
        if error:
            return None, error
        else:
            order_nos = []
            for order_info in success["data"]["orders"]:
                if order_info["contract_code"] != self._symbol:
                    continue
                order_nos.append(str(order_info["order_id"]))
            return order_nos, None

    def _update_order(self, order_info):
        """ Order update.

        Attributes:
            :param order_info: Order information.
        """
        if order_info["contract_code"] != self._symbol:
            return
        order_no = str(order_info["order_id"])
        status = order_info["status"]

        order = self._orders.get(order_no)
        if not order:
            if order_info["direction"] == "buy":
                if order_info["offset"] == "open":
                    trade_type = TRADE_TYPE_BUY_OPEN
                else:
                    trade_type = TRADE_TYPE_BUY_CLOSE
            else:
                if order_info["offset"] == "close":
                    trade_type = TRADE_TYPE_SELL_CLOSE
                else:
                    trade_type = TRADE_TYPE_SELL_OPEN

            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "client_order_id": order_info.get("client_order_id"),
                "order_price_type": order_info.get("order_price_type"),
                "order_type": order_info["order_type"],
                "action": ORDER_ACTION_BUY if order_info["direction"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol + '/' + self._contract_type,
                "price": order_info["price"],
                "quantity": order_info["volume"],
                "trade_type": trade_type
            }
            order = Order(**info)
            self._orders[order_no] = order

        order.trade_quantity = None
        order.trade_price = None
        if order_info.get("trade"):
            quantity = 0
            amount = 0
            for trade in order_info.get("trade"):
                order.role = trade.get("role")
                quantity += float(trade.get("trade_volume"))
                amount += float(trade.get("trade_volume")*trade.get("trade_price"))
            price = amount/quantity
            order.trade_quantity = int(quantity)
            order.trade_price = price

        if status in [1, 2, 3]:
            order.status = ORDER_STATUS_SUBMITTED
        elif status == 4:
            order.status = ORDER_STATUS_PARTIAL_FILLED
            order.remain = int(order.quantity) - int(order_info["trade_volume"])
        elif status == 6:
            order.status = ORDER_STATUS_FILLED
            order.remain = 0
        elif status in [5, 7]:
            order.status = ORDER_STATUS_CANCELED
            order.remain = int(order.quantity) - int(order_info["trade_volume"])
        else:
            return

        order.avg_price = order_info["trade_avg_price"]
        order.ctime = order_info["created_at"]
        order.utime = order_info["ts"]

        SingleTask.run(self._order_update_callback, copy.copy(order))

        # Delete order that already completed.
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

        # publish order
        logger.info("symbol:", order.symbol, "order:", order, caller=self)

    def _update_position(self, data):
        """ Position update.

        Attributes:
            :param data: Position information.

        :returns:
            None.
        """
        for position_info in data["data"]:
            if position_info["contract_code"] != self._symbol:
                continue
            if position_info["direction"] == "buy":
                self._position.long_quantity = int(position_info["volume"])
                self._position.long_avg_price = position_info["cost_open"]
            else:
                self._position.short_quantity = int(position_info["volume"])
                self._position.short_avg_price = position_info["cost_open"]
            # self._position.liquid_price = None
            self._position.utime = data["ts"]
            SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def _update_asset(self, data):
        """ Asset update.

        data:
            :param data: asset data.

        :returns:
            None.
        """
        assets = {}
        for item in data["data"]:
            symbol = item["symbol"].upper()
            total = float(item["margin_balance"])
            free = float(item["margin_available"])
            locked = float(item["margin_frozen"])
            if total > 0:
                assets[symbol] = {
                    "total": "%.8f" % total,
                    "free": "%.8f" % free,
                    "locked": "%.8f" % locked
                }
        if assets == self._assets:
            update = False
        else:
            update = True
        if hasattr(self._assets, "assets") is False:
            info = {
                "platform": self._platform,
                "account": self._account,
                "assets": assets,
                "timestamp": tools.get_cur_timestamp_ms(),
                "update": update
            }
            asset = Asset(**info)
            self._assets = asset
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))
        else:
            for symbol in assets:
                self._assets.assets.update({
                    symbol: assets[symbol]
                })
            self._assets.timestamp = tools.get_cur_timestamp_ms()
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))

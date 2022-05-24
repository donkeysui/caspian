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
import urllib
import hashlib
import datetime
import time
from urllib.parse import urljoin
from xuanwu.const import USER_AGENT
from xuanwu.const import MARKET_TYPE_KLINE
from xuanwu.model.market import Orderbook, Kline, Trade
from urllib.parse import urljoin
from xuanwu.model.asset import Asset
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask
from xuanwu.const import HUOBI_SWAP
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *

__all__ = ("HuobiSpotRestAPI", "HuobiSpotMarket", "HuobiSpotTrade",)


class HuobiSpotRestAPI:
    """ Huobi Spot REST API Client.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
    """

    def __init__(self, host="https://api.huobi.pro", access_key="", secret_key=""):
        """ initialize REST API client. """
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key
        self._account_id = None

    async def get_spot_info(self):
        """ Get Swap Info

        Args:

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        * Note: 1. If input `contract_code`, only matching this contract code.
                2. If not input 'contract_code', matching all contract_codes.
        """
        uri = "/v1/common/symbols"
        success, error = await self.request("GET", uri)
        return success, error

    async def get_server_time(self):
        """ 获取服务器时间
        @return data int 服务器时间戳(毫秒)
        """
        success, error = await self.request("GET", "/v1/common/timestamp")
        return success, error

    async def get_orderbook(self, symbol):
        """ Get orderbook information.

        Args:
            symbol:  such as "BTC-USD".

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/market/depth"
        params = {
            "symbol": symbol,
            "type": "step0"
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_klines(self, symbol, period, size=None):
        """ Get kline information.

        Args:
            symbol:  such as "BTC-USD".
            period: 1min, 5min, 15min, 30min, 60min,4hour,1day, 1mon
            size: [1,2000]

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/market/history/kline"
        params = {
            "symbol": symbol,
            "period": period
        }
        if size:
            params["size"] = size
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_user_accounts(self):
        """ 获取账户信息
        """
        success, error = await self.request("GET", "/v1/account/accounts", auth=True)
        return success, error

    async def get_account_id(self):
        if self._account_id:
            return self._account_id
        success, error = await self.get_user_accounts()
        if error:
            return None
        for item in success["data"]:
            if item["type"] == "spot":
                self._account_id = str(item["id"])
                return self._account_id
        return None

    async def get_asset_valuation(self, account_type="spot", valuation_currency="USD"):
        """ 获取账户资产估值，用于资产统计使用

        Args:

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/v2/account/asset-valuation"
        params = {
            "accountType": account_type,
            "valuationCurrency": valuation_currency
        }
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_account_balance(self):
        """ Get account asset information.

        Args:

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        account_id = await self.get_account_id()
        uri = f"/v1/account/accounts/{account_id}/balance"
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def get_asset_by_symbol(self, symbol):
        """ Get position information.

        Args:
            symbol: such as "BTC-USD".
            参数名称	  是否必须	数据类型	描述	取值范围
            balance	  true	    string	余额
            currency  true	    string	币种
            type	  true	    string	类型	trade: 交易余额，frozen: 冻结余额, loan: 待还借贷本金, interest: 待还借贷利息, lock: 锁仓, bank: 储蓄

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        s, e = await self.get_account_balance()
        if s:
            data_list = s["data"]["list"]
            symbol_list = []
            for dl in data_list:
                if dl["currency"] == symbol:
                    symbol_list.append(dl)
            if symbol_list:
                return symbol_list, None
            return None, None

    async def create_order(self, symbol, type_, quantity, price=None, client_order_id=None):
        """ Create an new order.

        Args:
            symbol: 交易对,即btcusdt, ethbtc.
            type_: 订单类型.buy-market, sell-market, buy-limit, sell-limit
            quantity: 订单交易量（市价买单为订单交易额）.
            price: 订单价格（对市价单无效）.
            client_order_id: 用户自编订单号

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        account_id = await self.get_account_id()
        uri = "/v1/order/orders/place"
        body = {
            "account-id": account_id,
            "symbol": symbol,
            "type": type_,
            "amount": quantity,
            "source": "spot-api"
        }
        if client_order_id:
            body.update({"client_order_id": client_order_id})
        if type_ != "buy-market" or type_ != "sell-market":
            body["price"] = price
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_orders(self, orders_data):
        """ Batch Create orders.
            orders_data = {'orders_data': [
                                            {
                                            "account-id": "123456",
                                            "price": "7801",
                                            "amount": "0.001",
                                            "symbol": "btcusdt",
                                            "type": "sell-limit",
                                            "client-order-id": "c1"
                                            },
                                            {
                                            "account-id": "123456",
                                            "price": "7802",
                                            "amount": "0.001",
                                            "symbol": "btcusdt",
                                            "type": "sell-limit",
                                            "client-order-id": "d2"
                                            }]
                            }
        """
        uri = "/v1/order/batch-orders"
        body = orders_data
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, order_id):
        """ Revoke an order.

        Args:
            order_id: Order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = f"/v1/order/orders/{order_id}/submitcancel"

        success, error = await self.request("POST", uri, auth=True)
        return success, error

    async def revoke_orders(self, order_ids):
        """ Revoke multiple orders.

        Args:
            order_ids: Order ID list.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/v1/order/orders/batchcancel"
        body = {}
        if order_ids:
            body["order_id"] = ",".join(order_ids)

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order_all(self, account_id=None, symbol=None, types=None, side=None, size=None):
        """ Revoke all orders.

        Args:
            :param account_id: 账户ID
            :param symbol: 交易代码列表（最多10 个symbols，多个交易代码间以逗号分隔）
            :param types: 订单类型组合，使用逗号分割
            :param side: 主动交易方向
            :param size: 撤销订单的数量

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        """
        uri = "/v1/order/orders/batchCancelOpenOrders"
        body = {
            "account-id": account_id,
        }
        if symbol:
            body["symbol"] = symbol
        if types:
            body["types"] = types
        if side:
            body["side"] = side
        if size:
            body["size"] = size

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, symbol):
        """ 获取当前还未完全成交的订单信息
        @param symbol 交易对
        * NOTE: 查询上限最多500个订单
        """
        account_id = await self.get_account_id()
        params = {
            "account-id": account_id,
            "symbol": symbol,
            "size": 500
        }
        success, error = await self.request("GET", "/v1/order/openOrders", params=params, auth=True)
        return success, error

    async def get_order_info(self, order_ids=None):
        """ Get order information.

        Args:
            order_ids: Order ID list. (different IDs are separated by ",", maximum 20 orders can be requested at
             one time.)

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = f"/v1/order/orders/{order_ids}"

        success, error = await self.request("POST", uri, auth=True)
        return success, error

    async def get_history_orders(self, symbol, types=None, start_time=None, end_time=None, start_date=None,
                                 end_date=None, from_=None, direct=None, size=None):
        """ Get history orders information.

        Args:
            :param symbol: 交易对
            :param types: 查询的订单类型组合，使用','分割
            :param start_time: 查询开始时间, 时间格式UTC time in millisecond。 以订单生成时间进行查询
            :param end_time: 查询结束时间, 时间格式UTC time in millisecond。 以订单生成时间进行查询
            :param start_date: 查询开始日期（新加坡时区）日期格式yyyy-mm-dd
            :param end_date: 查询结束日期（新加坡时区）, 日期格式yyyy-mm-dd
            :param from_: 查询起始 ID
            :param direct: 查询方向
            :param size: 查询记录大小

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        """
        uri = "/v1/order/matchresults"
        body = {
            "symbol": symbol
        }
        if types:
            body["types"] = types
        if start_time:
            body["start_time"] = start_time
        if end_time:
            body["end_time"] = end_time
        if start_date:
            body["start_date"] = start_date
        if end_date:
            body["end_date"] = end_date
        if from_:
            body["from_"] = from_
        if direct:
            body["direct"] = direct
        if size:
            body["size"] = size

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. `GET` / `POST` / `DELETE` / `PUT`.
            uri: HTTP request uri.
            params: HTTP query params.
            body: HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
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
        if result.get("status") and result.get("status") != "ok":
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


class HuobiSpotMarket:
    """ Huobi Spot Market Server.

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
        self._wss = kwargs.get("wss", "wss://api.huobi.pro/ws")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._init_callback = kwargs.get("init_callback")
        self._error_callback = kwargs.get("error_callback")

        self._c_to_s = {}  # {"channel": "symbol"}

        url = self._wss + "/ws"
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )

    async def connected_callback(self):
        """ After create Websocket connection successfully, we will subscribing orderbook/trade events.
        """
        for ch in self._channels:
            if ch == "kline":
                if self._kline_update_callback is None:
                    logger.error("Kline callback is None")
                    SingleTask.run(self._error_callback, False, "Kline callback is None")
                    return
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "kline")
                    if not channel:
                        continue
                    kline = {
                        "sub": channel,
                        "id": "id1"
                    }
                    await self._ws.send(kline)
                    SingleTask.run(self._init_callback, True, "Sub kline success")
            elif ch == "orderbook":
                if self._orderbook_update_callback is None:
                    logger.error("Orderbook callback is None")
                    SingleTask.run(self._error_callback, False, "Orderbook callback is None")
                    return
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "depth")
                    if not channel:
                        continue
                    data = {
                        "sub": channel,
                        "id": "id1"
                    }
                    await self._ws.send(data)
                    SingleTask.run(self._init_callback, True, "Sub orderbook success")
            elif ch == "trade":
                if self._trade_update_callback is None:
                    logger.error("Trade callback is None")
                    SingleTask.run(self._error_callback, False, "Trade callback is None")
                    return
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    if not channel:
                        continue
                    data = {
                        "sub": channel,
                        "id": "id1"
                    }
                    await self._ws.send(data)
                    SingleTask.run(self._init_callback, True, "Sub trade success")
            else:
                logger.error("channel error! channel:", ch, caller=self)
                SingleTask.run(self._error_callback, False, f"channel error! channel: {ch}")

    async def process_binary(self, msg):
        """ Process binary message that received from Websocket connection.
        """
        data = json.loads(gzip.decompress(msg).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self._ws.send(hb_msg)
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
            channel = "market.{s}.kline.1min".format(s=symbol.lower())
        elif channel_type == "depth":
            channel = "market.{s}.mbp.refresh.20".format(s=symbol.lower())
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol.lower())
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

    async def process_orderbook(self, data):
        """ process orderbook data
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        d = data.get("tick")
        asks = d.get("asks")[:self._orderbook_length]
        bids = d.get("bids")[:self._orderbook_length]

        info = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
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


class HuobiSpotTrade:
    """ Huobi Swap Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. default `https://api.hbdm.com"`.
        wss: Websocket address. default `wss://www.hbdm.com`.
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
        asset_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `asset_update_callback` is like `async def on_asset_update_callback(asset: Asset): pass` and this
            callback function will be executed asynchronous when received AssetEvent.
        order_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `order_update_callback` is like `async def on_order_update_callback(order: Order): pass` and this
            callback function will be executed asynchronous when some order state updated.
        position_update_callback: You can use this param to specific a async callback function when you initializing
        Trade
            object. `position_update_callback` is like `async def on_position_update_callback(order: Position): pass`
            and
            this callback function will be executed asynchronous when some position state updated.
        init_success_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `init_success_callback` is like `async def on_init_success_callback
            (success: bool, error: Error, **kwargs): pass`
            and this callback function will be executed asynchronous after Trade module object initialized successfully.
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
            kwargs["host"] = "https://api.huobi.pro"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://api.huobi.pro"
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

        url = self._wss + "/ws/v2"

        self._raw_symbol = self._symbol.replace("/", "").lower()  # 转换成交易所对应的交易对格式
        self._assets = {}  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.

        self._order_channel = "orders#{symbol}".format(symbol=self._symbol)
        self._account_channel = "accounts.update#1".format(symbol=self._symbol)

        self._subscribe_order_ok = False
        self._subscribe_account_ok = False

        self._rest_api = HuobiSpotRestAPI(self._host, self._access_key, self._secret_key)
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        """After connect to Websocket server successfully, send a auth message to server."""
        sign = self.create_signature_v2(
            api_key=self._access_key,
            method="GET",
            host="api.huobi.pro",
            path="/ws/v2",
            secret_key=self._secret_key
        )
        req = {
            "action": "req",
            "ch": "auth",
            "params": sign
        }
        await self._ws.send(req)

    def create_signature_v2(self, api_key, method, host, path, secret_key, get_params=None):
        sorted_params = [
            ("accessKey", api_key),
            ("signatureMethod", "HmacSHA256"),
            ("signatureVersion", "2.1"),
            ("timestamp", datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"))
        ]

        if get_params:
            sorted_params.extend(list(get_params.items()))
            sorted_params = list(sorted(sorted_params))
        encode_params = urllib.parse.urlencode(sorted_params)

        payload = [method, host, path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")

        secret_key = secret_key.encode(encoding="UTF8")

        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)

        params = dict(sorted_params)
        params["authType"] = "api"
        params["signature"] = signature.decode("UTF8")
        return params

    async def auth_callback(self, data):
        if data["code"] != 200:
            e = Error("Websocket connection authorized failed: {}".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        self._subscribe_order_ok = False
        self._subscribe_account_ok = False

        # subscribe order
        data = {
            "action": "sub",
            "ch": self._order_channel
        }
        await self._ws.send(data)

        # subscribe account
        data = {
            "action": "sub",
            "ch": self._account_channel
        }
        await self._ws.send(data)

    async def sub_callback(self, data):
        if data["code"] != 200:
            e = Error("Websocket connection authorized failed: {}".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        ch = data.get("ch")
        if ch:
            if ch == self._order_channel:
                self._subscribe_order_ok = True
            elif ch == self._account_channel:
                self._subscribe_account_ok = True

        if self._subscribe_order_ok and self._subscribe_account_ok:
            success, error = await self._rest_api.get_open_orders(self._raw_symbol)
            if error:
                e = Error(f"get open orders error: {error}")
                if self._init_success_callback:
                    SingleTask.run(self._init_success_callback, False, e)
                    return

            for order_info in success:
                data = {
                    "order-id": order_info["id"],
                    "order-type": order_info["type"],
                    "order-state": order_info["state"],
                    "unfilled-amount": float(order_info["amount"]) - float(order_info["filled-amount"]),
                    "order-price": float(order_info["price"]),
                    "price": float(order_info["price"]),
                    "order-amount": float(order_info["amount"]),
                    "created-at": order_info["created-at"],
                    "utime": order_info["created-at"],
                }
            self._update_order(data)

    @async_method_locker("HuobiSwapTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        data = json.loads(gzip.decompress(raw).decode())
        logger.debug("data:", data, caller=self)

        action = data.get("action")
        ch = data.get("ch")

        if action and ch:
            if action == "ping":
                hb_msg = {"action": "pong", "data": {
                    "ts": data.get("ts")
                }}
                await self._ws.send(hb_msg)
            elif action == "req" and ch == "auth":
                await self.auth_callback(data)
            elif action == "sub":
                await self.sub_callback(data)
            elif action == "push":
                if ch == self._order_channel:
                    self._update_order(data)
                elif ch == self._account_channel:
                    self._update_asset(data)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, LIMIT or MARKET.
            kwargs:
                lever_rate: Leverage rate, 10 or 20.
            action:

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if action == ORDER_ACTION_BUY:
            if order_type == ORDER_TYPE_LIMIT:
                t = "buy-limit"
            elif order_type == ORDER_TYPE_MARKET:
                t = "buy-market"
            else:
                logger.error("order_type error! order_type:", order_type, caller=self)
                return None, "order type error"
        elif action == ORDER_ACTION_SELL:
            if order_type == ORDER_TYPE_LIMIT:
                t = "sell-limit"
            elif order_type == ORDER_TYPE_MARKET:
                t = "sell-market"
            else:
                logger.error("order_type error! order_type:", order_type, caller=self)
                return None, "order type error"
        else:
            logger.error("action error! action:", action, caller=self)
            return None, "action error"
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        result, error = await self._rest_api.create_order(symbol=self._raw_symbol, price=price, quantity=quantity,
                                                          type_=t)

        if error:
            return None, error
        return str(result["data"]), None

    async def create_orders(self, orders, *args, **kwargs):
        """ batch create orders

        Args:
            orders: []

            kwargs:

        Returns:
            success: order info  if created successfully.
            error: erros information.
        """
        orders_data = []
        account_id = await self._rest_api.get_account_id()
        for order in orders:
            if int(order["quantity"]) > 0:
                if order["order_type"] == ORDER_TYPE_LIMIT:
                    type_ = "buy-limit"
                elif order["order_type"] == ORDER_TYPE_MARKET:
                    type_ = "buy-market"
                else:
                    return None, "order_type error"
            elif int(order["quantity"]) < 0:
                if order["order_type"] == ORDER_TYPE_LIMIT:
                    type_ = "sell-limit"
                elif order["order_type"] == ORDER_TYPE_MARKET:
                    type_ = "sell-market"
                else:
                    return None, "order_type error"
            elif int(order["quantity"]) == 0:
                return None, "quantity is None"
            else:
                return None, "order type error"

            quantity = abs(order["quantity"])

            client_order_id = order.get("client_order_id", "")

            orders_data.append({"symbol": self._symbol,
                                "account-id": account_id,
                                "price": order["price"],
                                "amount": quantity,
                                "type": type_,
                                "client-order-id": client_order_id})

        result, error = await self._rest_api.create_orders({"orders_data": orders_data})
        if error:
            return None, error
        order_nos = [order["order_id"] for order in result.get("data").get("order-id")]
        return order_nos, result.get("data").get("errors")

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Args:
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            Success or error, see bellow.
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            success, error = await self._rest_api.revoke_order_all(self._symbol)
            if error:
                return False, error
            if success.get("err-code"):
                return False, success["err-code"]
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(order_nos[0])
            if error:
                return order_nos[0], error
            if success.get("err-code"):
                return False, success["errors"]
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            success, error = await self._rest_api.revoke_orders(order_nos)
            if error:
                return order_nos[0], error
            if success.get("err-code"):
                return False, success["err-code"]
            return success, error

    async def get_open_order_nos(self):
        """ Get open order id list.

        Args:
            None.

        Returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(self._raw_symbol)
        if error:
            return None, error
        else:
            order_nos = []
            for order_info in success["data"]:
                if order_info["symbol"] != self._symbol.lower():
                    continue
                order_nos.append(str(order_info["id"]))
            return order_nos, None

    def _update_order(self, order_info):
        """ Order update.

        订单的更新推送由任一以下事件触发：
        - 计划委托或追踪委托触发失败事件（eventType=trigger）
        - 计划委托或追踪委托触发前撤单事件（eventType=deletion）
        - 订单创建（eventType=creation）
        - 订单成交（eventType=trade）
        - 订单撤销（eventType=cancellation）

        Args:
            order_info: Order information.
        """
        symbol = order_info["data"]["symbol"]
        if symbol != self._raw_symbol:
            return
        data = order_info.get("data")
        order_no = str(data["orderId"])
        action = ORDER_ACTION_BUY if data["type"] in ["buy-market", "buy-limit"] else ORDER_ACTION_SELL
        state = data["orderStatus"]
        avg_price = "%.8f" % float(data["orderPrice"])
        ctime = data["orderCreateTime"]
        utime = tools.get_cur_timestamp_ms()

        if state == "canceled":
            status = ORDER_STATUS_CANCELED
            remain = "%.8f" % float(data["remainAmt"])
        elif state == "partial-canceled":
            status = ORDER_STATUS_CANCELED
            remain = "%.8f" % float(data["remainAmt"])
        elif state == "submitted":
            status = ORDER_STATUS_SUBMITTED
            remain = "%.8f" % float(data["orderSize"])
        elif state == "partical-filled":
            status = ORDER_STATUS_PARTIAL_FILLED
            remain = "%.8f" % float(data["remainAmt"])
        elif state == "filled":
            status = ORDER_STATUS_FILLED
            remain = "%.8f" % float(data["remainAmt"])
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return None

        order = self._orders.get(order_no)
        if not order:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": action,
                "symbol": self._symbol,
                "price": "%.8f" % float(data["orderPrice"]),
                "quantity": "%.8f" % float(order_info["orderSize"]),
                "remain": remain,
                "status": status
            }
            order = Order(**info)
            self._orders[order_no] = order
        order.remain = remain
        order.status = status
        order.avg_price = avg_price
        order.ctime = ctime
        order.utime = utime

        if status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)
        if order and self._order_update_callback:
            SingleTask.run(self._order_update_callback, copy.copy(order))

    def _update_asset(self, data):
        """ Asset update.

        Args:
            data: asset data.

        Returns:
            None.
        """
        assets = {}
        data = data.get("data")
        currency = data["currency"]
        change_type = data["changeType"]

        if not change_type:
            balance = float(data["balance"])
            available = float(data["available"])
            frozen = balance - available
            assets[currency] = {
                "total": "%.8f" % balance,
                "free": "%.8f" % available,
                "locked": "%.8f" % frozen
            }
        elif "place" in change_type:
            if "available" not in data:
                return
            balance = self._assets.assets[currency]["total"]
            frozen = balance - float(data["available"])
            assets[currency] = {
                "total": "%.8f" % balance,
                "free": "%.8f" % float(data["available"]),
                "locked": "%.8f" % frozen
            }
        else:
            frozen = 0.0
            if "balance" in data:
                balance = float(data["balance"])
            else:
                balance = float(data["available"])
            assets[currency] = {
                "total": "%.8f" % balance,
                "free": "%.8f" % float(data["available"]),
                "locked": "%.8f" % frozen
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

# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/11/11 14:39
  @ Description: Binance USDT永续合约接口
  @ History:
"""
import hashlib
import copy
import hmac
from urllib.parse import urljoin
from xuanwu.utils.tools import get_cur_timestamp_ms
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.model.market import Orderbook, Kline, Trade
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask, LoopRunTask
from xuanwu.const import BINANCE_U_SWAP
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *

__all__ = ("BinanceUSwapTrade", "BinanceUSwapMarket", )

BASE_WS = "wss://fstream.binance.com"
BASE_REST = "https://fapi.binance.com"


class BinanceUSwapMarket:
    """ Binance USDT合约 websocket接口封装
    """

    def __init__(self, **kwargs):
        """ 基础数据初始化，基于websocket的数据连接参数初始化操作
        参数详情：
            symbol:                 订阅品种信息
            host：                  rest请求地址
            wss：                   websocket订阅地址
            channel_type：          订阅频道信息，限定只能为spot、futures、swap
            channel_detail：        订阅频道详情，只提供depth5和ticker
            ticker_update_callback：ticker数据回调函数，每100ms推送一次数据
            depth_update_callback： depth数据回调函数，有深度变化100毫秒推送一次
            trade_update_callback:  trade数据回调函数，有成交数据就推送
            init_success_callback： market类初始化回调函数
            depth_num:              depth_num只有获取市场深度数据的时候需要设置,其他时候不需要传递
        """
        self._platform = kwargs["platform"]
        self._wss = BASE_WS
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = list(set(kwargs.get("channels")))
        self._kline_interval = "5m"
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._init_callback = kwargs.get("init_callback")
        self._error_callback = kwargs.get("error_callback")

        url = self._wss + "/ws/"
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_callback=self.process_binary
        )

        LoopRunTask.register(self._send_heartbeat_msg, 5)

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """发送心跳数据"""
        if not self._ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        data = "ping"
        try:
            await self._ws.send(data)
        except ConnectionResetError:
            SingleTask.run(self._ws.reconnect)
            SingleTask.run(self._error_callback, False, "reconnect ws")

    async def connected_callback(self):
        """ 连接成功之后进行行情订阅
        :return:
        """
        for ch in self._channels:
            if ch == "kline":
                if self._kline_update_callback is None:
                    logger.error("Kline callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Kline callback is None")
                    return
                for symbol in self._symbols:
                    kline_sub = {
                        "method": "SUBSCRIBE",
                        "params":
                            [
                                f"{symbol.lower()}@kline_{self._kline_interval}"
                            ],
                        "id": get_cur_timestamp_ms()
                    }
                    await self._ws.send(json.dumps(kline_sub))
                    SingleTask.run(self._init_callback, True, "Sub Kline success")
            elif ch == "orderbook":
                if self._orderbook_update_callback is None:
                    logger.error("Orderbook callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Orderbook callback is None")
                    return
                for symbol in self._symbols:
                        depth_sub = {
                            "method": "SUBSCRIBE",
                            "params":
                                [
                                    f"{symbol.lower()}@depth20@100ms"
                                ],
                            "id": get_cur_timestamp_ms()
                        }
                        await self._ws.send(json.dumps(depth_sub))
                        SingleTask.run(self._init_callback, True, "Sub Orderbook success")
            elif ch == "trade":
                if self._trade_update_callback is None:
                    logger.error("Trade callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Trade callback is None")
                    return
                for symbol in self._symbols:
                    trade_sub = {
                        "method": "SUBSCRIBE",
                        "params":
                            [
                                f"{symbol.lower()}@aggTrade"
                            ],
                        "id": get_cur_timestamp_ms()
                    }
                    await self._ws.send(json.dumps(trade_sub))
                    SingleTask.run(self._init_callback, True, "Sub Trade success")
            else:
                logger.error("channel error! channel:", ch, caller=self)

    @async_method_locker("OKEXSwapMarket.process_binary.locker")
    async def process_binary(self, raw):
        """ 接受来自websocket的数据.
        Args:
            raw: 来自websocket的数据流.

        Returns:
            None.
        """
        e = raw.get("e")
        if e:
            if e == "aggTrade":
                await self._process_trade(raw)
            elif e == "depthUpdate":
                await self._process_orderbook(raw)
            elif e == "kline":
                await self._process_kline(raw)
            else:
                logger.info(raw, caller=self)
        else:
            pass

    async def _process_orderbook(self, data):
        """ orderbook 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        orderbook = Orderbook()
        orderbook.symbol = data["s"]
        orderbook.timestamp = data["E"]
        orderbook.asks = data["a"][:self._orderbook_length]
        orderbook.bids = data["b"][:self._orderbook_length]
        orderbook.platform = self._platform
        SingleTask.run(self._orderbook_update_callback, orderbook)

    async def _process_trade(self, data):
        """ trade 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        # 返回数据封装，加上时间戳和品种交易所信息
        trade = Trade()
        trade.platform = self._platform
        trade.symbol = data["s"]
        trade.price = data["p"]
        trade.quantity = data["q"]
        trade.side = "buy" if data["m"] else "sell"
        trade.trade_id = data["l"]
        trade.timestamp = data["T"]
        # 异步回调
        SingleTask.run(self._trade_update_callback, trade)

    async def _process_kline(self, data):
        """ kline 数据解析、封装、回调
         Args:
            :param data: WS推送数据

        Return:
            :return: None

        说明：币安USDT合约，没有提供K线成交按照币计算的额度，这里将kline.coin_volume = data["k"]["n"]赋值成成交笔数
        """
        x = data["k"]["x"]
        if x:
            kline = Kline(platform=self._platform)
            kline.symbol = data["s"]
            kline.open = data["k"]["o"]
            kline.high = data["k"]["h"]
            kline.low = data["k"]["l"]
            kline.close = data["k"]["c"]
            kline.volume = data["k"]["v"]
            kline.coin_volume = data["k"]["n"]
            kline.timestamp = data["E"]
            kline.kline_type = self._kline_interval
            SingleTask.run(self._kline_update_callback, kline)


class BinanceUSwapRestApi:
    """ Binance USDT永续合约REST API客户端.

    Attributes:
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        2020年11月26日 更新:
            添加:公共-获取全部ticker信息，为了统计每天成交比较活跃的品种做高频从策略
    """

    def __init__(self, access_key="", secret_key=""):
        """initialize REST API client."""
        self._access_key = access_key
        self._secret_key = secret_key

    async def ping(self):
        """测试 Rest API连接状态"""
        uri = "/fapi/v1/ping"
        success, error = await self.request("GET", uri)
        return success, error

    async def server_time(self):
        """测试Rest API连接状态并且获取当前服务器时间"""
        uri = "/fapi/v1/time"
        success, error = await self.request("GET", uri)
        return success, error

    async def exchange_information(self):
        """当前交易所交易规则和交易对信息"""
        uri = "/fapi/v1/exchangeInfo"
        success, error = await self.request("GET", uri)
        return success, error

    async def get_orderbook(self, symbol, limit=100):
        """获取订单簿信息.

        Args:
            symbol: 交易对名称.
            limit: The length of orderbook, Default 100, max 1000. Valid limits:[5, 10, 20, 50, 100, 500, 1000]

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/depth"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_trade(self, symbol, limit=500):
        """获取最新成交信息(up to last 500).

        Args:
            symbol: Trade pair name.
            limit: The length of trade, Default 500, max 1000.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/trades"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_kline(self, symbol, interval="1m", start=None, end=None, limit=500):
        """Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.

        Args:
            symbol: Trade pair name.
            interval: Kline interval, defaut `1m`, valid: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M.
            start: Start time(millisecond).
            end: End time(millisecond).
            limit: The length of kline, Default 20, max 1000.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_user_account(self):
        """ Get current account information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v2/account"
        ts = tools.get_cur_timestamp_ms()
        params = {
            "timestamp": str(ts)
        }
        success, error = await self.request("GET", uri, params, auth=True)
        return success, error

    async def get_position(self):
        """ Get current position information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v2/positionRisk"
        ts = tools.get_cur_timestamp_ms()
        params = {
            "timestamp": str(ts)
        }
        success, error = await self.request("GET", uri, params, auth=True)

        return success, error

    async def create_order(self, action, symbol, price, quantity, client_order_id=None):
        """ Create an order.
        Args:
            :param action: Trade direction, BUY or SELL.
            :param symbol: Symbol name, e.g. BTCUSDT.
            :param price: Price of each contract.
            :param quantity: The buying or selling quantity.
            :param client_order_id: A unique id for the order. Automatically generated if not sent.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/order"
        data = {
            "symbol": symbol,
            "side": action,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": price,
            "recvWindow": "5000",
            "timestamp": tools.get_cur_timestamp_ms()
        }
        if client_order_id:
            data["newClientOrderId"] = client_order_id
        success, error = await self.request("POST", uri, body=data, auth=True)
        return success, error

    async def revoke_order(self, symbol, order_id, client_order_id):
        """ Cancelling an unfilled order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/order"
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "origClientOrderId": client_order_id,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("DELETE", uri, params=params, auth=True)
        return success, error

    async def get_order_status(self, symbol, order_id, client_order_id):
        """ Check an order's status.

        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/order"
        params = {
            "symbol": symbol,
            "orderId": str(order_id),
            "origClientOrderId": client_order_id,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_all_orders(self, symbol, order_id=None, start=None, end=None, limit=500):
        """ Get all account orders; active, canceled, or filled.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id, default None.
            start: Start time(millisecond)
            end: End time(millisecond).
            limit: Limit return length, default 500, max 1000.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/allOrders"
        params = {
            "symbol": symbol,
            "limit": limit,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        if order_id:
            params["orderId"] = order_id
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_open_orders(self, symbol):
        """ Get all open order information.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/openOrders"
        params = {
            "symbol": symbol,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_listen_key(self):
        """ Get listen key, start a new user data stream

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/listenKey"
        params = {
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("POST", uri, params=params, auth=True)
        return success, error

    async def put_listen_key(self, listen_key):
        """ Keepalive a user data stream to prevent a time out.

        Args:
            listen_key: Listen key.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/listenKey"
        params = {
            "listenKey": listen_key,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("PUT", uri, params=params, auth=True)
        return success, error

    async def delete_listen_key(self, listen_key):
        """ Delete a listen key.

        Args:
            listen_key: Listen key.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/fapi/v1/listenKey"
        params = {
            "listenKey": listen_key,
            "timestamp": tools.get_cur_timestamp_ms()
        }
        success, error = await self.request("DELETE", uri, params=params, auth=True)
        return success, error

    async def get_funding(self, symbol):
        """ 获取资金费率

        Args:
            :param symbol: 交易币对

        Returns:
            success: 返回成功数据结果, 其他为None.
            error: 返回错误数据结果, 其他为None.
        """
        uri = "/fapi/v1/premiumIndex"
        params = {
            "symbol": symbol
        }
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(BASE_REST, uri)
        data = {}
        if params:
            data.update(params)
        if body:
            data.update(body)
        if not headers:
            headers = {}
        if data:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in data.items()])
        else:
            query = ""
        if auth and query:
            signature = hmac.new(self._secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += "&signature={s}".format(s=signature)
            headers["X-MBX-APIKEY"] = self._access_key
        if query:
            url += ("?" + query)
        _, success, error = await AsyncHttpRequests.fetch(method, url, headers=headers, timeout=10)
        return success, error


class BinanceUSwapTrade:
    """ Binance USDT永续合约交易接口，添加ws用户数据订阅功能.

        Attributes:
            account: Account name for this trade exchange.
            strategy: What's name would you want to created for you strategy.
            symbol: Symbol name for your trade.
            access_key: Account's ACCESS KEY.
            secret_key Account's SECRET KEY.
            asset_update_callback: You can use this param to specific a async callback function when you initializing Trade
                object. `asset_update_callback` is like `async def on_asset_update_callback(asset: Asset): pass` and this
                callback function will be executed asynchronous when received AssetEvent.
            order_update_callback: You can use this param to specific a async callback function when you initializing Trade
                object. `order_update_callback` is like `async def on_order_update_callback(order: Order): pass` and this
                callback function will be executed asynchronous when some order state updated.
            position_update_callback: You can use this param to specific a async callback function when you initializing Trade
                object. `position_update_callback` is like `async def on_position_update_callback(order: Position): pass` and
                this callback function will be executed asynchronous when some position state updated.
            init_success_callback: You can use this param to specific a async callback function when you initializing Trade
                object. `init_success_callback` is like `async def on_init_success_callback(success: bool, error: Error, **kwargs): pass`
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
        if not kwargs.get("wss"):
            kwargs["wss"] = BASE_WS
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
        self._platform = BINANCE_U_SWAP
        self._symbol = kwargs["symbol"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._error_callback = kwargs.get("error_callback")

        self._ws = None
        self._ok = False  # Initialize successfully ?
        self._assets = Asset(platform=self._platform,
                             account=self._account)  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(self._platform, self._account, self._strategy, self._symbol)
        self._listen_key = None
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        self._rest_api = BinanceUSwapRestApi(
            secret_key=self._secret_key,
            access_key=self._access_key
        )
        SingleTask.run(self._init_websocket)

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

    async def _reset_listen_key(self, *args, **kwargs):
        """ Reset listen key.
        """
        if not self._listen_key:
            logger.error("listen key not initialized!", caller=self)
            return
        await self._rest_api.put_listen_key(self._listen_key)
        logger.info("reset listen key success!", caller=self)

    async def _init_websocket(self):
        success, error = await self._rest_api.get_listen_key()
        if error:
            e = Error("get listen key failed: {}".format(error))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        self._listen_key = success["listenKey"]
        uri = "/ws/" + self._listen_key
        url = urljoin(self._wss, uri)
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_callback=self.process_binary
        )

    async def connected_callback(self):

        """After connect to Websocket server successfully, send a auth message to server."""
        logger.info("Websocket connection authorized successfully.", caller=self)
        order_infos, error = await self._rest_api.get_open_orders(self._symbol)
        if error:
            e = Error("get open orders error: {}".format(error))
            SingleTask.run(self._init_success_callback, False, e)
            return
        for order_info in order_infos:
            order_no = "{}_{}".format(order_info["orderId"], order_info["clientOrderId"])
            if order_info["status"] == "NEW":
                status = ORDER_STATUS_SUBMITTED
            elif order_info["status"] == "PARTIAL_FILLED":
                status = ORDER_STATUS_PARTIAL_FILLED
            elif order_info["status"] == "FILLED":
                status = ORDER_STATUS_FILLED
            elif order_info["status"] == "CANCELED":
                status = ORDER_STATUS_CANCELED
            elif order_info["status"] == "REJECTED":
                status = ORDER_STATUS_FAILED
            elif order_info["status"] == "EXPIRED":
                status = ORDER_STATUS_FAILED
            else:
                logger.warn("unknown status:", order_info, caller=self)
                continue

            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": order_info["side"],
                "order_type": order_info["type"],
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": order_info["origQty"],
                "remain": float(order_info["origQty"]) - float(order_info["executedQty"]),
                "status": status,
                "trade_type": int(order_info["clientOrderId"][-1]),
                "ctime": order_info["updateTime"],
                "utime": order_info["updateTime"]
            }
            order = Order(**info)
            self._orders[order_no] = order
            SingleTask.run(self._order_update_callback, copy.copy(order))
        self._ok = True
        SingleTask.run(self._init_success_callback, True, None)

    @async_method_locker("OkexSwapTrade.process_binary.locker")
    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        e = msg.get("e")
        if e == "ORDER_TRADE_UPDATE":  # Order update.
            self._update_order(msg["o"])
        elif e == "ACCOUNT_UPDATE":
            m = msg["a"].get("m")
            if m == "ORDER":
                a = msg["a"]
                self.on_asset_update(a["B"])
                self.on_position_update(a["P"])

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, client_oid=None,
                           *args, **kwargs):
        """ 创建单个订单.
        Args:
            :param action: 下单方向, `BUY` or `SELL`.
            :param price: 下单价格.
            :param quantity: 下单量.
            :param order_type: 订单类型, `MARKET` or `LIMIT`.
            :param client_oid: 客户端ID

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        if float(quantity) > 0:
            if action == ORDER_ACTION_BUY:
                trade_type = TRADE_TYPE_BUY_OPEN
            else:
                trade_type = TRADE_TYPE_SELL_CLOSE
        else:
            if action == ORDER_ACTION_BUY:
                trade_type = TRADE_TYPE_BUY_CLOSE
            else:
                trade_type = TRADE_TYPE_SELL_OPEN
        quantity = abs(float(quantity))
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        client_order_id = tools.get_uuid1().replace("-", "")[:21] + str(trade_type)
        result, error = await self._rest_api.create_order(action, symbol, price, quantity, client_order_id)
        if error:
            return None, error
        order_no = "{}_{}".format(result["orderId"], result["clientOrderId"])
        return order_no, None

    async def revoke_order(self, *order_nos):
        """ 撤销订单
        Args:
            :param order_nos: 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单

        Return:
            备注:关于批量删除订单函数返回值格式,如果函数调用失败了那肯定是return None, error
            如果函数调用成功,但是多个订单有成功有失败的情况,比如输入3个订单id,成功2个,失败1个,那么
            返回值统一都类似:
            return [(成功订单ID, None),(成功订单ID, None),(失败订单ID, "失败原因")], None

        注意: 每次撤销订单只能固定撤销最多10个, 接口限制
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            order_infos, error = await self._rest_api.get_open_orders(self._symbol)
            if error:
                return False, error
            for order_info in order_infos:
                _, error = await self._rest_api.revoke_order(self._symbol, order_info["orderId"],
                                                             order_info["clientOrderId"])
                if error:
                    return False, error
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            order_id, client_order_id = order_nos[0].split("_")
            success, error = await self._rest_api.revoke_order(self._symbol, order_id, client_order_id)
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            success, error = [], []
            for order_no in order_nos:
                order_id, client_order_id = order_no.split("_")
                _, e = await self._rest_api.revoke_order(self._symbol, order_id, client_order_id)
                if e:
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return success, error

    async def get_open_order_nos(self):
        """ Get open order id list.

        Args:
            None.

        Returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(self._symbol)
        if error:
            return None, error
        order_nos = []
        for order_info in success:
            order_no = "{}_{}".format(order_info["orderId"], order_info["clientOrderId"])
            order_nos.append(order_no)
        return order_nos, None

    async def get_position(self, symbol):
        """ 获取对应币对的仓位信息
        :param symbol:
        :return:
        """
        success, error = await self._rest_api.get_position()
        if success:
            for res in success:
                sym = res["symbol"]
                if sym == symbol:
                    position = Position(platform=BINANCE_U_SWAP, symbol=symbol)
                    positionAmt = float(res["positionAmt"])
                    position.leverage = res["leverage"]
                    position.liquid_price = res["liquidationPrice"]

                    if positionAmt > 0:
                        position.long_pnl = res["unRealizedProfit"]
                        position.long_quantity = str(positionAmt)
                        position.long_avg_price = res["entryPrice"]
                        position.long_avail_qty = str(positionAmt)
                    if positionAmt < 0:
                        position.short_pnl = res["unRealizedProfit"]
                        position.short_quantity = str(abs(positionAmt))
                        position.short_avg_price = res["entryPrice"]
                        position.short_avail_qty = str(abs(positionAmt))

                    return position, error
        else:
            return None, error

    async def _check_position_update(self, *args, **kwargs):
        """Check position update."""
        if not self._ok:
            return
        update = False
        success, error = await self._rest_api.get_position()
        if error:
            return

        position_info = None
        for item in success:
            if item["symbol"] == self._symbol:
                position_info = item
                break

        if not self._position.utime:  # Callback position info when initialized.
            update = True
            self._position.update()
        size = float(position_info["positionAmt"])
        average_price = float(position_info["entryPrice"])
        if size > 0:
            if self._position.long_quantity != size:
                update = True
                self._position.update(0, 0, size, average_price, 0)
        elif size < 0:
            if self._position.short_quantity != abs(size):
                update = True
                self._position.update(abs(size), average_price, 0, 0, 0)
        elif size == 0:
            if self._position.long_quantity != 0 or self._position.short_quantity != 0:
                update = True
                self._position.update()
        if update:
            await self._position_update_callback(copy.copy(self._position))

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            Return order object if or None.
        """
        if order_info["s"] != self._symbol.upper():
            return
        order_no = "{}_{}".format(order_info["i"], order_info["c"])
        client_order_id = order_info["c"]

        status = None
        if order_info["X"] == "NEW":
            status = ORDER_STATUS_SUBMITTED
        elif order_info["X"] == "PARTIAL_FILLED":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif order_info["X"] == "FILLED":
            status = ORDER_STATUS_FILLED
        elif order_info["X"] == "CANCELED":
            status = ORDER_STATUS_CANCELED
        elif order_info["X"] == "REJECTED":
            status = ORDER_STATUS_FAILED
        elif order_info["X"] == "EXPIRED":
            status = ORDER_STATUS_FAILED

        order = self._orders.get(order_no)

        if not order:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_id": order_no,
                "action": order_info["S"],
                "order_type": order_info["o"],
                "symbol": self._symbol,
                "price": order_info["p"],
                "quantity": order_info["q"],
                "ctime": order_info["T"],
                "client_order_id": client_order_id,
            }
            order = Order(**info)
            self._orders[order_no] = order
        # 最后处理方便后续订单更新的时候对 order dict做增删改查处理
        order.remain = float(order_info["q"]) - float(order_info["z"])
        order.avg_price = order_info["L"]
        order.status = status
        order.utime = order_info["T"]
        order.trade_price = order_info["L"]
        order.trade_quantity = order_info["z"]
        order.fee = None if not order_info.get("n") else order_info.get("n")
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)
        SingleTask.run(self._order_update_callback, copy.copy(order))

    def on_position_update(self, data, *args, **kwargs):
        """
        :param data:
        :param args:
        :param kwargs:
        :return:

        仓位更新 [{'s': 'ETHUSDT', 'pa': '-0.100', 'ep': '1321.93000', 'cr': '-0.06700000', 'up': '-0.00700000', 'mt': 'cross', 'iw': '0', 'ps': 'BOTH', 'ma': 'USDT'}]
        仓位更新 [{'s': 'ETHUSDT', 'pa': '0.200', 'ep': '1309.37000', 'cr': '-1.00700000', 'up': '-0.24400000', 'mt': 'cross', 'iw': '0', 'ps': 'BOTH', 'ma': 'USDT'}]
        仓位信息中"pa"表示持仓信息,正数为多头头寸,负数为空头头寸
        """
        for posi in data:
            s = posi.get("s")
            if s != self._symbol:
                return

            pa = posi["pa"]
            if pa > 0:
                self.position.long_quantity = posi.get("pa")  # 多仓数量
                self.position.long_avail_qty = posi.get("pa")  # 多仓可平仓数量
                self.position.long_avg_price = posi.get("ep")  # 多仓持仓平均价格
                self.position.long_pnl = posi.get("up")  # 多仓盈亏

                self.position.short_quantity = "0"  # 空仓数量
                self.position.short_avail_qty = "0"  # 空仓可平仓数量
                self.position.short_avg_price = "0"  # 空仓持仓平均价格
                self.position.short_pnl = "0"  # 空仓盈亏
            elif pa < 0:
                pa = posi.get("pa").split("-")[1]
                self.position.long_quantity = "0"  # 多仓数量
                self.position.long_avail_qty = "0"  # 多仓可平仓数量
                self.position.long_avg_price = "0"  # 多仓持仓平均价格
                self.position.long_pnl = "0"  # 多仓盈亏

                self.position.short_quantity = pa  # 空仓数量
                self.position.short_avail_qty = pa  # 空仓可平仓数量
                self.position.short_avg_price = posi.get("ep")  # 空仓持仓平均价格
                self.position.short_pnl = posi.get("up")  # 空仓盈亏
            elif pa == 0:
                self.position.short_quantity = "0"  # 空仓数量
                self.position.short_avail_qty = "0"  # 空仓可平仓数量
                self.position.short_avg_price = "0"  # 空仓持仓平均价格
                self.position.short_pnl = "0"  # 空仓盈亏

                self.position.long_quantity = "0"  # 多仓数量
                self.position.long_avail_qty = "0"  # 多仓可平仓数量
                self.position.long_avg_price = "0"  # 多仓持仓平均价格
                self.position.long_pnl = "0"  # 多仓盈亏
            else:
                pass
        SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def on_asset_update(self, asset):
        """ Asset data update callback.

        Args:
            asset: Asset object.
        """
        for ass in asset:
            assets = {}
            a = ass.get("a")
            total = float(ass.get("wb"))
            assets[a] = {
                "total": "%.8f" % total,
                "free": "%.8f" % 0,
                "locked": "%.8f" % 0
            }

            self._assets.assets = assets
            self._assets.timestamp = tools.get_cur_timestamp_ms()
            self._assets.update = True
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))


# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/12 16:55
  @ Description: 
  @ History:
"""
import copy
import hmac
import base64
import time
import zlib
from urllib.parse import urljoin
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.model.market import Orderbook, Kline, Trade
from xuanwu.model.asset import Asset
from xuanwu.model.symbol_info import SymbolInfo
from xuanwu.error import Error
from xuanwu.utils import tools, logger
from xuanwu.tasks import SingleTask, LoopRunTask
from xuanwu.const import OKEX
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *


class OKEXSpotMarket:
    """ OKEX websocket接口封装
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
        self._wss = "wss://real.okex.com:8443"
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = list(set(kwargs.get("channels")))
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._init_callback = kwargs.get("init_callback")
        self._error_callback = kwargs.get("error_callback")

        self._orderbook = Orderbook(platform=self._platform)

        url = self._wss + "/ws/v3"
        # 连接WS对象
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
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
                        "op": "subscribe",
                        "args": [f"spot/candle60s:{symbol}"]
                    }
                    await self._ws.send(kline_sub)
                    SingleTask.run(self._init_callback, True, "Sub kline success")
            elif ch == "orderbook":
                if self._orderbook_update_callback is None:
                    logger.error("Orderbook callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Orderbook callback is None")
                    return
                for symbol in self._symbols:
                    depth_sub = {
                        "op": "subscribe",
                        "args": [f"spot/depth_l2_tbt:{symbol}"]
                    }
                    await self._ws.send(depth_sub)
                    SingleTask.run(self._init_callback, True, "Sub orderbook success")
            elif ch == "trade":
                if self._trade_update_callback is None:
                    logger.error("Trade callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Trade callback is None")
                    return
                for symbol in self._symbols:
                    trade_sub = {
                        "op": "subscribe",
                        "args": [f"spot/trade:{symbol}"]
                    }
                    await self._ws.send(trade_sub)
                    SingleTask.run(self._init_callback, True, "Sub trade success")
            else:
                logger.error("channel error! channel:", ch, caller=self)
                SingleTask.run(self._error_callback, False, f"channel error! channel: {ch}")

    @async_method_locker("OKEXSpotMarket.process_binary.locker")
    async def process_binary(self, raw):
        """ 接受来自websocket的数据.
        Args:
            raw: 来自websocket的数据流.

        Returns:
            None.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()

        if msg == "pong":
            return

        msg = json.loads(msg)
        # Authorization message received.
        if msg.get("event") == "login":
            if not msg.get("success"):
                e = Error("Websocket连接用户认证失败: {}".format(msg))
                logger.error(e, caller=self)
                SingleTask.run(self._error_callback, "auth error", e)
                return
            logger.info("Websocket 连接用户认证成功.", caller=self)
            return

        # 订阅消息反馈
        if msg.get("event") == "error":
            logger.info(msg["message"], caller=self)
            return

        if "table" in msg.keys():
            channel = msg["table"].split("/")[1]
            # 调用市场交易数据解析函数
            if channel == "trade":
                await self._process_trade(msg["data"])
            # 调用市场深度数据解析函数
            elif channel == "depth_l2_tbt":
                await self._process_orderbook(msg)
            elif channel == "candle60s":
                await self._process_kline(msg["data"])

    async def _process_orderbook(self, data):
        """ orderbook 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        action = data["action"]
        instrument_id = data.get("data")[0].get("instrument_id")
        if instrument_id not in self._symbols:
            return
        instrument_id = data["data"][0]['instrument_id']
        bids = data["data"][0]["bids"]
        asks = data["data"][0]["asks"]

        if action == "partial":
            self._orderbook.symbol = instrument_id
            self._orderbook.bids = bids
            self._orderbook.asks = asks
            self._orderbook.timestamp = data['data'][0]['timestamp']

            checksum = data['data'][0]['checksum']
            # print(timestamp + '推送数据的checksum为：' + str(checksum))
            check_num = self.check(bids, asks)
            # print(timestamp + '校验后的checksum为：' + str(check_num))
            if check_num == checksum:
                logger.info("订单簿首次推送校验结果为：True", caller=self)
            else:
                # 发送订阅
                logger.info("校验错误，重新连接WS......", caller=self)
                SingleTask.run(self._ws.reconnect)
            if self._orderbook_update_callback:
                d = copy.copy(self._orderbook)
                asks = d.asks[:self._orderbook_length]
                bids = d.bids[:self._orderbook_length]

                d.asks = asks
                d.bids = bids

                SingleTask.run(self._orderbook_update_callback, d)

        if action == "update":
            # 获取全量数据
            bids_p = self._orderbook.bids
            asks_p = self._orderbook.asks
            bids_p = self.update_bids(bids, bids_p)
            asks_p = self.update_asks(asks, asks_p)

            self._orderbook.bids = bids_p
            self._orderbook.asks = asks_p

            checksum = data['data'][0]['checksum']
            check_num = self.check(bids_p, asks_p)

            if check_num == checksum:
                # logger.debug("Update 订单簿更新推送校验结果为：True", caller=self)
                pass
            else:
                logger.info(f"{instrument_id}, Update 校验结果为：False，正在重新订阅……", caller=self)

                # 发送订阅
                SingleTask.run(self._ws.reconnect)
            if self._orderbook_update_callback:
                d = copy.copy(self._orderbook)
                asks = d.asks[:self._orderbook_length]
                bids = d.bids[:self._orderbook_length]

                d.asks = asks
                d.bids = bids

                SingleTask.run(self._orderbook_update_callback, d)

    async def _process_trade(self, data):
        """ trade 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        # 返回数据封装，加上时间戳和品种交易所信息
        for dt in data:
            instrument_id = dt.get("instrument_id")
            if instrument_id not in self._symbols:
                return
            trade = Trade()
            trade.platform = self._platform
            trade.symbol = dt["instrument_id"]
            trade.price = dt["price"]
            if dt.get("size"):
                trade.quantity = dt["size"]
            if dt.get("qty"):
                trade.quantity = dt["qty"]
            trade.side = dt["side"]
            trade.trade_id = dt["trade_id"]
            trade.timestamp = dt['timestamp']

            # 异步回调
            SingleTask.run(self._trade_update_callback, trade)

    async def _process_kline(self, data):
        """ kline 数据解析、封装、回调
         Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        for k in data:
            instrument_id = k.get("instrument_id")
            if instrument_id not in self._symbols:
                return
            kline = Kline(platform=self._platform)
            kline.symbol = k["instrument_id"]
            kline.open = k["candle"][1]
            kline.high = k["candle"][2]
            kline.low = k["candle"][3]
            kline.close = k["candle"][4]
            kline.volume = k["candle"][5]
            kline.coin_volume = k["candle"][6]
            kline.timestamp = k["candle"][0]
            SingleTask.run(self._kline_update_callback, kline)

    # 订单簿增量数据相关校验、拼接等方法
    def update_bids(self, res, bids_p):
        """ 更新全量Bids数据
        Args:
            :param res: 原始Bids数据
            :param bids_p: 更新的Bids数据
        Return:
            :return: 返回新的全量Bids数据
        """
        # 获取增量bids数据
        bids_u = res
        # bids合并
        for i in bids_u:
            bid_price = i[0]
            for j in bids_p:
                if bid_price == j[0]:
                    if i[1] == '0':
                        bids_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    bids_p.append(i)
        else:
            bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
        return bids_p

    def update_asks(self, res, asks_p):
        """ 更新全量Asks数据
        Args:
            :param res: 原始Asks数据
            :param asks_p: 更新的Asks数据
        Return:
            :return: 返回新的全量Asks数据
        """
        # 获取增量asks数据
        asks_u = res
        # asks合并
        for i in asks_u:
            ask_price = i[0]
            for j in asks_p:
                if ask_price == j[0]:
                    if i[1] == '0':
                        asks_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    asks_p.append(i)
        else:
            asks_p.sort(key=lambda price: self.sort_num(price[0]))
        return asks_p

    def sort_num(self, n):
        """ 排序函数
        Args:
            :param n: 数据对象
        Return:
            :return: 排序结果
        """
        if n.isdigit():
            return int(n)
        else:
            return float(n)

    def check(self, bids, asks):
        """ 首次接受订单簿对订单簿数据进行校验
        Args:
            :param bids: 全量bids数据
            :param asks: 全量asks数据
        Return:
            :return: 返回校验结果
        """
        # 获取bid档str
        bids_l = []
        bid_l = []
        count_bid = 1
        while count_bid <= 25:
            if count_bid > len(bids):
                break
            bids_l.append(bids[count_bid - 1])
            count_bid += 1
        for j in bids_l:
            str_bid = ':'.join(j[0: 2])
            bid_l.append(str_bid)
        # 获取ask档str
        asks_l = []
        ask_l = []
        count_ask = 1
        while count_ask <= 25:
            if count_ask > len(asks):
                break
            asks_l.append(asks[count_ask - 1])
            count_ask += 1
        for k in asks_l:
            str_ask = ':'.join(k[0: 2])
            ask_l.append(str_ask)
        # 拼接str
        num = ''
        if len(bid_l) == len(ask_l):
            for m in range(len(bid_l)):
                num += bid_l[m] + ':' + ask_l[m] + ':'
        elif len(bid_l) > len(ask_l):
            # bid档比ask档多
            for n in range(len(ask_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(ask_l), len(bid_l)):
                num += bid_l[l] + ':'
        elif len(bid_l) < len(ask_l):
            # ask档比bid档多
            for n in range(len(bid_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(bid_l), len(ask_l)):
                num += ask_l[l] + ':'

        new_num = num[:-1]
        int_checksum = zlib.crc32(new_num.encode())
        fina = self.change(int_checksum)
        return fina

    def change(self, num_old):
        """ 生成checksum验证数据
        Args:
            :param num_old: 校验数据
        Return:
            :return: 返回校验数据
        """
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out


class OKExSpotRestAPI:
    """ OKEx REST API client.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
    """

    def __init__(self, host="https://www.okex.com", access_key="", secret_key="", passphrase=""):
        """initialize."""
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase

    async def get_symbols_info(self):
        """ 获取所有交易对基础信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self.request("GET", "/api/spot/v3/instruments")
        return result, error

    async def get_account_balance(self):
        """ Get account asset information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self.request("GET", "/api/spot/v3/accounts", auth=True)
        return result, error

    async def create_order(self, action, symbol, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ Create an order.
        Args:
            action: Action type, `BUY` or `SELL`.
            symbol: Trading pair, e.g. BTC-USDT.
            price: Order price.
            quantity: Order quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        info = {
            "side": "buy" if action == ORDER_ACTION_BUY else "sell",
            "instrument_id": symbol,
            "margin_trading": 1
        }
        if order_type == ORDER_TYPE_LIMIT:
            info["type"] = "limit"
            info["price"] = price
            info["size"] = quantity
        elif order_type == ORDER_TYPE_MARKET:
            info["type"] = "market"
            if action == ORDER_ACTION_BUY:
                info["notional"] = quantity  # 买金额.
            else:
                info["size"] = quantity  # sell quantity.
        elif order_type == ORDER_TYPE_IOC:
            info["type"] = "limit"
            info["price"] = price
            info["size"] = quantity
            info["order_type"] = "3"
        else:
            logger.error("order_type error! order_type:", order_type, caller=self)
            return None
        result, error = await self.request("POST", "/api/spot/v3/orders", body=info, auth=True)
        return result, error

    async def revoke_order(self, symbol, order_no):
        """ Cancelling an unfilled order.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        body = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/cancel_orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("POST", uri, body=body, auth=True)
        if error:
            return order_no, error
        if result["result"]:
            return order_no, None
        return order_no, result

    async def revoke_orders(self, symbol, order_nos):
        """ Cancelling multiple open orders with order_id，Maximum 10 orders can be cancelled at a time for each
            trading pair.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_nos: order IDs.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if len(order_nos) > 10:
            logger.warn("only revoke 10 orders per request!", caller=self)
        body = [
            {
                "instrument_id": symbol,
                "order_ids": order_nos[:10]
            }
        ]
        result, error = await self.request("POST", "/api/spot/v3/cancel_batch_orders", body=body, auth=True)
        return result, error

    async def get_open_orders(self, symbol, limit=100):
        """ Get order details by order ID.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            limit: order count to return, max is 100, default is 100.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/spot/v3/orders_pending"
        params = {
            "instrument_id": symbol,
            "limit": limit
        }
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

    async def get_order_status(self, symbol, order_no):
        """ Get order status.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

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
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)

        if auth:
            time_str = str(time.time())
            timestamp = time_str.split(".")[0] + "." + time_str.split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)

            if not headers:
                headers = {}
            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _, success, error = await AsyncHttpRequests.fetch(method, url, data=body, headers=headers, timeout=10)
        return success, error


class OKExSpotTrader:
    """ OKEx Trade module
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
        if not kwargs.get("host"):
            kwargs["host"] = "https://www.okex.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://real.okex.com:8443"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("passphrase"):
            e = Error("param passphrase miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs.get("account")
        self._strategy = kwargs.get("strategy")
        self._platform = OKEX
        self._symbol = kwargs.get("symbol")
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs.get("access_key")
        self._secret_key = kwargs.get("secret_key")
        self._passphrase = kwargs.get("passphrase")
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        self._order_channel = "spot/order:{symbol}".format(symbol=self._symbol)

        url = self._wss + "/ws/v3"
        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )

        self._assets = {}  # Asset object. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order objects. e.g. {"order_no": Order, ... }

        # Initializing our REST API client.
        self._rest_api = OKExSpotRestAPI(self._access_key, self._secret_key, self._passphrase)
        LoopRunTask.register(self._send_heartbeat_msg, 5)

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    async def _send_heartbeat_msg(self, *args, **kwargs):
        data = "ping"
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        result, error = await self._rest_api.create_order(action, symbol, price, quantity, order_type)
        if error:
            return None, error
        if not result["result"]:
            return None, result
        return result["order_id"], None

    async def revoke_order(self, symbol, *order_nos):
        """ 撤销订单
        @param symbol 交易对
        @param order_nos 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单
        备注:关于批量删除订单函数返回值格式,如果函数调用失败了那肯定是return None, error
        如果函数调用成功,但是多个订单有成功有失败的情况,比如输入3个订单id,成功2个,失败1个,那么
        返回值统一都类似:
        return [(成功订单ID, None),(成功订单ID, None),(失败订单ID, "失败原因")], None
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol
        if len(order_nos) == 0:
            success, error = await self._rest_api.get_open_orders(symbol)
            if error:
                return False, error
            order_nos = []
            for order_info in success:
                order_nos.append(order_info["order_id"])

        if len(order_nos) == 0:
            return [], None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(symbol, order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            """
            {
            "BTC-USDT":[
            {
               "result":true,
                "client_oid":"a123",
                "order_id": "2510832677225473"
             },
             {
               "result":true,
                "client_oid":"a1234",
                "order_id": "2510832677225474"
             }]
            }
            """
            s, e = await self._rest_api.revoke_orders(symbol, order_nos)
            if e:
                return [], e
            result = []
            for d in s.get(symbol):
                if d["result"]:
                    result.append((d["order_id"], None))
                else:
                    result.append((d["order_id"], d["error_message"]))
            return result, None

    async def get_open_order_nos(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(symbol)
        if error:
            return None, error
        else:
            if len(success) > 100:
                logger.warn("order length too long! (more than 100)", caller=self)
            order_nos = []
            for order_info in success:
                order_nos.append(order_info["order_id"])
            return order_nos, None

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_account_balance()
        if error:
            return None, error
        ast = self._convert_asset_format(success)
        return ast, None

    async def connected_callback(self):
        """After websocket connection created successfully, we will send a message to server for authentication."""
        time_str = str(time.time())
        timestamp = time_str.split(".")[0] + "." + time_str.split(".")[1][:3]
        message = timestamp + "GET" + "/users/self/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf8"), digestmod="sha256")
        d = mac.digest()
        signature = base64.b64encode(d).decode()
        data = {
            "op": "login",
            "args": [self._access_key, self._passphrase, timestamp, signature]
        }
        await self._ws.send(data)

    async def _auth_success_callback(self):
        """ 授权成功之后回调
        """
        # 获取账户余额,更新资产
        success, error = await self._rest_api.get_account_balance()
        if error:
            logger.error("get_account_balance error: {}".format(error), caller=self)
            SingleTask.run(self._init_success_callback, False, "get_account_balance error: {}".format(error))
            # 初始化过程中发生错误,关闭网络连接,触发重连机制
            await self._ws.reconnect()
            return
        self._update_asset(success)

        # Fetch orders from server. (open + partially filled)
        success, error = await self._rest_api.get_open_orders(self._symbol)
        if error:
            logger.error("get open orders error: {}".format(error))
            SingleTask.run(self._init_success_callback, False, "get open orders error: {}".format(error))
            # 初始化过程中发生错误,关闭网络连接,触发重连机制
            await self._ws.reconnect()
            return
        for order_info in success:
            self._update_order(order_info)

        # Subscribe order channel.
        if self._order_update_callback:
            data = {
                "op": "subscribe",
                "args": self._order_channel
            }
            await self._ws.send(data)

        # 订阅账户余额通知
        if self._asset_update_callback:
            self._account_channel = []
            for s in self._symbol.split("-"):
                self._account_channel.append(f"spot/account:{s}")
            # 发送订阅
            data = {
                "op": "subscribe",
                "args": self._account_channel
            }
            await self._ws.send(data)

    async def process_binary(self, raw):
        """ Process binary message that received from websocket.

        Args:
            raw: Binary message received from websocket.

        Returns:
            None.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":
            return
        logger.debug("msg:", msg, caller=self)
        msg = json.loads(msg)

        # Authorization message received.
        if msg.get("event") == "login":
            if not msg.get("success"):
                logger.error("Websocket connection authorized failed: {}".format(msg), caller=self)
                SingleTask.run(self._init_success_callback, False, "Websocket connection authorized failed: {}".format(msg))
                return
            logger.info("Websocket connection authorized successfully.", caller=self)
            await self._auth_success_callback()

        # Subscribe response message received.
        elif msg.get("event") == "subscribe":
            # msg.get("channel")
            if msg.get("channel") == self._order_channel:
                SingleTask.run(self._init_success_callback, True, None)
            else:
                e = Error("subscribe order event error: {}".format(msg))
                SingleTask.run(self._init_success_callback, False, e)
            return

        elif msg.get("event") == "error":
            e = Error("subscribe order event error: {}".format(msg))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)

        # Order update message received.
        elif msg.get("table") == "spot/order":
            for data in msg["data"]:
                data["ctime"] = data["timestamp"]
                data["utime"] = data["last_fill_time"]
                self._update_order(data)

        elif msg.get("table") == "spot/account":
            self._update_asset(msg["data"])

    def _convert_asset_format(self, data):
        for d in data:
            c = d["currency"]
            self._assets[c]["free"] = float(d["available"])
            self._assets[c]["locked"] = float(d["hold"])
            self._assets[c]["total"] = float(d["balance"])
        return Asset(self._platform, self._account, self._assets, tools.get_cur_timestamp_ms(), True)

    def _update_asset(self, data):
        ast = self._convert_asset_format(data)
        SingleTask.run(self._asset_update_callback, ast)

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            None.
        """
        if order_info.get("margin_trading") and order_info["margin_trading"] != "1":  # 1.币币交易订单 2.杠杆交易订单
            return
        order_no = str(order_info["order_id"])
        state = order_info["state"]
        remain = float(order_info["size"]) - float(order_info["filled_size"])
        ctime = tools.utctime_str_to_mts(order_info["ctime"])
        utime = tools.utctime_str_to_mts(order_info["utime"])

        if state == "-2":
            status = ORDER_STATUS_FAILED
        elif state == "-1":
            status = ORDER_STATUS_CANCELED
        elif state == "0":
            status = ORDER_STATUS_SUBMITTED
        elif state == "1":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "2":
            status = ORDER_STATUS_FILLED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return None

        order = self._orders.get(order_no)
        if order:
            order.remain = remain
            order.status = status
            order.price = order_info["price"]
        else:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": order_info["size"],
                "remain": remain,
                "status": status,
                "avg_price": order_info["price"]
            }
            order = Order(**info)
            self._orders[order_no] = order
        order.ctime = ctime
        order.utime = utime

        SingleTask.run(self._order_update_callback, copy.copy(order))

        if status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

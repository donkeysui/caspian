# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/22 10:40
  @ Description: 
  @ History:
"""
import time
import zlib
import copy
import hmac
from typing import Dict
from itertools import zip_longest
from requests import Request
from xuanwu.model.symbol_info import SymbolInfo

from xuanwu.utils.websocket import Websocket
from xuanwu.model.market import Orderbook, Ticker
from xuanwu.model.market import Trade
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask, LoopRunTask
from xuanwu.const import FTX
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *


class FTXRestApi:
    """ FTX REST API Client.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        subaccount_name: Account's Subaccount Name
    """

    def __init__(self, host, access_key=None, secret_key=None, subaccount_name=None) -> None:
        self._host = host
        self._api_key = access_key
        self._api_secret = secret_key
        self._subaccount_name = subaccount_name

    async def get_all_markets(self):
        """ 获取全市场信息，列出所有产品市场成交、类型、价格变化量等数据
        Attributes:
            None

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', 'markets')
        return success, error

    async def get_all_futures(self):
        """ 列出所有期货交易币对详细信息
        Attributes:
            None

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', 'futures')
        return success, error

    async def get_future(self, symbol: str):
        """ 获取单个期货交易对详细信息
        Attributes:
            :param symbol: 交易对信息

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', f'futures/{symbol}')
        return success, error

    async def get_funding_rate(self, symbol: str):
        """ 获取当前资金费率
        Attributes:
            :param symbol: 永续合约币对

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = f"/futures/{symbol}/stats"
        success, error = await self._request("GET", uri)
        return success, error

    async def get_funding_rate_history(self, symbol: str, start: int, end: int):
        """ 获取某一永续合约资金费率历史
        Attributes:
            :param symbol: 永续合约币对信息
            :param start: 开始时间
            :param end: 结束时间

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "funding_rates"
        params = {
            "future": symbol,
            "start_time": start,
            "end_time": end
        }
        success, error = await self._request("GET", uri, params=params)

        return success, error

    async def get_orderbook(self, symbol: str, depth: int = 20):
        """ 获取单个品种订单簿数据，默认返回20挡深度数据，最大返回100档深度
        Attributes：
            :param symbol: 产品名称
            :param depth: 市场深度，不传返回默认深度数据

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if depth > 100:
            depth = 100
        success, error = await self._request('GET', f'markets/{symbol}/orderbook', params={'depth': depth})
        return success, error

    async def get_trades(self, symbol: str, start: int = 0, end: int = 0):
        """ 获取市场成交信息, 默认返回20条成交历史数据
        Attributes：
            :param symbol: 产品名称
            :param start: 开始时间
            :param end: 结束时间

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = None
        if start != 0 and end != 0:
            params = {
                "start_time": start,
                "end_time": end
            }
        success, error = await self._request('GET', f'markets/{symbol}/trades', params=params)
        return success, error

    async def get_kline(self, symbol: str, resolution: int, limit: int = None, start_time: int = None,
                        end_time: int = None):
        """
        Attributes:
            :param symbol: 交易产品名称
            :param resolution: K线级别，15, 60, 300, 900, 3600, 14400, 86400, or any multiple of 86400 up to 30*86400
            :param limit: 返回条目数
            :param start_time: 开始时间
            :param end_time: 结束时间

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {'resolution': resolution}
        if limit:
            params["limit"] = limit
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        success, error = await self._request('GET', f'markets/{symbol}/candles', params=params)

        return success, error

    async def get_account_info(self):
        """获取账户信息, 账户资产和账户持仓信息
        Attributes:
            None
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', 'account')
        return success, error

    async def get_positions(self, symbol: str = "", show_avg_price: bool = False):
        """ 获取头寸仓位信息，支持单一和多个查询。
        Attributes:
            :param symbol: 交易产品名称，不传此参数则返回所有仓位
            :param show_avg_price: 是否显示均价

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {'showAvgPrice': str(show_avg_price)}
        success, error = await self._request('GET', 'positions', params=params)
        if symbol != "" and success["success"]:
            for r in success["result"]:
                if symbol == r["future"]:
                    d = {
                        'success': True,
                        'result': [r]
                    }
                    return d, error
            return None, error
        return success, error

    async def get_asset_info(self):
        """ 获取账户资产详情
        Attributes：
            None

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', 'wallet/balances')
        return success, error

    async def get_open_orders(self, symbol: str):
        """ 获取挂单信息
        Attributes
            :param symbol: 交易产品信息

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._request('GET', f"orders?market={symbol}")
        return success, error

    async def get_conditional_orders(self, market: str, type_: str = ""):
        """ 获取未完成条件挂单，限价止损，移动止损，止盈，限价止盈等
        Attributes
            :param market:
            :param type_:例如： stop, trailing_stop, or take_profit等

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {}
        if type_ != "":
            params["type"] = type_
        success, error = await self._request('GET', f'conditional_orders?market={market}', params=params)
        return success, error

    async def place_order(self, symbol: str, side: str, price: float, size: float,
                          type_: str = ORDER_TYPE_LIMIT.lower(),
                          reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                          client_id: str = None):
        """ 创建订单
        Attributes：
            :param symbol:交易币对名称
            :param side: 交易方向 "buy" or "sell"
            :param price: 价格
            :param size: 数量
            :param type_: 订单类型 "limit" or "market"
            :param reduce_only: 是否只减仓，true 或 false
            :param ioc: 立即成交并取消剩余，true 或 false
            :param post_only: 只做maker，true 或 false
            :param client_id: 客户端ID

        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "market": symbol,
            'side': side,
            'price': price,
            'size': size,
            'type': type_,
            'reduceOnly': reduce_only,
            'ioc': ioc,
            'postOnly': post_only,
            'clientId': client_id
        }

        success, error = await self._request('POST', 'orders', json=params)
        return success, error

    async def cancel_order(self, order_no: str = "", client_no: str = ""):
        """ 取消订单
        Attributes：
            :param order_no: 订单ID
            :param client_no: 客户端ID

        :return:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if order_no == "" and client_no == "":
            logger.error("order_no and client_no不可同时为空")
        uri = ""
        if order_no != "":
            uri = f'orders/{order_no}'
        if client_no:
            uri = f"/orders/by_client_id/{client_no}"
        success, error = await self._request('DELETE', uri)
        return success, error

    async def cancel_orders(self, symbol: str = None, side: str = "", conditional_orders: bool = False,
                            limit_orders: bool = False):
        """取消所有挂单，此操作会取消您设置的条件委托（止损委托和跟踪止损委托）
        Attributes：
            :param symbol: 交易对名称
            :param side: 交易方向 "buy" or "sell"
            :param conditional_orders: 是否取消条件挂单, 默认为False
            :param limit_orders: 仅仅取消现有的限价单, 默认为False

        :return:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "market": symbol,
            "side": side,
            'conditionalOrdersOnly': conditional_orders,
            'limitOrdersOnly': limit_orders
        }

        success, error = await self._request('DELETE', 'orders', json=params)
        return success, error

    async def get_fills(self):
        """获取账户成交信息
        Attributes：
            None

        :return:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = 'fills'
        success, error = await self._request('GET', uri)
        return success, error

    async def _request(self, method: str, path: str, **kwargs):
        url = self._host + "/api/" + path
        request = Request(method, url, **kwargs)
        if self._api_key and self._api_secret:
            self._sign_request(request)
        _, success, error = await AsyncHttpRequests.fetch(method, url, headers=request.headers, timeout=10, **kwargs)
        return success, error

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = self._subaccount_name


class FTXMarket(Websocket):
    """ FTX websocket接口封装
    """

    def __init__(self, **kwargs):
        """ 基础数据初始化，基于websocket的数据连接参数初始化操作
        参数详情：
            symbol:                 订阅品种信息
            host：                  rest请求地址
            wss：                   websocket订阅地址
            channel_type：          订阅频道信息，限定只能为spot、futures、swap
            channel_detail：        订阅频道详情，只提供orderbook5和ticker
            ticker_update_callback：ticker数据回调函数，每100ms推送一次数据
            orderbook_update_callback： orderbook数据回调函数，有深度变化100毫秒推送一次
            trade_update_callback:  trade数据回调函数，有成交数据就推送
            orderbook_num:              orderbook_num只有获取市场深度数据的时候需要设置,其他时候不需要传递
        """
        self._platform = kwargs["platform"]
        self._wss = "wss://ftx.com"
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = list(set(kwargs.get("channels")))
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._ticker_update_callback = kwargs.get("ticker_update_callback")

        self._orderbook = {'op': 'ping'}

        self.heartbeat_msg = "ping"

        url = self._wss + "/ws/"
        # 连接WS对象
        super(FTXMarket, self).__init__(url, send_hb_interval=15)
        self.initialize()

    async def connected_callback(self):
        """ 连接成功之后进行行情订阅
        Attributes:
            None

        :returns:
            None
        """
        for ch in self._channels:
            if ch == "orderbook":
                for symbol in self._symbols:
                    orderbook_sub = {
                        'channel': 'orderbook',
                        "market": symbol,
                        "op": "subscribe"
                    }
                    await self.ws.send_json(orderbook_sub)
            elif ch == "trade":
                for symbol in self._symbols:
                    trade_sub = {
                        'channel': 'trades',
                        'market': f"{symbol}",
                        "op": "subscribe"
                    }
                    await self.ws.send_json(trade_sub)
            elif ch == "ticker":
                for symbol in self._symbols:
                    trade_sub = {
                        'channel': 'ticker',
                        'market': f"{symbol}",
                        "op": "subscribe"
                    }
                    await self.ws.send_json(trade_sub)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process(self, msg):
        """只继承不实现"""
        pass

    @async_method_locker("FTXMarket.process_binary.locker")
    async def process_binary(self, raw: Dict):
        """ 接受来自websocket的数据.
        Attributes:
            raw: 来自websocket的数据流.

        :returns:
            None.
        """
        if not isinstance(raw, dict):
            return

        type_ = raw.get("type")

        if type_ == "pong":
            return
        elif type_ == "error":
            logger.error(f"Websocket connection failed: {raw}", caller=self)
        elif type_ == "info":
            if raw["msg"] == 20001:
                # 交易所重启了,我们就断开连接,websocket会自动重连
                logger.error("交易所重启了,我们就断开连接,websocket会自动重连")
                SingleTask.run(self._reconnect)

        if type_ == "subscribed":
            channel = raw.get("channel")
            if channel and channel == "trades":
                logger.info("trades订阅成功.......")
            elif channel and channel == "orderbook":
                logger.info("orderbook订阅成功.......")

        elif type_ == "partial" or "update":
            channel = raw.get("channel")
            if channel and channel == "orderbook":
                await self._process_orderbook(raw)
            elif channel and channel == "trades":
                await self._process_trade(raw)
            elif channel and channel == "ticker":
                await self._process_ticker(raw)

    async def _process_ticker(self, data):
        """ K线数据格式化
        Attributes：
            :param data: K线原始数据

        :returns:
            None
        """
        market = data["market"]
        if market in self._symbols:
            ts = int(float(data["data"]["time"]) * 1000)  # 转变为毫秒
            p = {
                "platform": self._platform,
                "symbol": data["market"],
                "ask": data["data"]["ask"],
                "bid": data["data"]["bid"],
                "last": data["data"]["last"],
                "timestamp": ts
            }
            ticker = Ticker(**p)
            SingleTask.run(self._ticker_update_callback, copy.copy(ticker))

    async def _process_orderbook(self, data):
        """ orderbook 数据解析、封装、回调
        Attributes:
            :param data: WS推送数据

        :returns:
            :return: None
        """
        symbol = data.get("market")
        data = data["data"]
        action = data.get("action")
        timestamp = data["time"]
        checksum = data['checksum']

        bids = data["bids"]
        asks = data["asks"]

        if symbol in self._symbols:
            if action == "partial":
                ob = Orderbook(symbol=symbol, platform=FTX, )
                ob.bids = bids
                ob.asks = asks
                ob.timestamp = timestamp
                # print(timestamp + '推送数据的checksum为：' + str(checksum))
                check_num = self.check(bids, asks)
                # print(timestamp + '校验后的checksum为：' + str(check_num))
                if check_num == checksum:
                    logger.info("订单簿首次推送校验结果为：True", caller=self)
                    self._orderbook[symbol] = ob

                    if self._orderbook_update_callback:
                        d = copy.copy(ob)
                        d.asks = d.asks[:self._orderbook_length]
                        d.bids = d.bids[:self._orderbook_length]
                        SingleTask.run(self._orderbook_update_callback, d)
                else:
                    # 发送订阅
                    logger.info("校验错误，重新连接WS......", caller=self)
                    SingleTask.run(self._reconnect)

            if action == "update":
                # 获取全量数据
                bids_p = self._orderbook[symbol].bids
                asks_p = self._orderbook[symbol].asks
                bids_p = self.update_bids(bids, bids_p)
                asks_p = self.update_asks(asks, asks_p)

                self._orderbook[symbol].bids = bids_p
                self._orderbook[symbol].asks = asks_p

                check_num = self.check(bids_p, asks_p)

                if check_num == checksum:
                    # logger.debug("Update 订单簿更新推送校验结果为：True", caller=self)
                    if self._orderbook_update_callback:
                        d = copy.copy(self._orderbook[symbol])

                        d.asks = d.asks[:self._orderbook_length]
                        d.bids = d.bids[:self._orderbook_length]

                        SingleTask.run(self._orderbook_update_callback, d)
                else:
                    logger.info(f"{symbol}, Update 校验结果为：False，正在重新订阅……", caller=self)

                    # 发送订阅
                    SingleTask.run(self._reconnect)

    async def _process_trade(self, data):
        """ trade 数据解析、封装、回调
        Attributes:
            :param data: WS推送数据

        :returns:
            :return: None
        """
        symbol = data.get("market")
        data = data.get("data")
        if symbol in self._symbols:
            # 返回数据封装，加上时间戳和品种交易所信息
            for dt in data:
                trade = Trade()
                trade.platform = self._platform
                trade.symbol = symbol
                trade.price = dt["price"]
                trade.quantity = dt["size"]
                trade.side = dt["side"]
                trade.trade_id = dt["id"]
                trade.timestamp = dt['time']

                # 异步回调
                SingleTask.run(self._trade_update_callback, trade)

    # 订单簿增量数据相关校验、拼接等方法
    def update_bids(self, res, bids_p):
        """ 更新全量Bids数据
        Attributes:
            :param res: 原始Bids数据
            :param bids_p: 更新的Bids数据

        :returns:
            :return: 返回新的全量Bids数据
        """
        # 获取增量bids数据
        bids_u = res
        # bids合并
        for i in bids_u:
            bid_price = i[0]
            for j in bids_p:
                if bid_price == j[0]:
                    if i[1] == 0:
                        bids_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != 0:
                    bids_p.append(i)
        else:
            bids_p.sort(key=lambda price: self.sort_num(str(price[0])), reverse=True)
        return bids_p

    def update_asks(self, res, asks_p):
        """ 更新全量Asks数据
        Attributes:
            :param res: 原始Asks数据
            :param asks_p: 更新的Asks数据

        :returns:
            :return: 返回新的全量Asks数据
        """
        # 获取增量asks数据
        asks_u = res
        # asks合并
        for i in asks_u:
            ask_price = i[0]
            for j in asks_p:
                if ask_price == j[0]:
                    if i[1] == 0:
                        asks_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != 0:
                    asks_p.append(i)
        else:
            asks_p.sort(key=lambda price: self.sort_num(str(price[0])))
        return asks_p

    def sort_num(self, n):
        """ 排序函数
        Attributes:
            :param n: 数据对象

        :returns:
            :return: 排序结果
        """
        if n.isdigit():
            return int(n)
        else:
            return float(n)

    def check(self, bids, asks):
        """ 首次接受订单簿对订单簿数据进行校验
        Attributes:
            :param bids: 全量bids数据
            :param asks: 全量asks数据

        :returns:
            :return: 返回校验结果
        """
        checksum_data = [
            ':'.join([f'{float(order[0])}:{float(order[1])}' for order in (bid, offer) if order])
            for (bid, offer) in zip_longest(bids[:100], asks[:100])
        ]
        return int(zlib.crc32(':'.join(checksum_data).encode()))


class FTXTrade(Websocket):
    """ FTX Future Trade module. You can initialize trade object with some attributes in kwargs.
        回调函数要求：
                    order_update_callback：订单函数
                    position_update_callback：仓位回调，由于FTX不提供仓位ws推送，所以每次订单成交之后都会主动请求推送
                    asset_update_callback：资产回调
                    init_success_callback：初始化成功回调
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
            kwargs["host"] = "https://ftx.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://ftx.com"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("order_update_callback"):
            e = Error("param order_update_callback miss")
        if not kwargs.get("position_update_callback"):
            e = Error("param position_update_callback miss")
        if not kwargs.get("asset_update_callback"):
            e = Error("param asset_update_callback miss")
        if not kwargs.get("init_success_callback"):
            e = Error("param init_success_callback miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = FTX
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._subaccount_name = kwargs.get("subaccount_name")
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        self._assets = {}  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(platform=self._platform,
                                  account=self._account,
                                  strategy=self._strategy,
                                  symbol=self._symbol)
        self._symbol_info = SymbolInfo(platform=self._platform)

        self._subscribe_order_ok = False
        self._subscribe_asset_ok = False
        self._subscribe_response_count = 0

        self.heartbeat_msg = {'op': 'ping'}

        self._rest_api = FTXRestApi(
            host=self._host,
            access_key=self._access_key,
            secret_key=self._secret_key,
            subaccount_name=self._subaccount_name)
        url = self._wss + "/ws/"
        # 连接WS对象
        super(FTXTrade, self).__init__(url, send_hb_interval=15)
        self.initialize()

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def symbol_info(self):
        return copy.copy(self._symbol_info)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def position(self):
        return copy.copy(self._position)

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        """网络链接成功回调
        """
        # 账号不为空就要进行登录认证,然后订阅2个需要登录后才能订阅的私有频道:用户挂单通知和挂单成交通知(FTX只支持这2个私有频道)
        """FTX的websocket接口真是逗逼,验证成功的情况下居然不会返回任何消息"""
        ts = int(time.time() * 1000)
        signature = hmac.new(self._secret_key.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
        args = {
            'key': self._access_key,
            'sign': signature,
            'time': ts
        }
        # 如果是子账户,就添加相应字段
        if self._subaccount_name:
            args["subaccount"] = self._subaccount_name
        data = {'op': 'login', 'args': args}
        await self.ws.send_json(data)
        # 登录请求发送之后请求账户详情
        await self._login_callback()

    async def process(self, msg):
        pass

    async def process_binary(self, msg):
        """ Process message that received from websocket.

        Args:
            msg: message received from websocket.

        Returns:
            None.
        """
        if not isinstance(msg, dict):
            return
        logger.debug("msg:", json.dumps(msg), caller=self)
        # {"type": "error", "code": 400, "msg": "Invalid login credentials"}
        if msg["type"] == "error":
            SingleTask.run(self._init_success_callback, False, "Websocket connection failed: {}".format(msg))
        elif msg["type"] == "pong":
            return
        elif msg["type"] == "info":
            if msg["code"] == 20001:
                # 交易所重启了,我们就断开连接,websocket会自动重连
                SingleTask.run(self._reconnect)
        elif msg["type"] == "unsubscribed":
            return
        # {'type': 'subscribed', 'channel': 'trades', 'market': 'BTC-PERP'}
        elif msg["type"] == "subscribed":
            self._subscribe_response_count = self._subscribe_response_count + 1  # 每来一次订阅响应计数就加一
            if self._subscribe_response_count == 2:  # 所有的订阅都成功了,通知上层接口都准备好了
                SingleTask.run(self._init_success_callback, True, "Environment ready")
        elif msg["type"] == "update":
            channel = msg['channel']
            if channel == 'orders':
                await self._update_order(msg)
            elif channel == 'fills':
                await self._update_fill(msg)

    async def _login_callback(self):
        """ FTX的websocket接口真是逗逼,验证成功的情况下居然不会返回任何消息
            登录成功之后需要拉去相关账户信息
        """
        success, error = await self._rest_api.get_all_markets()
        if error:
            logger.error(f"list_markets error: {error}", caller=self)
            # 初始化过程中发生错误,关闭网络连接,触发重连机制
            await self.ws.close()
            return
        for info in success["result"]:
            if info["name"] == self._symbol:
                self.get_symbol_info(sym_info=info)  # 全局币对详细信息，全局存储随掉随用

        # 查询当前挂单信息
        orders, error = await self.get_orders(self._symbol)
        if error:
            logger.error(f"get_orders error: {error}", caller=self)
            # 初始化过程中发生错误,关闭网络连接,触发重连机制
            return
        for o in orders:
            SingleTask.run(self._order_update_callback, o)

        pos, error = await self.get_position(self._symbol)
        if error:
            logger.error(f"get_position error: {error}", caller=self)
            return
        SingleTask.run(self._position_update_callback, copy.copy(self._position))

        ast, error = await self.get_assets()
        if error:
            logger.error(f"get_assets error: {error}")
            return
        SingleTask.run(self._asset_update_callback, ast)

        # 订阅ws的订单数据
        if self._order_update_callback is not None:
            # `用户挂单通知回调`不为空,就进行订阅
            await self.ws.send_json({'op': 'subscribe', 'channel': 'orders'})
            # `用户挂单成交通知回调`不为空,就进行订阅
            await self.ws.send_json({'op': 'subscribe', 'channel': 'fills'})

    def get_symbol_info(self, sym_info):
        """ 获取指定符号相关信息

        Args:
            :param sym_info: 产品币对详情

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        self._symbol_info.price_tick = float(sym_info["priceIncrement"])
        self._symbol_info.size_tick = float(sym_info["sizeIncrement"])
        self._symbol_info.size_limit = None  # 原始数据中没有
        self._symbol_info.value_tick = None  # 原始数据中没有
        self._symbol_info.value_limit = None  # 原始数据中没有
        if sym_info["type"] == "future":
            self._symbol_info.base_currency = sym_info["underlying"]
            self._symbol_info.quote_currency = "USD"
            self._symbol_info.settlement_currency = "USD"
        else:  # "spot"
            self._symbol_info.base_currency = sym_info["baseCurrency"]
            self._symbol_info.quote_currency = sym_info["quoteCurrency"]
            self._symbol_info.settlement_currency = sym_info["quoteCurrency"]
        self._symbol_info.symbol_type = sym_info["type"]
        self._symbol_info.is_inverse = False
        self._symbol_info.multiplier = 1

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ Create an order.

        Args:
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.
            {
                "result":
                    {
                        "avgFillPrice": null,
                        "clientId": null,
                        "createdAt": "2019-11-16T11:08:37.726313+00:00",
                        "filledSize": 0.0,
                        "future": "ETH-PERP",
                        "id": 871282987,
                        "ioc": false,
                        "market": "ETH-PERP",
                        "postOnly": false,
                        "price": 251.0,
                        "reduceOnly": false,
                        "remainingSize": 0.02,
                        "side": "sell",
                        "size": 0.02,
                        "status": "new",
                        "type": "limit"
                    },
                "success": true
            }

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

        if action == ORDER_ACTION_BUY:
            side = "buy"
        else:
            side = "sell"

        size = abs(float(quantity))
        price = float(price)

        if order_type == ORDER_TYPE_LIMIT:
            ot = "limit"
        elif order_type == ORDER_TYPE_MARKET:
            ot = "market"
            price = None
        else:
            raise NotImplementedError

        success, error = await self._rest_api.place_order(market=symbol, side=side, price=price, size=size, type_=ot)

        if error:
            return None, error

        if not success["success"]:
            return None, "place_order error"

        result = success["result"]

        return str(result["id"]), None

    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items.
                       If you set 0 param, you can cancel all orders for
            this symbol. If you set 1 or multiple param, you can cancel an or multiple order.

        Returns:
            删除全部订单情况: 成功=(True, None), 失败=(False, error information)
            删除单个或多个订单情况: (删除成功的订单id[], 删除失败的订单id及错误信息[]),比如删除三个都成功那么结果为([1xx,2xx,3xx], [])
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol.
        if len(order_nos) == 0:
            success, error = await self._rest_api.cancel_orders(symbol)
            if error:
                return False, error
            if not success["success"]:
                return False, "cancel_orders error"
            return True, None
        # If len(order_nos) > 0, you will cancel an or multiple orders.
        else:
            result = []
            for order_no in order_nos:
                _, e = await self._rest_api.cancel_order(order_no)
                if e:
                    result.append((order_no, e))
                else:
                    result.append((order_no, None))
            return tuple(result), None

    async def get_assets(self):
        """ 获取交易账户资产信息
        Args:
            {
                "result":
                    {
                        "backstopProvider": false,
                        "collateral": 110.094266926,
                        "freeCollateral": 109.734306926,
                        "initialMarginRequirement": 0.2,
                        "leverage": 5.0, "liquidating": false,
                        "maintenanceMarginRequirement": 0.03,
                        "makerFee": 0.0002,
                        "marginFraction": 61.1703338848761,
                        "openMarginFraction": 61.170278323147016,
                        "positionLimit": null,
                        "positionLimitUsed": 2.15976,
                        "positions":
                            [
                                {
                                    "collateralUsed": 0.35996,
                                    "cost": -1.7999,
                                    "entryPrice": 179.99,
                                    "estimatedLiquidationPrice": 11184.0172926,
                                    "future": "ETH-PERP",
                                    "initialMarginRequirement": 0.2,
                                    "longOrderSize": 0.0,
                                    "maintenanceMarginRequirement": 0.03,
                                    "netSize": -0.01, "openSize": 0.01,
                                    "realizedPnl": 0.01723393,
                                    "shortOrderSize": 0.0,
                                    "side": "sell",
                                    "size": 0.01,
                                    "unrealizedPnl": 0.0001
                                }
                            ],
                        "takerFee": 0.0007,
                        "totalAccountValue": 110.094366926,
                        "totalPositionSize": 1.7998,
                        "useFttCollateral": true,
                        "username": "8342537@qq.com"
                    },
                "success": true
            }
        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

        success, error = await self._rest_api.get_balances()
        if error:
            return None, error

        if not success["success"]:
            return None, "get_account_info error"

        data = success["result"]

        assets = {}
        for i in data:
            total = float(i["total"])
            free = float(i["free"])
            locked = total - free
            assets[i["coin"]] = {
                "total": total,
                "free": free,
                "locked": locked
            }

        if assets == self._assets:
            update = False
        else:
            update = True
        self._assets = assets

        timestamp = tools.get_cur_timestamp_ms()
        ast = Asset(
            platform=self._platform,
            account=self._account,
            assets=self._assets,
            timestamp=timestamp,
            update=update)
        return ast, None

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

        orders = []

        success, error = await self._rest_api.get_open_orders(symbol)
        if error:
            return None, error

        if not success["success"]:
            return None, "get_open_orders error"

        data = success["result"]
        for o in data:
            order = await self._convert_order_format(o)
            if order is None:
                return None, "get_open_orders error"
            orders.append(order)

        return orders, None

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target
            {
                "result":
                    [
                        {
                            "collateralUsed": 0.35986,
                            "cost": -1.7984, "entryPrice": 179.84,
                            "estimatedLiquidationPrice": 11184.0123266,
                            "future": "ETH-PERP",
                            "initialMarginRequirement": 0.2,
                            "longOrderSize": 0.0,
                            "maintenanceMarginRequirement": 0.03,
                            "netSize": -0.01,
                            "openSize": 0.01,
                            "realizedPnl": 0.01866927,
                            "recentAverageOpenPrice": 179.84,
                            "recentPnl": -0.0009,
                            "shortOrderSize": 0.0,
                            "side": "sell",
                            "size": 0.01,
                            "unrealizedPnl": -0.0009
                        }
                    ],
                "success": true
            }

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

        success, error = await self._rest_api.get_positions()
        if error:
            return None, error
        if not success["success"]:
            return None, "get_position error"
        p = next(filter(lambda x: x['future'] == symbol, success["result"]), None)
        if p is None:
            return Position(self._platform, self._account, self._strategy, symbol), None
        elif p["netSize"] == 0:
            return Position(self._platform, self._account, self._strategy, symbol), None
        pos = Position(self._platform, self._account, self._strategy, symbol)

        pos.utime = tools.get_cur_timestamp_ms()
        if p["netSize"] < 0:  # 空头仓位
            pos.long_quantity = 0
            pos.long_avail_qty = 0
            pos.long_open_price = 0
            pos.long_liquid_price = 0
            pos.long_unrealised_pnl = 0
            pos.long_leverage = 0
            pos.long_margin = 0

            pos.short_quantity = abs(p["netSize"])
            pos.short_avail_qty = pos.short_quantity - p["longOrderSize"] if p["longOrderSize"] < pos.short_quantity else 0
            pos.short_open_price = p["recentAverageOpenPrice"]
            pos.short_liquid_price = p["estimatedLiquidationPrice"]
            pos.short_unrealised_pnl = p["unrealizedPnl"]
            pos.short_leverage = int(1 / p["initialMarginRequirement"])
            pos.short_margin = p["collateralUsed"]
        else:  # 多头仓位
            pos.long_quantity = abs(p["netSize"])
            pos.long_avail_qty = pos.long_quantity - p["shortOrderSize"] if p["shortOrderSize"] < pos.long_quantity else 0
            pos.long_open_price = p["recentAverageOpenPrice"]
            pos.long_hold_price = p["entryPrice"]
            pos.long_liquid_price = p["estimatedLiquidationPrice"]
            pos.long_unrealised_pnl = p["unrealizedPnl"]
            pos.long_leverage = int(1 / p["initialMarginRequirement"])
            pos.long_margin = p["collateralUsed"]
            #
            pos.short_quantity = 0
            pos.short_avail_qty = 0
            pos.short_open_price = 0
            pos.short_hold_price = 0
            pos.short_liquid_price = 0
            pos.short_unrealised_pnl = 0
            pos.short_leverage = 0
            pos.short_margin = 0

        return pos, None

    def _convert_order_format(self, o):
        """将交易所订单结构转换为本交易系统标准订单结构格式
        """
        order_no = str(o["orderId"])
        state = o["status"]
        remain = float(o["remainingSize"])
        filled = float(o["filledSize"])
        size = float(o["size"])
        price = float(o["price"]) if o["price"] else None
        ctime = tools.utctime_str_to_mts(o['createdAt'], "%Y-%m-%dT%H:%M:%S.%f+00:00")
        avg_price = float(o["avgFillPrice"]) if o["avgFillPrice"] else 0
        if state == "new":
            status = ORDER_STATUS_SUBMITTED
        elif state == "open":
            if remain < size:
                status = ORDER_STATUS_PARTIAL_FILLED
            else:
                status = ORDER_STATUS_SUBMITTED
        elif state == "closed":
            if filled < size:
                status = ORDER_STATUS_CANCELED
            else:
                status = ORDER_STATUS_FILLED
        else:
            return None

        info = {
            "account": self._account,
            "platform": self._platform,
            "strategy": self._strategy,
            "order_no": order_no,
            "client_order_id": o["clientId"],
            "symbol": o["market"],
            "action": ORDER_ACTION_BUY if o["side"] == "buy" else ORDER_ACTION_SELL,
            "price": price,
            "quantity": size,
            "remain": remain,
            "status": status,
            "avg_price": avg_price,
            "order_type": ORDER_TYPE_LIMIT if o["type"] == "limit" else ORDER_TYPE_MARKET,
            "ctime": ctime,
            "utime": int(time.time() * 1000),
            "trade_quantity": size - remain
        }
        order = Order(**info)

        return order

    async def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            None.
        """
        o = order_info["data"]
        if o["market"] != self._symbol:
            return
        res = self._convert_order_format(o)
        SingleTask.run(self._order_update_callback, copy.copy(res))

        if res.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(res.order_no)

    async def _update_fill(self, fill_info):
        """ Fill update. 只用于校验订单完全成交

        Args:
            fill_info: Fill information.

        Returns:
            None.
        """
        data = fill_info["data"]
        if data["market"] != self._symbol:
            return
        order_no = str(data["orderId"])
        o = self._orders.get(order_no)
        if o:
            self._orders.pop(order_no)

# -*- coding: UTF-8 -*-
"""
  @ Author:   Turkey
  @ Email:    suiminyan@gmail.com
  @ Date:     2021/9/22 10:40
  @ Description: 
  @ History:
"""
import gzip
import json
import copy
import hmac
import base64
import time
import zlib
from xuanwu.model.asset import Asset
from xuanwu.model.order import Order
from xuanwu.model.position import Position
from xuanwu.error import Error
from xuanwu.utils import tools, logger
from xuanwu.tasks import SingleTask, LoopRunTask
from xuanwu.const import OKEX_FUTURE
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from xuanwu.model.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET, ORDER_TYPE_MAKER, ORDER_TYPE_FOK, ORDER_TYPE_IOC
from xuanwu.model.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED, TRADE_TYPE_BUY_OPEN, TRADE_TYPE_SELL_OPEN, TRADE_TYPE_BUY_CLOSE, \
    TRADE_TYPE_SELL_CLOSE
from urllib.parse import urljoin
from xuanwu.utils.http_client import AsyncHttpRequests
from collections import deque
from xuanwu.model.market import Orderbook, Kline, Trade

__all__ = ("OKEXFutureMarket", "OKExFutureRestApi", "OkexFutureTrade", )


HOST = "https://www.okex.com"


class OKEXFutureMarket:
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
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._depth_update_callback = kwargs.get("depth_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._init_callback = kwargs.get("init_callback")
        self._error_callback = kwargs.get("error_callback")

        self._orderbook = Orderbook(platform=self._platform)

        url = self._wss + "/ws/v3"
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
                for symbol in self._symbols:
                    kline_sub = {
                        "op": "subscribe",
                        "args": [f"futures/candle60s:{symbol}"]
                    }
                    await self._ws.send(kline_sub)
            elif ch == "orderbook":
                for symbol in self._symbols:
                    depth_sub = {
                        "op": "subscribe",
                        "args": [f"futures/depth_l2_tbt:{symbol}"]
                    }
                    await self._ws.send(depth_sub)
            elif ch == "trade":
                for symbol in self._symbols:
                    trade_sub = {
                        "op": "subscribe",
                        "args": [f"futures/trade:{symbol}"]
                    }
                    await self._ws.send(trade_sub)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    @async_method_locker("OKEXFutureMarket.process_binary.locker")
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
            if self._depth_update_callback:
                d = copy.copy(self._orderbook)
                asks = d.asks[:self._orderbook_length]
                bids = d.bids[:self._orderbook_length]

                d.asks = asks
                d.bids = bids
                SingleTask.run(self._depth_update_callback, d)

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
            if self._depth_update_callback:
                d = copy.copy(self._orderbook)
                asks = d.asks[:self._orderbook_length]
                bids = d.bids[:self._orderbook_length]

                d.asks = asks
                d.bids = bids

                SingleTask.run(self._depth_update_callback, d)

    async def _process_trade(self, data):
        """ trade 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        if self._trade_update_callback:
            # 返回数据封装，加上时间戳和品种交易所信息
            for dt in data:
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


class OKExFutureRestApi:
    """ OKEx合约交易REST API客户端.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
    """

    def __init__(self, access_key='', secret_key='', passphrase=''):
        """初始化REST API 接口所需参数."""
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase

    async def get_contract_info(self, symbol=None, contract_type=None, contract_code=None):
        """ 获取合约信息
        Args：
            :param symbol: 交易对信息. 默认为None。
            :param contract_type: 合约信息. 默认为None. 类型：'this_week'/'next_week'/'quarter'
            :param contract_code: 合约合约代码. e.g. BTC180914.

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.

        * 注意: 1. 如果`contract_code`参数不为None, 仅仅匹配contract_code所有信息.
                2. 如果`contract_code`参数为None, 匹配`symbol + contract_type`所有信息.
        """
        success, error = await self.request("GET", "/api/futures/v3/instruments", auth=False)
        if error:
            return None, error
        if contract_code:
            for i in success:
                if contract_code == i["instrument_id"]:
                    return i, error
        if contract_type and symbol:
            for i in success:
                if contract_type == i["alias"] and symbol == i["underlying"]:
                    return i, error

    async def get_price_limit(self, symbol):
        """ 获取合约当前交易的最高买价和最低卖价。此接口为公共接口，不需要身份验证。
        Args：
            :param symbol: 需要查询的合约名称

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.
        """
        success, error = await self.request("GET", f"/api/futures/v3/instruments/{symbol}/price_limit", auth=False)

        if error:
            return None, error
        else:
            return success, error

    async def get_kline(self, symbol, start='', end='', granularity=''):
        """ 获取合约信息
        Args：
            :param symbol: 交易对信息. 默认为None。
            :param granularity: 如[60/180/300/900/1800/3600/7200/14400/21600/43200/86400/604800]
            :param end: 结束时间（ISO 8601标准，例如：2018-06-20T02:31:00Z）
            :param start: 开始时间（ISO 8601标准，例如：2018-06-20T02:31:00Z）

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.

        * 注意: 1. 如果`contract_code`参数不为None, 仅仅匹配contract_code所有信息.
                2. 如果`contract_code`参数为None, 匹配`symbol + contract_type`所有信息.
        """
        uri = f'/api/futures/v3/instruments/{symbol}/candles'
        params = {}
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        if granularity:
            params['granularity'] = granularity
        success, error = await self.request("GET", uri, params)
        if error:
            return None, error
        return success, error

    async def get_orderbook(self, symbol, depth=None, limit=10):
        """ 获取订单簿信息
        Args：
            :param limit: 返回深度数量，最大值可传200，即买卖深度共400条
            :param depth: 按价格合并深度，例如：0.1，0.001
            :param symbol: 合约名称

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.

        """
        params = {
            "size": limit
        }
        if depth:
            params["depth"] = depth
        success, error = await self.request("GET", f"/api/futures/v3/instruments/{symbol}/book", params=params)

        return success, error

    async def get_asset_info(self):
        """ 获取所有合约的账户资产信息，当用户没有持仓时，保证金率为10000
        Args:
            NONE
        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        success, error = await self.request("GET", '/api/futures/v3/accounts', auth=True)
        return success, error

    async def get_position(self, instrument_id):
        """ 获取某个合约的持仓信息.
        Args:
            :param instrument_id: 合约名称ID, e.g. BTC-USD-180213.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        uri = "/api/futures/v3/{instrument_id}/position".format(instrument_id=instrument_id)
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def create_order(self, symbol, type, price, size, order_type="0", match_price="0", client_oid=None):
        """Create an order.
        Args:
            :param symbol: 合约ID，如BTC-USD-180213 ,BTC-USDT-191227
            :param type: 1:开多, 2:开空, 3:平多, 4:平空
            :param price: 下单价格
            :param size: 买入或卖出合约的数量（以张计数）
            :param client_oid: 客户端ID
            :param order_type:  0：普通委托（order type不填或填0都是普通委托）
                                1：只做Maker（Post only）
                                2：全部成交或立即取消（FOK）
                                3：立即成交并取消剩余（IOC）
                                4：市价委托
            :param match_price: 是否以对手价下单(0:不是; 1:是)

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        order_info = {
            "instrument_id": symbol,
            "type": type,
            "order_type": order_type,
            "price": price,
            "size": size,
            "match_price": match_price
        }
        if client_oid:
            order_info["client_oid"] = client_oid

        result, error = await self.request("POST", "/api/futures/v3/order", body=order_info, auth=True)
        return result, error

    async def create_orders(self, symbol, order_datas):
        """ 批量进行合约下单操作。每个合约可批量下10个单。
        Args:
            :param symbol: 合约代码
            :param order_datas: 订单List:
                                    JSON类型的字符串 例：[{order_type:"0",price:"5",size:"2",type:"1",match_price:"1"},
                                    {order_type:"0",price:"2",size:"3",type:"1",match_price:"1"}]
                                    最大下单量为10，当以对手价下单，order_type只能选择0（普通委托）。
                                    price, size, type, match_price 参数参考future_trade接口中的说明
        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.

        Note:
            限速规则：30次/2s
                    1）不同合约之间限速不累计；
                    2）同一合约的当周次周季度之间限速累计；
                    3）同一合约的币本位和USDT保证金之间限速不累计
        """
        if len(order_datas) > 10:
            return {"error", "order_data is to long, limit 10"}

        params = {
            "instrument_id": symbol,
            "orders_data": order_datas
        }

        success, error = await self.request("POST", "/api/futures/v3/orders", body=params, auth=True)

        return success, error

    async def revoke_order(self, symbol, order_no):
        """ 撤销指定订单.
        Args:
            :param symbol: 交易币对, e.g. BTC-USDT.
            :param order_no: 订单ID或者是客户端ID

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        uri = "/api/futures/v3/cancel_order/{symbol}/{order_no}".format(symbol=symbol, order_no=order_no)
        result, error = await self.request("POST", uri, auth=True)
        if error:
            return order_no, error
        if result["result"]:
            return order_no, None
        return order_no, result

    async def revoke_orders(self, symbol, order_ids=None, client_oids=None):
        """ 批量撤单, 根据指定的order_id或者是client_id列表撤销某个合约的未完成订单，
            order_id和client_id只能选择其一

        Args:
            :param client_oids: 客户ID列表
            :param order_ids:  订单ID列表
            :param symbol: 交易币对, e.g. BTC-USDT.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        ids = None
        key = ""

        uri = "/api/futures/v3/cancel_batch_orders/{symbol}".format(symbol=symbol)

        if order_ids or client_oids:
            ids = order_ids if order_ids else client_oids
            key = "order_ids" if order_ids else "client_oids"
        if len(ids) > 10:
            return {"error", "order_ids/client_ids is to long, limit 10"}
        else:
            body = {key: ids}
            result, error = await self.request("POST", uri, body=body, auth=True)
            return result, error

    async def revoke_orders_all(self, symbol):
        """ 撤销所有订单
        Args:
            :param symbol: 合约名称ID, e.g. BTC-USDT.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        order_ids = []
        success, error = await self.get_order_list(instrument_id=symbol, state=6)
        if error:
            return None, None
        if success["result"]:
            order_infos = success["order_info"]
            if len(order_infos) > 0:
                for order_info in order_infos:
                    order_ids.append(order_info["order_id"])
                res, err = await self.revoke_orders(symbol=symbol, order_ids=order_ids)
                if err:
                    return None, err
                return res, None

    async def get_order_list(self, instrument_id, state, after='', before='', limit=''):
        """ 列出您当前所有的订单信息。本接口能查询最近三个月的数据。这个请求支持分页，
            并且按委托时间倒序排序和存储，最新的排在最前面

        Args:
            :param before:
            :param after:
            :param instrument_id: Contract ID, e.g. BTC-USD-SWAP.
            :param state: Order state for filter. ("-2": Failed, "-1": Cancelled, "0": Open , "1": Partially Filled,
                    "2": Fully Filled, "3": Submitting, "4": Cancelling, "6": Incomplete(open + partially filled),
                    "7": Complete(cancelled + fully filled)).
            :param limit: Number of results per request. Maximum 100. (default 100)

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.

        TODO: Add args `from` & `to`.
        """
        uri = "/api/futures/v3/orders/{instrument_id}".format(instrument_id=instrument_id)
        params = {"state": state}
        if after:
            params['after'] = after
        if before:
            params['before'] = before
        if limit:
            params['limit'] = limit
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_order_info(self, instrument_id, order_id):
        """ Get order detail by order ID. Canceled unfilled orders will be kept in record for 2 hours only.

        Args:
            instrument_id: Contract ID, e.g. BTC-USD-180213.
            order_id: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/futures/v3/orders/{instrument_id}/{order_id}".format(
            instrument_id=instrument_id, order_id=order_id)
        success, error = await self.request("GET", uri, auth=True)
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
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(HOST, uri)

        if auth:
            timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
                           digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)

            if not headers:
                headers = {}
            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase

        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        return success, error


class OkexFutureTrade:
    """ Huobi Future Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. default `https://www.okex.com"`.
        wss: Websocket address. default `wss://real.okex.com:8443`.
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
        if not kwargs.get("contract_type"):
            e = Error("param contract_type miss")
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

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = OKEX_FUTURE
        self._symbol = kwargs["symbol"]
        self._contract_type = kwargs["contract_type"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        url = self._wss + "/ws/v3"

        self._ws = Websocket(
            url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )

        LoopRunTask.register(self._send_heartbeat_msg, 5)

        self._assets = Asset(platform=self._platform,
                             account=self._account)  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(self._platform, self._account, self._strategy, self._symbol)
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        self._rest_api = OKExFutureRestApi(
            secret_key=self._secret_key,
            access_key=self._access_key,
            passphrase=self._passphrase
        )

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
        data = "ping"
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    async def connected_callback(self):
        """After connect to Websocket server successfully, send a auth message to server."""
        timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
        message = str(timestamp) + "GET" + "/users/self/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf8"), digestmod="sha256")
        d = mac.digest()
        signature = base64.b64encode(d).decode()
        data = {
            "op": "login",
            "args": [self._access_key, self._passphrase, timestamp, signature]
        }
        await self._ws.send(data)

    async def auth_callback(self, data):
        if not data.get("success"):
            e = Error("Websocket连接用户认证失败: {}".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, "auth error", e)
            return
        logger.info("Websocket 连接用户认证成功.", caller=self)
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False
        # subscribe order
        data = {
            "op": "subscribe",
            "args": [f"futures/order:{self._symbol}"]
        }
        await self._ws.send(data)

        # subscribe position
        data = {
            "op": "subscribe",
            "args": [f"futures/position:{self._symbol}"]
        }
        await self._ws.send(data)

        # subscribe asset
        symbol = ""
        symbol_ = self._contract_type.split("-")
        if symbol_[1] == "USD":
            symbol = f"{symbol_[0]}"
        elif symbol_[1] == "USDT":
            symbol = f"{symbol_[0]}-USDT"
        data = {
            "op": "subscribe",
            "args": [f"futures/account:{symbol}"]
        }
        await self._ws.send(data)

    async def sub_callback(self, data):
        if not data.get("channel"):
            e = Error("subscribe {} failed!".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return

        channel = data.get("channel").split(":")[0].split("/")[1]
        if channel == "order":
            self._subscribe_order_ok = True
        elif channel == "position":
            self._subscribe_position_ok = True
        elif channel == "account":
            self._subscribe_asset_ok = True
        if self._subscribe_order_ok and self._subscribe_position_ok and self._subscribe_asset_ok:

            # 初始化订单列表详情
            success, error = await self._rest_api.get_order_list(self._symbol, state="6")
            if error:
                e = Error("get open orders failed!")
                SingleTask.run(self._init_success_callback, False, e)
            elif success:
                self._update_order(success["order_info"])
                SingleTask.run(self._init_success_callback, True, None)
            else:
                logger.warn("get open orders:", success, caller=self)
                e = Error("Get Open Orders Unknown error")
                SingleTask.run(self._init_success_callback, False, e)

            # 初始化仓位列表详情
            success, error = await self._rest_api.get_position(self._symbol)
            if error:
                e = Error("get open position failed!")
                SingleTask.run(self._init_success_callback, False, e)
            elif success:
                self._update_position(success)
                SingleTask.run(self._init_success_callback, True, None)
            else:
                logger.warn("get position:", success, caller=self)
                e = Error("Get Position Unknown error")
                SingleTask.run(self._init_success_callback, False, e)

    @async_method_locker("OkexFutureTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
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
            await self.auth_callback(msg)
            return

        # 订阅消息反馈
        if msg.get("event") == "error":
            logger.info(msg["message"], caller=self)
            return

        if msg.get("event") == "subscribe":
            await self.sub_callback(msg)

        if "table" in msg.keys():
            channel = msg["table"].split("/")[1]
            # 调用用户订单数据解析函数
            if channel == "order":
                self._update_order(msg["data"])
            # 调用用户交易仓位解析函数
            elif channel == "position":
                self._update_position(msg["data"][0])
            elif channel == "account":
                self._update_asset(msg["data"])

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, client_oid=None,
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
        if int(quantity) > 0:
            if action == ORDER_ACTION_BUY:
                _type = "1"
            elif action == ORDER_ACTION_SELL:
                _type = "2"
            else:
                return None, "action error"
        else:
            if action == ORDER_ACTION_BUY:
                _type = "3"
            elif action == ORDER_ACTION_SELL:
                _type = "4"
            else:
                return None, "action error"

        if order_type == ORDER_TYPE_LIMIT:
            order_type = "0"
            price = price
            match_price = "0"
        elif order_type == ORDER_TYPE_MARKET:
            order_type = "0"
            price = "0"
            match_price = "1"
        else:
            logger.error("order_type error! order_type:", order_type, caller=self)
            return None

        result, error = await self.rest_api.create_order(symbol=self._symbol,
                                                         type=_type,
                                                         price=price,
                                                         size=abs(int(quantity)),
                                                         order_type=order_type,
                                                         match_price=match_price,
                                                         client_oid=client_oid)
        if result and result.get("result"):
            order_id = result["order_id"]
            return order_id, None
        else:
            return None, error

    async def create_orders(self, orders, *args, **kwargs):
        """ batch create orders

        Args:
            :param orders: []
            list item:
                action: Trade direction, BUY or SELL.
                price: Price of each contract.
                quantity: The buying or selling quantity.
                order_type: Order type, LIMIT or MARKET.
                lever_rate: leverage.

        Returns:
            success: order info  if created successfully.
            error: erros information.
        """
        orders_data = []
        for order in orders:
            if int(order["quantity"]) > 0:
                if order["action"] == ORDER_ACTION_BUY:
                    _type = "1"
                elif order["action"] == ORDER_ACTION_SELL:
                    _type = "2"
                else:
                    return None, "action error"
            else:
                if order["action"] == ORDER_ACTION_BUY:
                    _type = "3"
                elif order["action"] == ORDER_ACTION_SELL:
                    _type = "4"
                else:
                    return None, "action error"
            if order["order_type"] == ORDER_TYPE_LIMIT:
                order_type = "0"
                price = order["price"]
                match_price = "0"
            elif order["order_type"] == ORDER_TYPE_MARKET:
                order_type = "0"
                price = "0"
                match_price = "1"
            else:
                logger.error("order_type error! order_type:", order["order_type"], caller=self)
                return None, "order type error"

            quantity = abs(int(order["quantity"]))

            client_order_id = order.get("client_order_id", "")
            d = {
                "order_type": order_type,
                "client_oid": client_order_id,
                "type": _type,
                "price": price,
                "size": quantity,
                "match_price": match_price
            }
            orders_data.append(d)

        result, error = await self._rest_api.create_orders(self._symbol, orders_data)
        if error:
            return None, error
        order_nos = [order["order_id"] for order in result.get("order_info")]
        return order_nos, None

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
        if len(order_nos) == 0:
            success, error = await self.rest_api.revoke_orders_all(self._symbol)
            if error:
                return False, error
            if success["result"]:
                order_nos = success.get("ids") if success.get("ids") else success.get("client_oids")
                return order_nos, None

        if len(order_nos) == 1:
            success, error = await self.rest_api.revoke_order(symbol=self._symbol, order_no=order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        if 10 > len(order_nos) > 1:
            success, error = await self.rest_api.revoke_orders(self._symbol, order_nos)
            if error:
                return [], error
            if success["result"]:
                order_nos = success.get("ids") if success.get("ids") else success.get("client_oids")

                return order_nos, None

    async def get_open_order_nos(self):
        """ Get open order id list.

        Args:
            None.

        Returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_order_list(self._symbol, 6)
        if error:
            return None, error
        else:
            if len(success) > 100:
                logger.warn("order length too long! (more than 100)", caller=self)
            order_nos = []
            for order_info in success["order_info"]:
                order_nos.append(order_info["order_id"])
            return order_nos, None

    def _update_order(self, order_info):
        for order in order_info:
            if order["instrument_id"] != self._symbol:
                return
            order_id = str(order["order_id"])
            status = order_info["state"]
            trade_type = None
            action = None
            if order["type"] == "1":
                trade_type = TRADE_TYPE_BUY_OPEN
                action = ORDER_ACTION_BUY
            elif order["type"] == "2":
                trade_type = TRADE_TYPE_SELL_OPEN
                action = ORDER_ACTION_SELL
            elif order["type"] == "3":
                trade_type = TRADE_TYPE_BUY_CLOSE
                action = ORDER_ACTION_BUY
            elif order["type"] == "4":
                trade_type = TRADE_TYPE_SELL_CLOSE
                action = ORDER_ACTION_SELL

            order_type = None
            if order["order_type"] == "0":
                order_type = ORDER_TYPE_LIMIT
            elif order["order_type"] == "1":
                order_type = ORDER_TYPE_MAKER
            elif order["order_type"] == "2":
                order_type = ORDER_TYPE_FOK
            elif order["order_type"] == "3":
                order_type = ORDER_TYPE_IOC
            elif order["order_type"] == "4":
                order_type = ORDER_TYPE_MARKET
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_id": order_id,
                "client_order_id": order.get("client_oid"),
                "order_type": order_type,
                "action": action,
                "symbol": self._symbol,
                "price": order["price"],
                "quantity": order["size"],
                "trade_type": trade_type,
                "fee": order["fee"],
                "avg_price": order["price_avg"]
            }
            order_ = Order(**info)
            self._orders[order_id] = order_

            if status == "-1":
                order.status = ORDER_STATUS_CANCELED
                order.remain = 0
            elif status == "0":
                order_.status = ORDER_STATUS_SUBMITTED
                order_.ctime = order["timestamp"]
                order_.utime = order["last_fill_time"] if order.get("last_fill_time") else order["timestamp"]
            if status == "1":
                order.status = ORDER_STATUS_PARTIAL_FILLED
                order.remain = int(order.quantity) - int(order["last_fill_qty"])
                order_.utime = order["last_fill_time"]
            elif status == "2":
                order.status = ORDER_STATUS_FILLED
                order.remain = 0
                order_.utime = order["last_fill_time"]
            else:
                return

            SingleTask.run(self._order_update_callback, copy.copy(order))

            # Delete order that already completed.
            if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
                self._orders.pop(order_id)

    def _update_position(self, position_info):
        position_info = position_info["holding"]
        for position_ in position_info:
            if position_.get("instrument_id") != self._symbol:
                return
            self._position.leverage = position_info["leverage"]  # 杠杆倍数
            self._position.short_quantity = position_["short_qty"]  # 空仓数量
            self._position.short_avail_qty = position_["short_avail_qty"]  # 空仓可平仓数量
            self._position.short_avg_price = position_["short_avg_cost"]  # 空仓持仓平均价格
            self._position.short_pnl_ratio = position_["short_pnl_ratio"]  # 空仓收益率
            self._position.short_pnl = position_["short_pnl"]  # 空仓盈亏
            self._position.long_quantity = position_["long_qty"]  # 多仓数量
            self._position.long_avail_qty = position_["long_avail_qty"]  # 多仓可平仓数量
            self._position.long_avg_price = position_["long_avg_cost"]  # 多仓持仓平均价格
            self._position.long_pnl_ratio = position_["long_pnl_ratio"]  # 多仓收益率
            self._position.long_pnl = position_["long_pnl"]  # 多仓盈亏
            self._position.long_pos_margin = position_["long_margin"]  # 多仓持仓保证金
            self._position.short_pos_margin = position_["short_margin"]  # 空仓持仓保证金
            self._position.liquid_price = position_["liquidation_price"]  # 预估爆仓价格
            self._position.maint_margin_ratio = None  # 保证金率
            self._position.created_time = position_["created_at"]
            self._position.utime = position_["updated_at"]

            SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def _update_asset(self, data):
        """ Asset update.

        Args:
            data: asset data.

        Returns:
            None.
        """
        data = data[0]

        for item in data:
            assets = {}
            symbol = item
            total = float(item["equity"])
            free = float(item["available"])
            locked = float(item["margin"])
            assets[symbol] = {
                "total": "%.8f" % total,
                "free": "%.8f" % free,
                "locked": "%.8f" % locked
            }

            info = {
                "platform": self._platform,
                "account": self._account,
                "assets": assets,
                "timestamp": tools.get_cur_timestamp_ms(),
                "update": True
            }
            asset = Asset(**info)
            self._assets = asset
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))

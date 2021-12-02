# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/9/27 0:09
  @ Description: 
  @ History:
"""
import copy
import zlib
from xuanwu.utils import logger
from xuanwu.utils.websocket import Websocket
from xuanwu.const import KLINE_TYPE
from xuanwu.tasks import SingleTask
from xuanwu.model.market import Orderbook, Kline, Trade

class BybitMarket(Websocket):
    """ Okex V5 Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `huobi_usdt_swap`.
            wss: Exchange Websocket host address.
            symbols: Trade pair list, e.g. ["BTC_USDT"].
            channels: channel list, only `orderbook`, `kline` and `trade` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://stream.bybit.com/realtime")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = list(set(kwargs.get("channels")))
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")

        self._c_to_s = {}  # {"channel": "symbol"}
        self._orderbook = {}

        self.heartbeat_msg = "ping"

        url = self._wss
        super(BybitMarket, self).__init__(url, send_hb_interval=15)
        self.initialize()

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
                        "op": "subscribe",
                        "args": [channel]
                    }
                    await self.ws.send_json(kline)
            elif ch == "orderbook":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "orderbook")
                    if not channel:
                        continue
                    data = {
                        "op": "subscribe",
                        "args": [channel]
                    }
                    await self.ws.send_json(data)
            elif ch == "trade":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    if not channel:
                        continue
                    data = {
                        "op": "subscribe",
                        "args": [channel]
                    }
                    await self.ws.send_json(data)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process(self, msg):
        """ 推送数据回调
            Attributes:
                :param msg: 服务器推送数据回调信息

            :returns:
                None
        """
        if isinstance(msg, str) and msg == "pong":
            return

        if msg.get("event") and msg.get("event") == "error":
            logger.error(f"行情数据请阅出错, error msg: {msg}", caller=self)
            return
        
        topic = msg.get("topic", "")
        if topic.count("trade"):
            await self.process_trade(msg)
        elif topic.count("orderBookL2_25"):
            await self.process_orderbook(msg)
        elif topic.count("klineV2"):
            await self.process_kline(msg)

    async def process_binary(self, msg):
        """只继承不实现"""
        pass

    def _symbol_to_channel(self, symbol, channel_type):
        """ 订阅信息格式化

        Attributes:
            symbol: Trade pair name.such as BTC-USD
            channel_type: channel name, kline / ticker / orderbook.
        """
        if channel_type == "kline":
            channel = f"klineV2.1.{symbol}"
        elif channel_type == "orderbook":
            channel = f"orderBookL2_25.{symbol}"
        elif channel_type == "trade":
            channel = "trade"
        else:
            logger.error("channel type error! channel type:", channel_type, caller=self)
            return None
        self._c_to_s[f"{channel_type}-{symbol}"] = symbol
        return channel

    async def process_kline(self, data):
        """ K线数据处理
        """
        topic = data.get("topic")
        data = data.get("data")
        symbol = topic.split('.')[-1]
        if not symbol:
            return
        if data:
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "open": data[0]['open'],
                "high": data[0]['high'],
                "low": data[0]['low'],
                "close": data[0]['close'],
                "volume": data[0]['volume'],
                "coin_volume": data[0]['turnover'],
                "timestamp": int(data[0]['start']),
                "kline_type": KLINE_TYPE[0]
            }
            kline = Kline(**info)
            SingleTask.run(self._kline_update_callback, copy.copy(kline))

    async def process_orderbook(self, data):
        """ orderbook数据处理
        """
        topic = data.get("topic")
        action = data.get("type")
        timestamp = data.get("timestamp_e6")
        data = data.get("data")

        symbol = topic.split('.')[-1]

        if not symbol:
            return

        if action == "snapshot":
            ob = Orderbook(platform=self._platform)
            ob.symbol = data[0]['symbol']

            bids = []
            asks = []

            for each_dict in data:
                if each_dict['side'] == 'Buy':
                    bids.append([each_dict['price'], each_dict['size']])
                elif each_dict['side'] == 'Sell':
                    asks.append([each_dict['price'], each_dict['size']])
            
            bids.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
            asks.sort(key=lambda price: self.sort_num(price[0]))

            ob.bids = bids
            ob.asks = asks
            ob.timestamp = timestamp / 1_000_000

            check_num = self.check(bids, asks)
            if check_num:
                logger.info("订单簿首次推送校验结果为：True", caller=self)
            else:
                logger.info("校验错误，重新连接WS......", caller=self)
                SingleTask.run(self._reconnect)

            self._orderbook[symbol] = ob

        if action == "delta": # TODO

            asks = []
            bids = []

            for delete in data.get('delete', []):
                single_data = [delete['price'], 0]
                if delete['side'] == 'Buy':
                    bids.append(single_data)
                elif delete['side'] == 'Sell':
                    asks.append(single_data)

            for update in data.get('update', []):
                single_data = [update['price'], update['size']]
                if update['side'] == 'Buy':
                    bids.append(single_data)
                elif update['side'] == 'Sell':
                    asks.append(single_data)

            for insert in data.get('insert', []):
                single_data = [insert['price'], insert['size']]
                if insert['side'] == 'Buy':
                    bids.append(single_data)
                elif insert['side'] == 'Sell':
                    asks.append(single_data)

            bids_p = self._orderbook[symbol].bids
            asks_p = self._orderbook[symbol].asks

            logger.info(bids)

            bids_p = self.update_bids(bids, bids_p)
            asks_p = self.update_asks(asks, asks_p)

            check = self.check(bids_p, asks_p)
            if check:
                pass
            else:
                logger.info(f"{arg['instId']}, Update 校验结果为：False，正在重新订阅……", caller=self)
                SingleTask.run(self._reconnect)

            d = copy.copy(self._orderbook[symbol])
            d.asks = d.asks[:self._orderbook_length]
            d.bids = d.bids[:self._orderbook_length]

            SingleTask.run(self._orderbook_update_callback, d)

    async def process_trade(self, data):
        """ trade数据处理
        """
        topic = data.get("topic")
        data = data.get("data")
        symbol = topic.split('.')[-1]
        if not symbol:
            return
        if not data:
            return
        for tick in data:
            direction = tick["side"]
            price = tick["price"]
            size = tick["size"]
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "side": direction.upper(),
                "price": price,
                "quantity": size,
                "timestamp": tick.get("trade_time")
            }
            trade = Trade(**info)
            SingleTask.run(self._trade_update_callback, copy.copy(trade))

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
            bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
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
            asks_p.sort(key=lambda price: self.sort_num(price[0]))
        return asks_p

    def check(self, bids, asks):
        return True

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

    def change(self, num_old):
        """ 生成checksum验证数据
        Attributes:
            :param num_old: 校验数据
        :returns:
            :return: 返回校验数据
        """
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out

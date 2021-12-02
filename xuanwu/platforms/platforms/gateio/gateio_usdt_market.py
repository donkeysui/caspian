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
import time
from xuanwu.utils import logger
from xuanwu.utils.websocket import Websocket
from xuanwu.const import KLINE_TYPE
from xuanwu.tasks import SingleTask
from xuanwu.model.market import Orderbook, Kline, Trade


class GateIOMarket(Websocket):
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
        self._wss = kwargs.get("wss", "wss://fx-ws.gateio.ws/")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = list(set(kwargs.get("channels")))
        self._orderbook_length = kwargs.get("orderbook_length", 5)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._bestaskbid_update_callback = kwargs.get("bestaskbid_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")

        self._c_to_s = {}  # {"channel": "symbol"}
        self._orderbook = {}

        self.heartbeat_msg = "ping"

        url = self._wss + "v4/ws/usdt"
        super(GateIOMarket, self).__init__(url, send_hb_interval=15)
        self.initialize()

    async def connected_callback(self):
        """ After create Websocket connection successfully, we will subscribing orderbook/trade events.
        """
        now_time = int(time.time())
        for ch in self._channels:
            if ch == "kline":
                for symbol in self._symbols:
                    kline = {"time" : now_time,
                            "channel" : "futures.candlesticks",
                            "event": "subscribe",
                            "payload" : [symbol]
                            }
                    await self.ws.send_json(kline)

            elif ch == "orderbook":
                for symbol in self._symbols:
                    data = {"time" : now_time,
                            "channel" : "futures.order_book",
                            "event": "subscribe",
                            "payload" : [symbol, "5", "0"]
                            }
                    await self.ws.send_json(data)

            elif ch == "bestaskbid":
                for symbol in self._symbols:
                    data = {"time" : now_time,
                            "channel" : "futures.book_ticker",
                            "event": "subscribe",
                            "payload" : [symbol]
                            }
                    await self.ws.send_json(data)

            elif ch == "trade":
                for symbol in self._symbols:
                    data = {"time" : now_time,
                            "channel" : "futures.trades",
                            "event": "subscribe",
                            "payload" : [symbol]
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
        arg = msg
        if arg["channel"] == "futures.trades":
            await self.process_trade(msg)
        elif arg["channel"] == "futures.order_book":
            await self.process_orderbook(msg)
        elif arg["channel"] == "futures.book_ticker":
            await self.process_bestaskbid(msg)
        elif arg["channel"] == "futures.candlesticks":
            await self.process_kline(msg)

    async def process_binary(self, msg):
        """只继承不实现"""
        pass

    def _symbol_to_channel(self, symbol, channel_type):
        self._c_to_s[f"{channel_type}-{symbol}"] = symbol

    async def process_kline(self, data):
        """ K线数据处理
        """
        arg = data.get("arg")
        data = data.get("result")
        symbol = data[0]['n']
        if data:
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "open": data[0]["o"],
                "high": data[0]["h"],
                "low": data[0]["l"],
                "close": data[0]["c"],
                "volume": data[0]["v"],
                "coin_volume": 0,
                "timestamp": int(data[0]["t"]),
                "kline_type": KLINE_TYPE[0]
            }
            kline = Kline(**info)
            SingleTask.run(self._kline_update_callback, copy.copy(kline))

    async def process_bestaskbid(self, data):
        """ bestaskbid数据处理
        """
        event = data.get("event")
        result = data.get("result")
    
        if not event == "update":
            return

        info = {
            "timestamp": result['t'],
            "symbol": result['s'],
            "bid_price": result['b'],
            "bid_volume": result['B'],
            "ask_price": result['a'],
            "ask_volume": result['A']
        }
        SingleTask.run(self._bestaskbid_update_callback, copy.copy(info))

    async def process_orderbook(self, data):
        """ orderbook数据处理
        """
        event = data.get("event")
        result = data.get("result")
        
        if event == "all":

            symbol = self._c_to_s.get(f"orderbook-{result['contract']}")

            ob = Orderbook(platform=self._platform)
            ob.symbol = result['contract']
            ob.bids = [[float(x['p']), float(x['s'])] for x in result["bids"]]
            ob.asks = [[float(x['p']), float(x['s'])] for x in result["asks"]]
            ob.timestamp = data["time"]

            # check_result = self.check(ob.bids, ob.asks)
            # if check_result:
            #     # logger.info("订单簿首次推送校验结果为：True", caller=self)
            #     pass
            # else:
            #     # 发送订阅
            #     logger.info("校验错误，重新连接WS......", caller=self)
            #     SingleTask.run(self._reconnect)

            self._orderbook[f"{result['contract']}"] = ob
            SingleTask.run(self._orderbook_update_callback, ob)

        if event == "update":

            symbol = self._c_to_s.get(f"{result['c']}")

            asks = [[float(x['p']), float(x['s'])] for x in result["asks"]]
            bids = [[float(x['p']), float(x['s'])] for x in result["bids"]]
            bids_p = self._orderbook[f"{result['contract']}"].bids
            asks_p = self._orderbook[f"{result['contract']}"].asks

            bids_p = self.update_bids(bids, bids_p)
            asks_p = self.update_asks(asks, asks_p)

            check_result = self.check(bids_p, asks_p)
            if check_result:
                pass
            else:
                # 发送订阅
                logger.info("校验错误，重新连接WS......", caller=self)
                SingleTask.run(self._reconnect)

            d = copy.copy(self._orderbook[f"{result['contract']}"])
            d.asks = d.asks[:self._orderbook_length]
            d.bids = d.bids[:self._orderbook_length]

            SingleTask.run(self._orderbook_update_callback, d)

    async def process_trade(self, data):
        """ trade数据处理
        """
        result = data.get('result')
        if not data:
            return
        if not result:
            return

        if isinstance(result, list):
            for tick in result:
                direction = 'buy' if tick["size"] > 0 else 'sell'
                price = tick["price"]
                quantity = abs(tick["size"])
                info = {
                    "platform": self._platform,
                    "symbol": tick['contract'],
                    "side": direction.upper(),
                    "price": price,
                    "quantity": quantity,
                    "timestamp": tick.get("create_time_ms")
                }
                trade = Trade(**info)
                SingleTask.run(self._trade_update_callback, copy.copy(trade))            
        elif isinstance(result, dict):
            return 

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
        """ 首次接受订单簿对订单簿数据进行校验
        Attributes:
            :param bids: 全量bids数据
            :param asks: 全量asks数据
        :returns:
            :return: 返回校验结果
        """
        # 获取bid档str
        logger.info(bids)
        logger.error(asks)

        highest_bid = max([x[0] for x in bids])
        lowest_ask = min([x[0] for x in asks])
        if highest_bid < lowest_ask:
            res = True
        else:
            res = False
        return res

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

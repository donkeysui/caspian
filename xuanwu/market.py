# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/9/29 1:09
  @ Description: 
  @ History:
"""
import copy
from xuanwu import const
from xuanwu.utils import logger
from xuanwu.error import Error


class Market:
    """ 市场数据模型

    Attributes:
        platform: 交易所常量
        symbols: 交易币对列表，支持多订阅. e.g. [`BTC-USD`]
        channels: 订阅频道列表.e.g.['kline', 'orderbook', 'trade']
        orderbook_length: 订单簿长度.default 10.
        wss: Websocket address.
        orderbook_update_callback: 订单簿回调函数，异步执行
        kline_update_callback: K线回调函数，异步执行
        trade_update_callback: 市场最新交易回调函数，异步执行
    """

    def __init__(self, platform=None, symbols=None, channels=None, orderbook_length=None, wss=None,
                 orderbook_update_callback=None, kline_update_callback=None, trade_update_callback=None, **kwargs):
        """initialize trade object."""
        kwargs["platform"] = platform
        kwargs["symbols"] = symbols
        kwargs["channels"] = channels
        kwargs["orderbook_length"] = orderbook_length
        kwargs["wss"] = wss
        kwargs["orderbook_update_callback"] = orderbook_update_callback
        kwargs["kline_update_callback"] = kline_update_callback
        kwargs["trade_update_callback"] = trade_update_callback

        e = None
        if "kline" in channels and kline_update_callback is None:
            e = Error("kline_update_callback 回调函数为None")
        if "trade" in channels and trade_update_callback is None:
            e = Error("trade_update_callback 回调函数为None")
        if "orderbook" in channels and trade_update_callback is None:
            e = Error("orderbook_update_callback 回调函数为None")
        if e:
            logger.error(e, caller=self)

        self._raw_params = copy.copy(kwargs)

        if platform == const.OKEX_V5:
            from xuanwu.platforms.okex_v5.okex_v5_market import OkexV5Market as M
        # elif platform == const.HUOBI_FUTURE:
        #     from alpha.platforms.huobi_future_market import HuobiFutureMarket  as M
        # elif platform == const.HUOBI_OPTION:
        #     from alpha.platforms.huobi_option_market import HuobiOptionMarket  as M
        # elif platform == const.HUOBI_USDT_SWAP:
        #     from alpha.platforms.huobi_usdt_swap_market import HuobiUsdtSwapMarket  as M
        # elif platform == const.HUOBI_USDT_SWAP_CROSS:
        #     from alpha.platforms.huobi_usdt_swap_market import HuobiUsdtSwapMarket  as M
        else:
            logger.error("platform error:", platform, caller=self)
            return
        self._m = M(**kwargs)
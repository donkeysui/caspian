# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/25 9:48
  @ Description: 
  @ History:
"""
import time
import operator
from xuanwu.tasks import SingleTask
from xuanwu.platforms.ftx.ftx import FTXMarket
from xuanwu.configure import config
from xuanwu.model.market import Orderbook
from xuanwu.utils import logger
from xuanwu.utils.decorator import async_method_locker


class SpreadCount:
    def __init__(self, futu_symbol, perp_symbol, decimal, orderbook_callback=None, degree_callback=None):
        """
        运行：python main.py config.uni
        uni-usd：币对名称
        3：交易精度，小数点位数
        :param symbol:
        """
        self.futu_symbol = futu_symbol
        self.perp_symbol = perp_symbol
        self._decimal = decimal
        FTXMarket(
            platform=config.accounts["platform"],
            symbols=[self.futu_symbol, self.perp_symbol],
            channels=["orderbook"],
            orderbook_length=1,
            orderbook_update_callback=self._orderbook_update_callback,
            init_callback=self._init_callback,
            error_callback=self._error_callback
        )

        self._futu_orderbook = {}  # 交割合约盘口
        self._swap_orderbook = {}  # 永续合约盘口
        self._orderbook_callback = orderbook_callback
        self._degree_callback = degree_callback
        self._last_degree = None

    @async_method_locker("FtxTickerCount.locker")
    async def _orderbook_update_callback(self, data: Orderbook):
        symbol = data.symbol

        if symbol == self.futu_symbol:
            self._futu_orderbook = data
        elif symbol == self.perp_symbol:
            self._swap_orderbook = data

        if self._swap_orderbook and self._futu_orderbook:
            futu_ask = self._futu_orderbook.asks[0][0]
            futu_bid = self._futu_orderbook.bids[0][0]
            futu_mid = round((futu_ask + futu_bid) / 2, self._decimal)
            swap_ask = self._swap_orderbook.asks[0][0]
            swap_bid = self._swap_orderbook.bids[0][0]
            swap_mid = round((swap_ask + swap_bid) / 2, self._decimal)
            degree = round((swap_mid - futu_mid) / swap_mid * 100, self._decimal)

            orderbook = {
                "futu_asks": f"{futu_ask}",
                "futu_bids": f"{futu_bid}",
                "futu_mid": f"{futu_mid}",
                "swap_asks": f"{swap_ask}",
                "swap_bids": f"{swap_bid}",
                "swap_mid": f"{swap_mid}",
                "timestamp": int(time.time() * 1000)
            }
            if self._orderbook_callback:
                SingleTask.run(self._orderbook_callback, orderbook)

            if self._last_degree != degree:
                self._last_degree = degree

                d = {
                    "degree": f"{degree}",
                    "timestamp": int(time.time() * 1000)
                }
                if self._degree_callback:
                    SingleTask.run(self._degree_callback, d)

    async def _init_callback(self, tip, msg):
        logger.info(f"{tip}------{msg}", caller=self)

    async def _error_callback(self, tip, msg):
        logger.info(f"{tip}------{msg}", caller=self)

# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/5/11 11:36
  @ Description: 
  @ History:
"""
import asyncio

from loguru import logger
from .spread_count import SpreadCount
from xuanwu.platforms.ftx.ftx import FTXTrade
from xuanwu.configure import config
from xuanwu.tasks import SingleTask, LoopRunTask
from xuanwu.model.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL

_db_config = {
    "ftx_ticker_count": {
        "host": "",
        "port": 3306,
        "username": "",
        "password": "",
        "schema": ""
    }
}


class CurrentArbitrage:
    def __init__(self, symbol):
        """ 运行：python main.py config.uni
        :param symbol:
        """
        sy = symbol.upper()
        cfg = config.symbols
        init_ok = False
        for i in cfg:
            if i["symbol"] == sy:
                init_ok = True
                self.futu_symbol = i["futu_symbol"]
                self.perp_symbol = i["perp_symbol"]
                self.decimal = i["decimal"]
                self.init_asset = i["init_asset"]
                self.level = i["level"]

        if init_ok is False:
            logger.error("配置文件初始化错误，请重新核实配置文件参数信息")
            return

        self.sc = SpreadCount(
            futu_symbol=self.futu_symbol,
            perp_symbol=self.perp_symbol,
            decimal=self.decimal,
            orderbook_callback=self._orderbook_callback,
            degree_callback=self._degree_callback
        )
        logger.error(config.accounts)

        self._ftx_ = FTXTrade(
            account=config.accounts["subaccount_name"],
            platform=config.accounts["platform"],
            strategy=config.accounts["strategy"],
            symbol=[self.sc.futu_symbol, self.sc.perp_symbol],
            access_key=config.accounts["access_key"],
            secret_key=config.accounts["secret_key"],
            subaccount_name=config.accounts["subaccount_name"],
            asset_update_callback=self._asset_update_callback,
            order_update_callback=self._order_update_callback,
            fill_update_callback=self._fill_update_callback,
            position_update_callback=self._position_update_callback,
            init_success_callback=self._init_success_callback
        )

        self._orderbook = None  # 订单簿
        self._degree = None  # 延迟百分比
        # =======================================================
        self._position = None
        self._is_ok = False
        self._is_count = 0

        self.max_asset = self.init_asset * self.level

        self._open = []

        # SingleTask.run(self.get_position)

    async def _orderbook_callback(self, data):
        """ 订单簿回调函数
        {
            'spot_asks': '36.532',
            'spot_bids': '36.475',
            'spot_mid': '36.504',
            'swap_asks': '36.519',
            'swap_bids': '36.486',
            'swap_mid': '36.502',
            'timestamp': 1620713095559
        }
        :param data:
        :return:
        """
        self._orderbook = data
        # logger.info(data)

    async def _degree_callback(self, data):
        """ 价差百分比回调函数
        {
            'degree': '0.005',
            'timestamp': 1620713095542
        }
        :param data:
        :return:
        """
        self._degree = data
        # 做全局行情数据初始化控制
        if self._degree:
            if self._is_count == 20:
                self._is_ok = True
            else:
                self._is_count += 1

        logger.error(data)

    async def _asset_update_callback(self, data):
        logger.error(data)

    async def _order_update_callback(self, data):
        logger.error(data)
        logger.warning(self._ftx_.orders)

    async def _fill_update_callback(self, data):
        logger.error(data)

    async def _position_update_callback(self, data):
        logger.error(data)

    async def _init_success_callback(self, tip, msg):
        logger.warning(f"{tip}---{msg}")

    # ------------------------------------------------------------------------------------------------------------------

    async def init_account_asset(self):
        """ 获取仓位和资产信息
        :return:
        """
        asset = self.init_asset * self.level

    async def init_grid(self):
        if self._orderbook and self._degree:
            pass









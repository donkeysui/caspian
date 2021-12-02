# -*- coding: utf-8 -*-
"""
  @ Author:   Donkey Khan
  @ Email:    vancleef_turkey@foxmail.com
  @ Date:     2021/10/11 22:50
  @ Description: 
  @ History:
"""

from xuanwu.platforms.okex_v5.okex_v5_market import OkexV5Market
from loguru import logger
from xuanwu.model.market import Orderbook
import time
import copy
from pprint import pprint
import datetime
from influxdb import InfluxDBClient
from .FileWriter import FileWriter
from collections import OrderedDict


class Listener:
    def __init__(self, configs):

        symbol = configs.get('symbol', None)
        influx_database = configs.get('influx_database', None)
        silent = configs.get('silent', None)
        channels = configs.get('channels', None)
        file = configs.get('file', None)
        file_url = configs.get('file_url', None)
        platform = configs.get('platform', None)

        if symbol is None:
            logger.error("symbol is None, check the config file!")
            return

        if channels is None:
            logger.error("channels is None, check the config file!")
            return

        if platform is None:
            logger.error("platform is None, check the config file!")
            return

        self._swap_symbol = [x for x in symbol]

        self.market = OkexV5Market(
            platform=platform,
            symbols=self._swap_symbol,
            channels=channels,
            orderbook_length=5,
            orderbook_update_callback=self._orderbook_callback,
            trade_update_callback=self._trades_callback,
            init_callback=self._init_callback,
            error_callback=self._error_callback
        )
        self._last_ticker = dict()
        self._last_trade = dict()
        self.isInitialized = None
        self.silent = silent

        if influx_database:
            self.influx = InfluxDBClient(database=influx_database)
        else:
            self.influx = None

        if file and file_url:

            self.file_writer_dict = dict()
            for single_symbol in self._swap_symbol:

                self.file_writer_dict[single_symbol] = dict()
                for single_channel in channels:
                    fw_configs = dict()
                    fw_configs['symbol'] = single_symbol
                    fw_configs['exchange'] = platform
                    fw_configs['data_type'] = single_channel
                    fw_configs['file_url'] = file_url
                    self.file_writer_dict[single_symbol][single_channel] = FileWriter(fw_configs)
        else:
            if file or file_url:
                logger.error("Have one of file or file_url, but both of them needed!")
            self.file_writer_dict = None

        self.now_time = time.time()
 
    async def _orderbook_callback(self, data: Orderbook):
        platform = data.platform
        symbol = data.symbol

        now = time.time()
        if now - self.now_time > 60:
            self.now_time = now

        if data:
            ask_1_price = float(data.asks[0][0])
            ask_2_price = float(data.asks[1][0])
            ask_3_price = float(data.asks[2][0])
            ask_4_price = float(data.asks[3][0])
            ask_5_price = float(data.asks[4][0])
            
            bid_1_price = float(data.bids[0][0])
            bid_2_price = float(data.bids[1][0])
            bid_3_price = float(data.bids[2][0])
            bid_4_price = float(data.bids[3][0])
            bid_5_price = float(data.bids[4][0])

            ask_1_vol = float(data.asks[0][1])
            ask_2_vol = float(data.asks[1][1])
            ask_3_vol = float(data.asks[2][1])
            ask_4_vol = float(data.asks[3][1])
            ask_5_vol = float(data.asks[4][1])

            bid_1_vol = float(data.bids[0][1])
            bid_2_vol = float(data.bids[1][1])
            bid_3_vol = float(data.bids[2][1])
            bid_4_vol = float(data.bids[3][1])
            bid_5_vol = float(data.bids[4][1])

            timestamp = data.timestamp

            d = {
                "symbol": symbol,
                "timestamp": timestamp,
                "ap1": ask_1_price,
                "ap2": ask_2_price,
                "ap3": ask_3_price,
                "ap4": ask_4_price,
                "ap5": ask_5_price,
                "bp1": bid_1_price,
                "bp2": bid_2_price,
                "bp3": bid_3_price,
                "bp4": bid_4_price,
                "bp5": bid_5_price,
                "az1": ask_1_vol,
                "az2": ask_2_vol,
                "az3": ask_3_vol,
                "az4": ask_4_vol,
                "az5": ask_5_vol,
                "bz1": bid_1_vol,
                "bz2": bid_2_vol,
                "bz3": bid_3_vol,
                "bz4": bid_4_vol,
                "bz5": bid_5_vol,
            }

            if self._last_ticker.get(symbol, None) != d:
                self._last_ticker[symbol] = copy.copy(d)

                if self.influx:
                    self.influx.write_points([
                        {
                            "measurement": "orderbook",
                            "fields": d
                        }])
                
                if self.file_writer_dict:
                    self.file_writer_dict[symbol]['orderbook'].write(OrderedDict(d))

                if not self.silent:
                    logger.info(d)

    async def _trades_callback(self, trade):
        platform = trade.platform

        if trade:
            price = trade.price
            symbol = trade.symbol
            side = trade.side
            quantity = trade.quantity
            timestamp = trade.timestamp

            d = {
                'price': price,
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'timestamp': timestamp
            }

            if self._last_trade.get(symbol, None) != d:
                self._last_trade[symbol] = copy.copy(d)

                if self.influx:
                    self.influx.write_points([
                        {
                            "measurement": "trade",
                            "fields": d
                        }
                    ])

                if self.file_writer_dict:
                    self.file_writer_dict[symbol]['trade'].write(OrderedDict(d))

                if not self.silent:
                    logger.info(d)

    async def _init_callback(self, tip, msg):
        logger.info(f"{tip}-------{msg}", caller=self)

    async def _error_callback(self, tip, msg):
        logger.info(f"{tip}------{msg}", caller=self)


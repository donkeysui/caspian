# -*- coding: utf-8 -*-
'''
  @ Copyright (C), www.honglianziben.com
  @ FileName: market_pojo
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/5/21 15:16
  @ Description:
  @ History:
  @ <author>          <time>          <version>          <desc>
  @ 作者姓名           修改时间           版本号            描述
'''

import json


class Orderbook:
    """Orderbook 对象.

    Args:
        platform: 交易所名称，参考cnost.py文件.
        symbol: 交易币对名称, e.g. `ETH/BTC`.
        asks: Asks list, e.g. `[[price, quantity], [...], ...]`
        bids: Bids list, e.g. `[[price, quantity], [...], ...]`
        timestamp: 订单簿更新时间.
    """

    def __init__(self, platform=None, symbol=None, asks=None, bids=None, timestamp=None):
        """Initialize."""
        self.platform = platform
        self.symbol = symbol
        self.asks = asks
        self.bids = bids
        self.timestamp = timestamp

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "asks": self.asks,
            "bids": self.bids,
            "timestamp": self.timestamp
        }
        return d

    @property
    def smart(self):
        d = {
            "p": self.platform,
            "s": self.symbol,
            "a": self.asks,
            "b": self.bids,
            "t": self.timestamp
        }
        return d

    def load_smart(self, d):
        self.platform = d["p"]
        self.symbol = d["s"]
        self.asks = d["a"]
        self.bids = d["b"]
        self.timestamp = d["t"]
        return self

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)


class Ticker:
    """ticket 对象.

    Args:
        platform: 交易所名称
        symbol: 交易币对名称，根据具体的合约、现货、永续品种对应
        asks: Asks 价格
        asks_volume: Asks 挂单量
        bids: Bids 价格
        bids_volume: Bids 挂单量
        best_price: 最有成交价格
        timestamp: 最新更新时间.
    """

    def __init__(self, platform=None, symbol=None, asks=None, asks_volume=None, bids=None, bids_volume=None,
                 best_price=None, timestamp=None):
        """Initialize."""
        self.platform = platform
        self.symbol = symbol
        self.asks = asks
        self.asks_volume = asks_volume
        self.bids = bids
        self.bids_volume = bids_volume
        self.best_price = best_price
        self.timestamp = timestamp

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "asks": self.asks,
            "asks_volume": self.asks_volume,
            "bids": self.bids,
            "bids_volume": self.bids_volume,
            "best_price": self.best_price,
            "timestamp": self.timestamp
        }
        return d

    @property
    def smart(self):
        d = {
            "p": self.platform,
            "s": self.symbol,
            "a": self.asks,
            "av": self.asks_volume,
            "b": self.bids,
            "bv": self.bids_volume,
            "bp": self.best_price,
            "t": self.timestamp
        }
        return d

    def load_smart(self, d):
        self.platform = d["p"]
        self.symbol = d["s"]
        self.asks = d["a"]
        self.asks_volume = d["av"]
        self.bids = d["b"]
        self.bids_volume = d["bv"]
        self.best_price = d["bp"]
        self.timestamp = d["t"]
        return self

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)


class Trade:
    """Trade object.

    Args:
        platform: 交易所名称
        symbol: 交易币对名称，根据具体的合约、现货、永续品种对应
        side: 交易方向.
        price: 成交价格.
        trade_id: 成交ID.
        quantity: 交易量.
        timestamp: 更新时间.
    """

    def __init__(self, platform=None, symbol=None, side=None, price=None, trade_id=None, quantity=None, timestamp=None):
        """Initialize."""
        self.platform = platform
        self.symbol = symbol
        self.side = side
        self.price = price
        self.trade_id = trade_id
        self.quantity = quantity
        self.timestamp = timestamp

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "side": self.side,
            "price": self.price,
            "trade_id": self.trade_id,
            "quantity": self.quantity,
            "timestamp": self.timestamp
        }
        return d

    @property
    def smart(self):
        d = {
            "p": self.platform,
            "sl": self.symbol,
            "sd": self.side,
            "P": self.price,
            "ti": self.trade_id,
            "q": self.quantity,
            "t": self.timestamp
        }
        return d

    def load_smart(self, d):
        self.platform = d["p"]
        self.symbol = d["s"]
        self.side = d["sd"]
        self.price = d["P"]
        self.trade_id = d["ti"]
        self.quantity = d["q"]
        self.timestamp = d["t"]
        return self

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)


class Kline:
    """Kline object.

    Args:
        platform: 交易所名称
        symbol: 交易币对名称，根据具体的合约、现货、永续品种对应
        open: 开盘价.
        high: 最高价.
        low: 最低价.
        close: 收盘价.
        volume: 成交量.
        timestamp: 更新时间.
        kline_type: K线级别, `kline`, `kline_5min`, `kline_15min` ... and so on.
    """

    def __init__(self, platform=None, symbol=None, open=None, high=None, low=None, close=None, volume=None,
                 coin_volume=None, timestamp=None, kline_type=None):
        """Initialize."""
        self.platform = platform
        self.symbol = symbol
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.coin_volume = coin_volume
        self.timestamp = timestamp
        self.kline_type = kline_type

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "coin_volume": self.coin_volume,
            "timestamp": self.timestamp,
            "kline_type": self.kline_type
        }
        return d

    @property
    def smart(self):
        d = {
            "p": self.platform,
            "s": self.symbol,
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
            "cv": self.coin_volume,
            "t": self.timestamp,
            "kt": self.kline_type
        }
        return d

    def load_smart(self, d):
        self.platform = d["p"]
        self.symbol = d["s"]
        self.open = d["o"]
        self.high = d["h"]
        self.low = d["l"]
        self.close = d["c"]
        self.volume = d["v"]
        self.coin_volume = d["cv"]
        self.timestamp = d["t"]
        self.kline_type = d["kt"]
        return self

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)

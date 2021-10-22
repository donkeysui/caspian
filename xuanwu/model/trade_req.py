# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/9/24 13:44
  @ Description: 
  @ History:
"""
import json


class TradeReq:
    """ Trade object.

    Args:
        platform: Exchange platform name, e.g. huobi_swap.
        symbol: Trade pair name, e.g. BTC-USD.
        action: Trade action, BUY or SELL.
        price: Order place price.
        quantity: Order place quantity.
        timestamp: Update time, millisecond.
    """

    def __init__(self, platform=None, symbol=None, action=None, price=None, quantity=None, timestamp=None):
        """ Initialize. """
        self.platform = platform
        self.symbol = symbol
        self.action = action
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "action": self.action,
            "price": self.price,
            "quantity": self.quantity,
            "timestamp": self.timestamp
        }
        return d

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)
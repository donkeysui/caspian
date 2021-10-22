# -*- coding: UTF-8 -*-
"""
 * @Title: symbol_imfo
 * @ProjectName: xuanwu
 * @Description: TODO
 * @Author: Mr.Hat
 * @Email: shenghong6560@gmail.com
 * @Date: 2020/9/2020:53
"""
import json


class SymbolInfo:
    """ 符号的相关信息

    Attributes:
        platform: Exchange platform_old name, e.g. binance/bitmex.
        symbol: Trading pair name, e.g. ETH/BTC.
        price_tick: `报价`每一跳的最小单位,也可以理解为`报价`精度或者`报价`增量
        size_tick: `下单量`每一跳的最小单位,也可以理解为`下单量`精度或者`下单量`增量
        size_limit: 最小`下单量` (`下单量`指当订单类型为`限价单`或`sell-market`时,下单接口传的'quantity')
        value_tick: `下单金额`每一跳的最小单位,也可以理解为`下单金额`精度或者`下单金额`增量
        value_limit: 最小`下单金额` (`下单金额`指当订单类型为`限价单`时,下单接口传入的(quantity * price).当订单类型为`buy-market`时,下单接口传的'quantity')
        base_currency: 交易对中的基础币种
        quote_currency: 交易对中的报价币种
        settlement_currency: 交易对中的结算币种
        symbol_type: 符号类型,"spot"=现货,"future"=期货
        is_inverse: 如果是期货的话,是否是反向合约
        multiplier: 合约乘数,合约大小
    """

    def __init__(self, platform=None, symbol=None, price_tick: float = None, size_tick: float = None,
                 size_limit: float = None, value_tick: float = None, value_limit: float = None, base_currency="",
                 quote_currency="", settlement_currency="", symbol_type="spot", is_inverse=False, multiplier=1):
        self.platform = platform
        self.symbol = symbol
        self.price_tick = price_tick
        self.size_tick = size_tick
        self.size_limit = size_limit
        self.value_tick = value_tick
        self.value_limit = value_limit
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.settlement_currency = settlement_currency
        self.symbol_type = symbol_type
        self.is_inverse = is_inverse
        self.multiplier = multiplier

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "symbol": self.symbol,
            "price_tick": self.price_tick,
            "size_tick": self.size_tick,
            "size_limit": self.size_limit,
            "value_tick": self.value_tick,
            "value_limit": self.value_limit,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "settlement_currency": self.settlement_currency,
            "symbol_type": self.symbol_type,
            "is_inverse": self.is_inverse,
            "multiplier": self.multiplier
        }
        return d

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)
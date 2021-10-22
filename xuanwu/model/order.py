# -*- coding: utf-8 -*-
'''
  @ Copyright (C), www.honglianziben.com
  @ FileName: order_pojo
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/5/14 19:32
  @ Description: 
  @ History:
  @ <author>          <time>          <version>          <desc>
  @ 作者姓名           修改时间           版本号            描述
'''
from xuanwu.utils import tools
import json

# maker or taker
LIQUIDITY_TYPE_TAKER = "TAKER"
LIQUIDITY_TYPE_MAKER = "MAKER"

# Order type.
ORDER_TYPE_LIMIT = "LIMIT"  # Limit order.
ORDER_TYPE_MARKET = "MARKET"  # Market order.
ORDER_TYPE_MAKER = "POST_ONLY"  # Market order.
ORDER_TYPE_FOK = "FOK"  # FOK order.
ORDER_TYPE_IOC = "IOC"  # IOC order.
ORDER_TYPE_LIMIT_IOC = "OPTIMAL_LIMIT_IOC"  # IOC order.


# Order direction.
ORDER_ACTION_BUY = "BUY"  # Buy
ORDER_ACTION_SELL = "SELL"  # Sell

# Order status.
ORDER_STATUS_NONE = "NONE"  # New created order, no status.
ORDER_STATUS_SUBMITTED = "SUBMITTED"  # The order that submitted to server successfully.
ORDER_STATUS_PARTIAL_FILLED = "PARTIAL-FILLED"  # The order that filled partially.
ORDER_STATUS_FILLED = "FILLED"  # The order that filled fully.
ORDER_STATUS_CANCELED = "CANCELED"  # The order that canceled.
ORDER_STATUS_FAILED = "FAILED"  # The order that failed.

# Future order trade type.
TRADE_TYPE_NONE = 0  # Unknown type, some Exchange's order information couldn't known the type of trade.
TRADE_TYPE_BUY_OPEN = 1  # Buy open, action = BUY & quantity > 0.
TRADE_TYPE_SELL_OPEN = 2  # Sell open, action = SELL & quantity < 0.
TRADE_TYPE_SELL_CLOSE = 3  # Sell close, action = SELL & quantity > 0.
TRADE_TYPE_BUY_CLOSE = 4  # Buy close, action = BUY & quantity < 0.


class Order:
    def __init__(self, account=None, platform=None, strategy=None, order_no=None, symbol=None, action=None, price=0,
                 quantity=0, remain=0, status=ORDER_STATUS_NONE, avg_price=0, order_type=ORDER_TYPE_LIMIT,
                 trade_type=TRADE_TYPE_NONE, client_order_id=None, order_price_type=None, role=None,
                 trade_quantity=0, trade_price=0, ctime=None, utime=None):
        """订单对象类.
        Attributes:
            :param platform: 交易所名称.
            :param account: 交易账户名称, e.g. `demo@gmail.com`.
            :param strategy: 策略名称, e.g. `my_strategy`.
            :param order_no: 订单id.
            :param symbol: 交易币对名称.
            :param action: 交易方向. "BUY", "SELL"
            :param price: 委托价格.
            :param quantity: 委托数量.
            :param remain: 剩余未成交订单数量.
            :param status: 订单状态.
                           NONE：无状态,
                           SUBMITTED：已提交,但未成交,
                           PARTIAL-FILLED：部分成交,
                           FILLED: 完全成交
                           CANCELED：取消,
                           FAILED：失败
            :param avg_price: 成交平均价.
            :param order_type: 订单类型，LIMIT，MARKET等.
            :param trade_type: 交易类型, 仅适用于合约交易.
                               "NONE"：无类型
                               "BUY"：买入
                               "SELL"：卖出
            :param client_order_id: 订单客户端id.
            :param order_price_type:
            :param role: 成交角色 taker或maker
            :param trade_quantity： 此次提交订单的成交量
            :param trade_price: 撮合价格
            :param ctime:订单创建时间, millisecond.
            :param utime: 订单更新时间, millisecond.
        """
        self.platform = platform
        self.account = account
        self.strategy = strategy
        self.symbol = symbol
        self.order_no = order_no
        self.action = action
        self.order_type = order_type
        self.price = price
        self.quantity = quantity
        self.remain = remain if remain else quantity
        self.status = status
        self.avg_price = avg_price
        self.trade_type = trade_type
        self.client_order_id = client_order_id
        self.order_price_type = order_price_type
        self.role = role
        self.trade_quantity = trade_quantity
        self.trade_price = trade_price
        self.ctime = ctime if ctime else tools.get_cur_timestamp_ms()
        self.utime = utime if utime else tools.get_cur_timestamp_ms()

    def __str__(self):
        d = {
            "platform": self.platform,
            "account": self.account,
            "strategy": self.strategy,
            "symbol": self.symbol,
            "order_no": self.order_no,
            "action": self.action,
            "order_type": self.order_type,
            "price": self.price,
            "quantity": self.quantity,
            "remain": self.remain,
            "status": self.status,
            "avg_price": self.avg_price,
            "trade_type": self.trade_type,
            "client_order_id": self.client_order_id,
            "order_price_type": self.order_price_type,
            "role": self.role,
            "trade_quantity": self.trade_quantity,
            "trade_price": self.trade_price,
            "ctime": self.ctime,
            "utime": self.utime
        }
        info = json.dumps(d)
        return info

    def __repr__(self):
        return str(self)


class Fill:
    """ Fill object.

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        account: Trading account name, e.g. test@gmail.com.
        symbol: Trading pair name, e.g. ETH/BTC.
        strategy: Strategy name, e.g. my_test_strategy.
        order_no: 哪个订单发生了成交
        fill_no: 成交ID
        price: 成交价格
        quantity: 成交数量
        side: 成交方向买还是卖
        liquidity: 是maker成交还是taker成交
        fee: 成交手续费
        ctime: 成交时间
    """

    def __init__(self, platform=None, account=None, symbol=None, strategy=None, order_no=None, fill_no=None,
                 price=0, quantity=0, side=None, liquidity=None, fee=0, ctime=None):
        self.platform = platform
        self.account = account
        self.symbol = symbol
        self.strategy = strategy
        self.order_no = order_no
        self.fill_no = fill_no
        self.price = price
        self.quantity = quantity
        self.side = side
        self.liquidity = liquidity
        self.fee = fee
        self.ctime = ctime if ctime else tools.get_cur_timestamp_ms()

    def __str__(self):
        info = "[platform: {platform}, account: {account}, symbol: {symbol}, strategy: {strategy}, " \
               "order_no: {order_no}, fill_no: {fill_no}, price: {price}, quantity: {quantity}, side: {side}, " \
               "liquidity: {liquidity}, fee: {fee}, ctime: {ctime}]".format(
                platform=self.platform, account=self.account, symbol=self.symbol, strategy=self.strategy,
                order_no=self.order_no,
                fill_no=self.fill_no, price=self.price, quantity=self.quantity, side=self.side,
                liquidity=self.liquidity, fee=self.fee, ctime=self.ctime)
        return info

    def __repr__(self):
        return str(self)

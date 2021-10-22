# -*- coding: utf-8 -*-
'''
  @ Copyright (C), www.honglianziben.com
  @ FileName: account_pojo
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/5/26 12:41
  @ Description: 
  @ History:
  @ <author>          <time>          <version>          <desc>
  @ 作者姓名           修改时间           版本号            描述
'''
import json
from xuanwu.utils import tools


class Account:
    """用户账户详情.

    Attributes:
        platform: 交易所名称, e.g. `binance` / `bitmex`.
        account: 交易账户名称, e.g. `demo@gmail.com`.
        strategy: 策略名称, e.g. `my_strategy`.
        symbol: 交易对名称, e.g. `ETH/BTC`.
    """

    def __init__(self, platform=None, account=None, strategy=None, currency=None):
        self.platform = platform
        self.account = account
        self.strategy = strategy
        self.currency = currency  # 账户持有货币种类 BTC、ETH...
        self.balance = 0.0  # 账户动态权益，当前账户资产状态
        self.total_avail_balance = 0.0  # 账户余额，减去保证金部分余额
        self.margin = 0.0  # 已使用保证金数量
        # 以下参数合约中使用
        self.realized_pnl = 0.0  # 未平仓盈利，浮动盈亏
        self.unrealized_pnl = 0.0  # 已平仓盈利，已盈利金额
        self.margin_ratio = 0.0  # 保证金率
        self.can_withdraw = None  # 可划转数量
        self.timestamp = None  # 账户更新时间

    def update(self, balance=0.0, total_avail_balance=0.0, margin=0.0, realized_pnl=0.0, unrealized_pnl=0.0,
               margin_ratio=0.0, can_withdraw=0.0, timestamp=None):
        self.balance = balance
        self.total_avail_balance = total_avail_balance
        self.margin = margin
        self.realized_pnl = realized_pnl
        self.unrealized_pnl = unrealized_pnl
        self.margin_ratio = margin_ratio
        self.can_withdraw = can_withdraw
        self.timestamp = timestamp if timestamp else tools.get_cur_timestamp_ms()

    @property
    def data(self):
        d = {
            "platform": self.platform,
            "account": self.account,
            "strategy": self.strategy,
            "currency": self.currency,
            "balance": self.balance,
            "total_avail_balance": self.total_avail_balance,
            "margin": self.margin,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "margin_ratio": self.margin_ratio,
            "can_withdraw": self.can_withdraw,
            "timestamp": self.timestamp
        }
        return d

    def __str__(self):
        info = json.dumps(self.data)
        return info

    def __repr__(self):
        return str(self)

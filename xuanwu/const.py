# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/9/27 17:17
  @ Description:
  @ History:
"""
# Version
VERSION = "1.0.26_200802"
# REQUEST AGENT
USER_AGENT = "XuanWuQuant" + VERSION

# 交易所名称常量
HUOBI = "huobi"  # huobi现货
HUOBI_FUTURE = "huobi_future"  # 火币合约
HUOBI_SWAP = "huobi_swap"  # 火币永续
HUOBI_USDT_SWAP = "huobi_usdt_swap"  # Huobi Usdt Swap
HUOBI_USDT_SWAP_CROSS = "huobi_usdt_swap_cross"  # Huobi USDT SWAP CROSS MODE
OKEX = "okex"  # okex现货
OKEX_FUTURE = "okex_future"  # okex合约
OKEX_SWAP = "okex_swap"  # okex永续
OKEX_U_SWAP = "okex_u_swap"  # okex永续
OKEX_V5 = "okex_v5"  # okex V5
BINANCE_U_SWAP = "binance_u_swap"
FTX = "ftx"

# 频道类型常量
CHANNEL_TYPE = ["spot", "margin", "futures", "swap", "option"]
CHANNEL_DETAIL = ["ticker", "depth", "trade", "order", "position"]
KLINE_TYPE = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

CHANNEL = ["orderbook", "kline", "trade"]

# Market Types
MARKET_TYPE_TRADE = "trade"
MARKET_TYPE_ORDERBOOK = "orderbook"
MARKET_TYPE_KLINE = "kline"


EXCHANGES = {
    FTX: {
        "name": FTX,
        "describe": "FTX Exchange,FTX 现货 & 合约",
        "url": "https://ftx.com",
        "document": "https://docs.ftx.com",
        "host": "https://ftx.com",
        "wss": "wss://ftx.com",
        "rest_api": "aioquant.platform.ftx.ftx.FtxRestAPI",
        "trader": "aioquant.platform.ftx.ftx.FtxTrader",
        "market": "aioquant.platform.ftx.ftx.FtxMarket"
    }
}

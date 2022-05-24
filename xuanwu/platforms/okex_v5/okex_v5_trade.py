# -*- coding: utf-8 -*-
"""
  @ Author:   Turkey
  @ Email:    suiminyan@gmail.com
  @ Date:     2021/9/22 10:40
  @ Description: 
  @ History:
"""
import copy
import hmac
import base64
import time
import asyncio
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position, CROSS, ISOLATED
from xuanwu.model.symbol_info import SymbolInfo
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask
from xuanwu.const import OKEX_V5
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *
from xuanwu.utils.tools import decimal_digits
from .okex_v5_rest import OkexV5Rest

__all__ = ("OkexV5Trade",)


class OkexV5Trade(Websocket):
    def __init__(self, **kwargs):
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("contract_type"):
            e = Error("param contract_type miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://www.okex.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://ws.okex.com:8443"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("passphrase"):
            e = Error("param passphrase miss")
        if not kwargs.get("order_update_callback"):
            e = Error("param order_update_callback miss")
        if not kwargs.get("position_update_callback"):
            e = Error("param position_update_callback miss")
        if not kwargs.get("asset_update_callback"):
            e = Error("param asset_update_callback miss")
        if not kwargs.get("init_success_callback"):
            e = Error("param init_success_callback miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return
        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = OKEX_V5
        self._symbol = kwargs["symbol"]
        self._contract_type = kwargs["contract_type"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        self._assets = {}  # Asset object. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order objects. e.g. {"order_no": Order, ... }
        self._position = Position(platform=self._platform,
                                  account=self._account,
                                  strategy=self._strategy,
                                  symbol=self._symbol)
        self._symbol_info = SymbolInfo(platform=self._platform)

        # If our channels that subscribed successfully.
        self._subscribe_order_ok = False
        self._subscribe_assets_ok = False
        self._subscribe_position_ok = False

        self.heartbeat_msg = "ping"

        self._rest_api = OkexV5Rest(self._host, self._access_key, self._secret_key, self._passphrase)
        url = self._wss + "/ws/v5/private"
        super(OkexV5Trade, self).__init__(url, send_hb_interval=15)
        self.initialize()

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def symbol_info(self):
        return copy.copy(self._symbol_info)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def position(self):
        return copy.copy(self._position)

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        timestamp = int(time.time())
        msg = str(timestamp) + "GET" + '/users/self/verify'
        mac = hmac.new(bytes(self._secret_key, encoding='utf8'), bytes(msg, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        sign = base64.b64encode(d)
        login_param = {
            "op": "login",
            "args": [{
                "apiKey": self._access_key,
                "passphrase": self._passphrase,
                "timestamp": timestamp,
                "sign": sign.decode("utf-8")
            }]
        }
        await self.ws.send_json(login_param)

    async def _sub_callback(self):
        """数据订阅之后，初始化账户未成交订单和交易币对信息"""
        # 查询未成交订单(未成交订单和部分成交)
        orders, error = await self.get_open_orders()
        if error:
            logger.error(f"get_orders error: {orders}, {error}", caller=self)
            # 初始化过程中发生错误,关闭网络连接,触发重连机制
            return
        for o in orders:
            SingleTask.run(self._order_update_callback, o)

        position, error = await self.get_position()
        if error:
            logger.error(f"get_position error: {error}", caller=self)
            return
        SingleTask.run(self._position_update_callback, position)

        # 查询交易币对基础规则及数据
        success, error = await self._rest_api.get_all_markets(inst_type=self._contract_type.upper(),
                                                              symbol=self._symbol)
        if error:
            e = Error(f"get symbol info failed!, {error}")
            logger.error(e, caller=self)
            return
        if success and success["data"]:
            data = success["data"]
            for d in data:
                self._symbol_info = SymbolInfo(platform=self._platform)
                self._symbol_info.symbol = d["instId"]
                self._symbol_info.price_tick = d["tickSz"]
                self._symbol_info.size_tick = d["tickSz"]
                self._symbol_info.size_limit = d["minSz"]
                self._symbol_info.value_tick = None
                self._symbol_info.value_limit = None
                if d["instType"] in ["SPOT", "MARGIN"]:
                    self._symbol_info.base_currency = d["baseCcy"]
                    self._symbol_info.quote_currency = d["quoteCcy"]
                    self._symbol_info.settlement_currency = d["quoteCcy"]
                else:
                    self._symbol_info.base_currency = d["ctValCcy"]
                    self._symbol_info.quote_currency = d["settleCcy"]
                    self._symbol_info.settlement_currency = d["settleCcy"]
                self._symbol_info.symbol_type = d["instType"]
                self._symbol_info.is_inverse = True if d["ctType"] == "inverse" else False
                self._symbol_info.multiplier = d["ctVal"]

    async def _login_callback(self):
        """登录成功之后，订阅相关数据"""
        pos_channel = self._symbol_to_channel(symbol=self._symbol, channel_type="position")
        if not pos_channel:
            return
        data = {
            "op": "subscribe",
            "args": [pos_channel]
        }
        await self.ws.send_json(data)

        order_channel = self._symbol_to_channel(symbol=self._symbol, channel_type="order")
        if not order_channel:
            return
        data = {
            "op": "subscribe",
            "args": [order_channel]
        }
        await self.ws.send_json(data)

        account_channel = self._symbol_to_channel(symbol=self._symbol, channel_type="account")
        if not account_channel:
            return
        data = {
            "op": "subscribe",
            "args": [account_channel]
        }
        await self.ws.send_json(data)

    @async_method_locker("OkexV5Trade.process.locker")
    async def process(self, msg):
        if isinstance(msg, str) and msg == "pong":
            return
        if msg.get("event"):
            if msg.get("event") == "error":
                logger.error(f"私有接口订阅失败, error msg: {msg}", caller=self)
                return
            elif msg.get("event") == "login":
                logger.info("Private API Connected Successed!")
                await self._login_callback()
            elif msg.get("event") == "subscribe":
                if msg.get("arg")["channel"] == "account":
                    self._subscribe_assets_ok = True
                if msg.get("arg")["channel"] == "positions":
                    self._subscribe_position_ok = True
                if msg.get("arg")["channel"] == "orders":
                    self._subscribe_order_ok = True
            else:
                pass

            if self._subscribe_order_ok and self._subscribe_position_ok and self._subscribe_assets_ok:
                await self._sub_callback()
                SingleTask.run(self._init_success_callback, True)

        else:
            if msg.get("arg"):
                if msg.get("arg")["channel"] == "account":
                    self._update_asset(msg)
                if msg.get("arg")["channel"] == "positions":
                    self._update_position(msg)
                if msg.get("arg")["channel"] == "orders":
                    self._update_order(msg)

    async def process_binary(self, msg):
        """只继承，不实现"""
        pass

    def _update_position(self, data):
        data = data.get("data")
        if data:
            for d in data:
                if d["instId"] != self._symbol:
                    continue
                else:
                    self._position.margin_mode = CROSS if d["mgnMode"] == CROSS else ISOLATED
                    pos_side = d["posSide"]
                    pos_size = 0 if d["pos"] == "" or d["pos"] == "0" else float(d["pos"])

                    """
                        多头条件：
                            1、币币杠杠： posCcy为交易货币时，代表多头；posCcy为计价货币时，代表空头
                            2、其他产品类型下：
                                a、单向持仓：pos > 0 多头， pso < 0 空头
                                b、双向持仓：long：双向持仓多头， short：双向持仓空头
                    """
                    avail_qty = 0 if d["availPos"] == "" or d["availPos"] == "0" else float(d["availPos"])
                    open_price = 0 if d["avgPx"] == "" or d["avgPx"] == "0" else float(d["avgPx"])
                    unrealised_pnl = 0 if d["upl"] == "" or d["upl"] == "0" else float(d["upl"])
                    liquid_price = 0 if d["liqPx"] == "" or d["liqPx"] == "0" else float(d["liqPx"])
                    margin = 0 if d["margin"] == "" or d["margin"] == "0" else float(d["margin"])

                    if pos_side == "long" or (pos_side == "net" and pos_size > 0) or \
                            d["posCcy"] == self._symbol_info.base_currency:
                        self._position.long_quantity = pos_size
                        self._position.long_avail_qty = avail_qty
                        self._position.long_open_price = open_price
                        self._position.long_unrealised_pnl = unrealised_pnl
                        self._position.long_leverage = d["lever"]
                        self._position.long_liquid_price = liquid_price
                        self._position.long_margin = margin

                    elif pos_side == "short" or (pos_side == "net" and float(pos_size) < 0) or \
                            d["posCcy"] == self._symbol_info.quote_currency:
                        self._position.short_quantity = pos_size
                        self._position.short_avail_qty = avail_qty
                        self._position.short_open_price = open_price
                        self._position.short_unrealised_pnl = unrealised_pnl
                        self._position.short_leverage = d["lever"]
                        self._position.short_liquid_price = liquid_price
                        self._position.short_margin = margin

                    self._position.utime = d["uTime"]
                    self._position.ctime = d["cTime"]

            SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def _update_order(self, data):
        if data.get("arg").get("instId") != self._symbol:
            return
        if data.get("data"):
            for d in data.get("data"):
                o = self._convert_order_format(d)
                SingleTask.run(self._order_update_callback, o)
                if self._orders.get(o.order_no):
                    if o.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
                        self._orders.pop(o.order_no)
                    else:
                        self._orders[o.order_no] = o
                else:
                    self._orders[o.order_no] = o

    def _update_asset(self, data):
        if data.get("data"):
            ast = self._convert_asset_format(data.get("data")[0])
            SingleTask.run(self._asset_update_callback, copy.copy(ast))

    def _symbol_to_channel(self, symbol, channel_type):
        """ Convert symbol to channel.

        Attributes:
            symbol: Trade pair name.such as BTC-USD
            channel_type: channel name, kline / ticker / orderbook.
        """
        if channel_type == "account":
            channel = {
                "channel": "account"
            }
        elif channel_type == "position":
            channel = {
                "channel": "positions",
                "instType": self._contract_type.upper(),
                "instId": symbol
            }
        elif channel_type == "order":
            channel = {
                "channel": "orders",
                "instType": self._contract_type.upper(),
                "instId": symbol
            }
        else:
            logger.error("channel type error! channel type:", channel_type, caller=self)
            return None
        return channel

    def _convert_asset_format(self, data):
        for d in data["details"]:
            symbol = d["ccy"]
            total = d["eq"]
            free = d["availEq"]
            locked = d["frozenBal"]
            self._assets[symbol] = {
                "total": total,
                "free": free,
                "locked": locked
            }
        timestamp = data["uTime"]
        ast = Asset(
            platform=self._platform,
            account=self._account,
            assets=self._assets,
            timestamp=timestamp,
            update=True
        )
        return ast

    def _convert_order_format(self, data):
        symbol = data["instId"]
        order_no = data["ordId"]
        side = data["side"]
        pos_side = data["posSide"]
        trade_type = None
        if side == "buy":
            if pos_side == "long":  # 买入开多
                trade_type = TRADE_TYPE_BUY_OPEN
            if pos_side == "short":  # 买入平空
                trade_type = TRADE_TYPE_BUY_CLOSE
        if side == "sell":
            if pos_side == "long":  # 卖出平多
                trade_type = TRADE_TYPE_SELL_CLOSE
            if pos_side == "short":  # 卖出开空
                trade_type = TRADE_TYPE_SELL_OPEN

        quantity = data["sz"]
        if self._symbol_info.size_limit:
            symbol_size_limit = decimal_digits(float(self._symbol_info.size_limit))
        else:
            symbol_size_limit = 6
        # below row was edited by Turkey Sui 2021/10/13 1:22 A.M.
        remain = quantity if data["accFillSz"] == "" or data["accFillSz"] == "0" else \
            round(float(quantity) - float(data["accFillSz"]), symbol_size_limit)

        state = data["state"]
        """
        canceled：撤单成功
        live：等待成交
        partially_filled：部分成交
        filled：完全成交
        """
        if state == "canceled":
            status = ORDER_STATUS_CANCELED
        elif state == "live":
            status = ORDER_STATUS_SUBMITTED
        elif state == "partially_filled":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "filled":
            status = ORDER_STATUS_FILLED
        else:
            status = ORDER_STATUS_NONE
        order_type = None
        if data["ordType"] == "market":
            order_type = ORDER_TYPE_MARKET
        elif data["ordType"] == "limit":
            order_type = ORDER_TYPE_LIMIT
        elif data["ordType"] == "post_only":
            order_type = ORDER_TYPE_MAKER
        elif data["ordType"] == "fok":
            order_type = ORDER_TYPE_FOK
        elif data["ordType"] == "ioc":
            order_type = ORDER_TYPE_IOC
        elif data["ordType"] == "optimal_limit_ioc":
            order_type = ORDER_TYPE_LIMIT_IOC
        else:
            pass
        trade_quantity = 0 if data["accFillSz"] == "" or data["accFillSz"] == "0" else float(data["accFillSz"])
        avg_price = 0 if data["avgPx"] == "" or data["avgPx"] == "0" else float(data["avgPx"])
        trade_price = 0 if data["fillPx"] == "" or data["fillPx"] == "0" else float(data["fillPx"])
        info = {
                "account": self._account,
                "platform": self._platform,
                "strategy": self._strategy,
                "order_no": order_no,
                "symbol": symbol,
                "action": ORDER_ACTION_BUY if side == "buy" else ORDER_ACTION_SELL,
                "price": float(data["px"]),
                "quantity": float(quantity),
                "remain": float(remain),
                "status": status,
                "avg_price": avg_price,
                "order_type": order_type,
                "trade_type": trade_type,
                "client_order_id": data["clOrdId"],
                "trade_quantity": trade_quantity,
                "trade_price": trade_price,
                "ctime": data["cTime"],
                "utime": data["uTime"]
        }
        return Order(**info)

    async def create_order(self, td_mode, side, price, quantity, order_type=ORDER_TYPE_LIMIT, **kwargs):
        """ 创建订单，二次封装，如果需要请求更加复杂的订单，需要通过rest接口请求原始订单请求类型
        Attributes:
            :param td_mode: 交易模式 isolated：逐仓 ；cross：全仓；cash：非保证金
            :param side: 订单交易方向， BUY 或者 SELL
            :param price: 委托价格
            :param quantity: 交易数量, 数量大于0则为多单，数量小于0则为空单
            :param order_type: 订单类型，默认是limit订单
            kwargs:
                client_no: 客户端ID
        :returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if quantity > 0:
            if side == ORDER_ACTION_BUY:  # 买入开多
                direction = "buy"
                offset = "long"
            elif side == ORDER_ACTION_SELL:  # 卖出平多
                direction = "sell"
                offset = "long"
            else:
                return None, "action error"
        elif quantity < 0:
            if side == ORDER_ACTION_BUY:  # 买入平空
                direction = "buy"
                offset = "short"
            elif side == ORDER_ACTION_SELL:  # 卖出开空
                direction = "sell"
                offset = "short"
            else:
                return None, "action error"
        else:
            return None, "quantity error"

        if order_type == ORDER_TYPE_LIMIT:  # 限价单
            order_price_type = "limit"
        elif order_type == ORDER_TYPE_MARKET:  # 市价单
            order_price_type = "market"
        elif order_type == ORDER_TYPE_MAKER:  # 只做maker单
            order_price_type = "post_only"
        elif order_type == ORDER_TYPE_FOK:  # 全部成交或立即取消
            order_price_type = "fok"
        elif order_type == ORDER_TYPE_IOC:  # 立即成交并取消剩余
            order_price_type = "ioc"
        elif order_type == ORDER_TYPE_LIMIT_IOC:  # 市价委托立即成交并取消剩余（仅适用交割、永续）
            order_price_type = "optimal_limit_ioc"
        else:
            return None, "order type error"

        quantity = abs(int(quantity))
        result, error = await self._rest_api.create_order(
                                                symbol=self._symbol,
                                                td_mode=td_mode,
                                                side=direction,
                                                sz=str(quantity),
                                                ord_type=order_price_type,
                                                cl_ordId=kwargs.get("client_no") if kwargs.get("client_no") else "",
                                                pos_side=offset,
                                                px=str(price)
                                            )
        if error:
            return None, error
        if result["code"] != "0":
            return None, result
        return str(result["data"][0]["ordId"]), None

    # This function was added by Donkey Khan 2021/10/12 00:24
    # Designed to create orders of RAW list(dict(Order))
    async def create_orders_raw(self, orders):
        result, error = await self._rest_api.create_orders(orders)
        if error:
            return None, error
        if result["code"] != "0":
            return None, result
        return str(result), None

    async def revoke_order(self, order_nos):
        """ 删除订单
        Attributes:
            :param order_nos: 订单号码列表
                              如果传入0，则删除所有挂单，
                              否则删除指定订单，最大数量为20
        :returns:Success or error, see bellow.
        """
        if len(order_nos) == 0:  # 删除指定符号下所有订单
            result, error = await self._rest_api.get_open_orders(symbol=self._symbol)
            if error:
                return False, error
            if result.get("data"):
                order_detail = [{"instId": self._symbol, "ordId": d["ordId"]} for d in result.get("data")]
                for i in order_detail:
                    success, error = await self._rest_api.revoke_order(symbol=i["instId"], order_id=i["ordId"])
                    if error:
                        return False, error
                    await asyncio.sleep(0.1)
                return True, None

        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(symbol=self._symbol, order_id=order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None
        if len(order_nos) > 1:
            order_detail = [{"instId": self._symbol, "ordId": d} for d in order_nos]
            success, error = await self._rest_api.revoke_orders(order_detail)
            if error:
                return False, error
            if success and success.get("code") == "0":
                order_ids = [i["ordId"] for i in success.get("data")]
            else:
                return None, success
            return order_ids, None

    async def get_open_orders(self):
        """ 获取当前币对挂单信息
        Attributes:
            None

        :returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(symbol=self._symbol)
        order_ids = []
        if error:
            return None, error
        if success and success.get("data"):
            for d in success.get("data"):
                res = self._convert_order_format(data=d)
                order_ids.append(res)
            return order_ids, None

        return None, success

    async def get_assets(self):
        """ 获取当前账户资产信息
        Attributes:
            None

        :return:
        """
        success, error = await self._rest_api.get_asset_info()
        if error:
            return None, error
        if success["code"] != "0":
            return None, success
        ast = self._convert_asset_format(success["data"][0])
        return ast, None

    async def get_position(self):
        """ 获取当前账户仓位详情
        Attributes:
            None

        :returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_position(symbol=self._symbol)
        if error:
            return None, error
        if success["code"] != "0":
            return None, success
        else:
            position = Position(platform=self._platform, account=self._account,
                                strategy=self._strategy, symbol=self._symbol)
            for d in success["data"]:
                if d["instId"] != self._symbol:
                    continue
                else:
                    position.margin_mode = CROSS if d["mgnMode"] == CROSS else ISOLATED
                    pos_side = d["posSide"]
                    pos_size = 0 if d["pos"] == "" or d["pos"] == "0" else float(d["pos"])

                    """
                        多头条件：
                            1、币币杠杠： posCcy为交易货币时，代表多头；posCcy为计价货币时，代表空头
                            2、其他产品类型下：
                                a、单向持仓：pos > 0 多头， pso < 0 空头
                                b、双向持仓：long：双向持仓多头， short：双向持仓空头
                    """
                    avail_qty = 0 if d["availPos"] == "" or d["availPos"] == "0" else float(d["availPos"])
                    open_price = 0 if d["avgPx"] == "" or d["avgPx"] == "0" else float(d["avgPx"])
                    unrealised_pnl = 0 if d["upl"] == "" or d["upl"] == "0" else float(d["upl"])
                    liquid_price = 0 if d["liqPx"] == "" or d["liqPx"] == "0" else float(d["liqPx"])
                    margin = 0 if d["margin"] == "" or d["margin"] == "0" else float(d["margin"])

                    if pos_side == "long" or (pos_side == "net" and pos_size > 0) or \
                            d["posCcy"] == self._symbol_info.base_currency:
                        position.long_quantity = pos_size
                        position.long_avail_qty = avail_qty
                        position.long_open_price = open_price
                        position.long_unrealised_pnl = unrealised_pnl
                        position.long_leverage = d["lever"]
                        position.long_liquid_price = liquid_price
                        position.long_margin = margin

                    elif pos_side == "short" or (pos_side == "net" and float(pos_side) < 0) or \
                            d["posCcy"] == self._symbol_info.quote_currency:
                        position.short_quantity = pos_size
                        position.short_avail_qty = avail_qty
                        position.short_open_price = open_price
                        position.short_unrealised_pnl = unrealised_pnl
                        position.short_leverage = d["lever"]
                        position.short_liquid_price = liquid_price
                        position.short_margin = margin

                    position.utime = d["uTime"]
                    position.ctime = d["cTime"]

            return position, None

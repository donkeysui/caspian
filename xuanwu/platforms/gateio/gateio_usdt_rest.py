# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/9/26 10:21
  @ Description: 
  @ History:
"""
import hmac
import base64
import datetime
from urllib.parse import urljoin
from xuanwu.const import CHANNEL_TYPE
from xuanwu.model.order import *
from xuanwu.utils import logger
from xuanwu.utils.http_client import AsyncHttpRequests

__all__ = ("GateIORest",)

INST_TYPE = [i.upper() for i in CHANNEL_TYPE]
K_DATE = ["1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "1W", "1M", "3M", "6M", "1Y"]
ORDER_TYPE = [ORDER_TYPE_LIMIT.lower(), ORDER_TYPE_MARKET.lower(), ORDER_TYPE_MAKER.lower(), ORDER_TYPE_FOK.lower(),
              ORDER_TYPE_IOC.lower(), "optimal_limit_ioc"]


class GateIORest:
    """ Okex V5 REST API Client.

    Attributes:
        :param host: HTTP request host.
        :param access_key: Account's ACCESS KEY.
        :param secret_key: Account's SECRET KEY.
        :param passphrase: Account's PASSPHRASE.
    """

    def __init__(self, host, access_key, secret_key, passphrase):
        """initialize REST API client."""
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase

    # --------------------------------------------------- Public API ---------------------------------------------------

    async def get_all_markets(self, inst_type, uly="", symbol=""):
        """ 获取所有可交易品种的信息列表
        Attributes:
            :param inst_type: 必填参数
                             SPOT：币币
                             MARGIN：币币杠杆
                             SWAP：永续合约
                             FUTURES：交割合约
                             OPTION：期权
            :param uly: 可选参数
                        合约标的指数，仅适用于交割/永续/期权，期权必填
            :param symbol: 交易币对ID，如果不填写则返回对应产品旗下的所有币对信息

            Note: 1、只填写inst_type参数，则返回对应类型下的所有可交易币对信息
                  2、填写inst_type和symbol，要确保产品类型和币对信息统一，返回对应账户类型下的单一币对数据
                  3、期权请求必须添加uly参数
        :return:
        """
        params = {}
        uri = "/api/v5/public/instruments"
        if inst_type in INST_TYPE:
            params["instType"] = inst_type
        else:
            logger.error("账户类型错误，请重新输入")
        if uly:
            params["uly"] = uly
        if symbol:
            params["instId"] = symbol
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_funding_rate(self, symbol):
        """ 获取当前资金费率
        Attributes:
            :param symbol: 永续合约币对
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/public/funding-rate"
        params = {"instId": symbol}
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_funding_rate_history(self, symbol, after="", before="", limit="100"):
        """ 获取永续合约资金费率历史
        Attributes:
            :param symbol: 永续合约币对信息
            :param after: 请求此时间戳之前（更旧的数据）的分页内容，传的值为对应接口的fundingTime
            :param before: 请求此时间戳之后（更新的数据）的分页内容，传的值为对应接口的fundingTime
            :param limit:分页返回的结果集数量，最大为100，不填默认返回100条
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/public/funding-rate-history"
        params = {"instId": symbol}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        if limit > "100":
            limit = "100"
        params["limit"] = limit
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_estimated_price(self, symbol):
        """ 获取交割合约和期权预估交割/行权价。交割/行权预估价只有交割/行权前一小时才有返回值
        Attributes:
            :param symbol: 交易币对ID， 仅适用于交割/期权
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/public/estimated-price"
        success, error = await self.request("GET", uri, params={"instId": symbol})
        return success, error

    async def get_system_time(self):
        """获取系统时间"""
        uri = "/api/v5/public/time"
        success, error = await self.request("GET", uri)
        return success, error

    async def get_mark_price(self, inst_type, uly="", symbol=""):
        """ 获取标记价格，根据现货指数和合理基差设定标记价格
        Attributes：
            :param inst_type: 产品类型
                              MARGIN：币币杠杆
                              SWAP：永续合约
                              FUTURES：交割合约
                              OPTION：期权
            :param uly: 合约标的指数
            :param symbol: 交易对ID，如果不填写则返回左右币对数据信息
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/public/mark-price"
        params = {}
        if inst_type in INST_TYPE:
            params["instType"] = inst_type
        else:
            logger.error("账户类型错误，请重新输入")
        if uly:
            params["uly"] = uly
        if symbol:
            params["instId"] = symbol
        success, error = await self.request("GET", uri, params=params)
        return success, error

    # -------------------------------------------------- Market Data --------------------------------------------------

    async def get_all_ticker(self, inst_type, uly=""):
        """ 获取所有品种的ticker信息
        Attributes：
            :param inst_type: 合约类型
                              MARGIN：币币杠杆
                              SWAP：永续合约
                              FUTURES：交割合约
                              OPTION：期权
            :param uly: 合约标的指数
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/market/tickers"
        params = {}
        if inst_type in INST_TYPE:
            params["instType"] = inst_type
        else:
            logger.error("账户类型错误，请重新输入")
        if uly:
            params["uly"] = uly

        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_ticker(self, symbol):
        """ 获取单个品种的ticker信息
        Attributes：
            :param symbol: 交易币对ID
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/market/ticker"
        params = {"instId": symbol}

        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_index_ticker(self, quote_ccy, symbol):
        """ 获取指数行情数据
        Attributes:
            :param quote_ccy: 指数计价单位， 目前只有 USD/USDT/BTC为计价单位的指数，quoteCcy和instId必须填写一个
            :param symbol: 指数，如 BTC-USD
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/market/index-tickers"
        params = {}
        if quote_ccy:
            params["quoteCcy"] = quote_ccy
        if symbol:
            params["instId"] = symbol

        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_orderbook(self, symbol, limit="400"):
        """ 获取产品深度数据

        Attributes:
            :param symbol: 交易币对
            :param limit:  深度限制，最大400

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/market/books"
        if limit > "400":
            limit = "400"
        params = {
            "instId": symbol,
            "sz": limit
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_klines(self, symbol, after="", before="", bar="", limit="100"):
        """ 获取K线数据信息

        Attributes:
            :param symbol: 交易币对ID
            :param after: 请求此时间戳之前（更旧的数据）的分页内容，传的值为对应接口的ts
            :param before: 请求此时间戳之后（更新的数据）的分页内容，传的值为对应接口的ts
            :param bar: 时间颗粒 [1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y]
            :param limit: 分页返回的结果集数量，最大为100，不填默认返回100条

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/market/candles"
        params = {"instId": symbol}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        if bar:
            if bar not in K_DATE:
                return
            else:
                params["to"] = bar
        if limit:
            if limit > "100":
                limit = "100"
            params["limit"] = limit
        success, error = await self.request("GET", uri, params=params)
        return success, error

    # -------------------------------------------------- Status Data --------------------------------------------------

    async def get_system_status(self):
        """获取系统升级事件的状态"""
        uri = "/api/v5/system/status"
        success, error = await self.request("GET", uri)
        return success, error

    # -------------------------------------------------- Account Data --------------------------------------------------

    async def get_asset_info(self, ccy=""):
        """ 获取账户中资金余额信息
        Attributes:
            :param ccy: 币种，如果币种参数为None，则返回所有资产
        :returns:
            Success results, otherwise it's None.
            Error information, otherwise it's None.
        """
        uri = "/api/v5/account/balance"
        params = {}
        if ccy != "":
            params["ccy"] = ccy
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def get_position(self, inst_type="", symbol="", pos_id=""):
        """ 获取该账户下拥有实际持仓的信息。
            账户为单向持仓模式会显示净持仓（net），账户为双向持仓模式下会分别返回多头（long）或空头（short）的仓位。

        Attributes:
            :param inst_type: 账户类型，
                              MARGIN：币币杠杆
                              SWAP：永续合约
                              FUTURES：交割合约
                              OPTION：期权
                              instType和instId同时传入的时候会校验instId与instType是否一致，结果返回instId的持仓信息
            :param symbol: 交易币对信息, 如果不传入symbol则返回对应账户类型上所有交易对的持仓信息
            :param pos_id: 持仓ID，支持多个posId查询（不超过20个），半角逗号分割

        :returns:
            success: Success results, otherwise it's None.
            error:   Error information, otherwise it's None.
        """
        uri = "/api/v5/account/positions"
        params = {}
        if inst_type != "" and inst_type in INST_TYPE:
            params["instType"] = inst_type
        if symbol:
            params["instId"] = symbol
        if pos_id:
            params["posId"] = pos_id
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_account_position_risk(self, inst_type=""):
        """ 获取账户整体风险(获取同一时间切片上的账户和持仓的基础信息)
        Attributes:
            :param inst_type: 账户类型
                              MARGIN：币币杠杆
                              SWAP：永续合约
                              FUTURES：交割合约
                              OPTION：期权
        :returns:
            success: Success results, otherwise it's None.
            error:   Error information, otherwise it's None.
        """
        uri = "/api/v5/account/account-position-risk"
        if inst_type in INST_TYPE:
            params = {"instType": inst_type}
            success, error = await self.request("GET", uri, params=params, auth=True)
            return success, error
        else:
            success, error = await self.request("GET", uri, auth=True)
            return success, error

    async def set_position_mode(self, pos_mode):
        """ 设置持仓模式
        :param pos_mode: 持仓方式
                         long_short_mode：双向持仓
                         net_mode：单向持仓
                         仅适用交割/永续
        :return:
        """
        uri = "/api/v5/account/set-position-mode"
        body = {
            "posMode": pos_mode
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    # -------------------------------------------------- Trade Mode --------------------------------------------------

    async def create_order(self, symbol, td_mode, side, sz, ord_type="limit", ccy="", cl_ordId="", tag="", pos_side="",
                           px="", reduce_only="", tgt_ccy=""):
        """ Create an new order.

        Attributes:
            :param symbol: 交易币对ID
            :param td_mode: 交易模式
                            保证金模式：isolated：逐仓 ；cross：全仓
                            非保证金模式：cash：非保证金
                            交易模式，下单时需要指定:
                                简单交易模式：
                                    - 币币和期权买方：cash
                                单币种保证金模式：
                                    - 逐仓杠杆：isolated
                                    - 全仓杠杆：cross
                                    - 全仓币币：cash
                                    - 全仓交割/永续/期权：cross
                                    - 逐仓交割/永续/期权：isolated
                                跨币种保证金模式：
                                    - 逐仓杠杆：isolated
                                    - 全仓币币：cross
                                    - 全仓交割/永续/期权：cross
                                    - 逐仓交割/永续/期权：isolated
            :param ccy: 保证金币种，仅适用于单币种保证金模式下的全仓杠杆订单
            :param cl_ordId: 客户自定义订单ID
            :param tag: 订单标签
            :param side: 订单方向 buy：买 sell：卖
            :param pos_side: 持仓方向 在双向持仓模式下必填，且仅可选择 long 或 short
                             单向持仓模式下此参数非必填，如果填写仅可以选择net；
                             在双向持仓模式下必填，且仅可选择 long 或 short。
                                双向持仓模式下，side和posSide需要进行组合
                                    开多：买入开多（side 填写 buy； posSide 填写 long ）
                                    开空：卖出开空（side 填写 sell； posSide 填写 short ）
                                    平多：卖出平多（side 填写 sell；posSide 填写 long ）
                                    平空：买入平空（side 填写 buy； posSide 填写 short ）
            :param ord_type:订单类型
                            market：市价单, 币币和币币杠杆，是市价委托吃单；交割合约和永续合约，
                                    是自动以`最高买/最低卖`价格委托，遵循限价机制；`期权合约`不支持市价委托
                            limit：限价单, 限价单，要求指定sz 和 px
                            post_only：只做maker单
                            fok：全部成交或立即取消
                            ioc：立即成交并取消剩余
                            optimal_limit_ioc：市价委托立即成交并取消剩余（仅适用交割、永续）
            :param sz: 委托数量
                            当币币/币币杠杆以限价买入和卖出时，指交易货币数量。
                            当币币/币币杠杆以市价买入时，指计价货币的数量。
                            当币币/币币杠杆以市价卖出时，指交易货币的数量。
                            当交割、永续、期权买入和卖出时，指合约张数。
            :param px: 委托价格，仅适用于limit、post_only、fok、ioc类型的订单
            :param reduce_only: 是否只减仓，true 或 false，默认false
                                仅适用于币币杠杆订单
            :param tgt_ccy: 市价单委托数量的类型
                            base_ccy：交易货币 ；quote_ccy：计价货币
                            仅适用于币币订单

        :returns :
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/order"
        if td_mode not in ["isolated", "cross", "cash"]:
            logger.error("交易模式错误")
            return
        if side not in [ORDER_ACTION_BUY.lower(), ORDER_ACTION_SELL.lower()]:
            logger.error("订单方向错误")
            return
        if ord_type not in ORDER_TYPE:
            logger.error("订单类型错误")
            return
        body = {
            "instId": symbol,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": sz
        }
        if ccy != "":
            body["ccy"] = ccy
        if cl_ordId != "":
            body["clOrdId"] = cl_ordId
        if tag != "":
            body["tag"] = tag
        if pos_side != "":
            body["posSide"] = pos_side
        if px != "":
            body["px"] = px
        if reduce_only != "":
            body["reduceOnly"] = reduce_only
        if tgt_ccy != "":
            body["tgtCcy"] = tgt_ccy

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_orders(self, orders_data):
        """ 批量下单
            :param orders_data:
                 [
                    {
                        "instId":"BTC-USDT",
                        "tdMode":"cash",
                        "clOrdId":"b15",
                        "side":"buy",
                        "ordType":"limit",
                        "px":"2.15",
                        "sz":"2"
                    },
                    {
                        "instId":"BTC-USDT",
                        "tdMode":"cash",
                        "clOrdId":"b15",
                        "side":"buy",
                        "ordType":"limit",
                        "px":"2.15",
                        "sz":"2"
                    }
                ]
            :returns :
                :return success: Success results, otherwise it's None.
                :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/batch-orders"
        body = orders_data
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, symbol, order_id="", client_order_id=""):
        """ 撤单，单一订单撤销

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param order_id: Order ID.
            :param client_order_id: Custom Order ID.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/cancel-order"
        body = {
            "instId": symbol
        }
        if order_id:
            body["ordId"] = order_id
        if client_order_id:
            body["clOrdId"] = client_order_id

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_orders(self, order_list):
        """ 撤销未完成的订单，每次最多可以撤销20个订单。请求参数应该按数组格式传递。

        Attributes:
            :param order_list:list中需要包含以下参数类型
                symbol: Currency name, e.g. BTC.
                order_ids: Order ID list.
                client_order_ids: Client Order Ids.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/cancel-batch-orders"
        if len(order_list) > 20:
            order_list = order_list[:20]

        success, error = await self.request("POST", uri, body=order_list, auth=True)
        return success, error

    async def change_order(self, symbol, new_sz="", new_px="", cxl_on_fail="", ord_id="", cl_ord_id="", req_id=""):
        """ 修改当前未成交的挂单
        Attributes:
            :param symbol: 交易对信息
            :param cxl_on_fail: false：不自动撤单 true：自动撤单 当订单修改失败时，该订单是否需要自动撤销。默认为false
            :param ord_id: 订单ID， ordId和clOrdId必须传一个，若传两个，以ordId为主
            :param cl_ord_id: 用户自定义order ID
            :param req_id: 用户自定义修改事件ID 字母（区分大小写）与数字的组合，可以是纯字母、纯数字且长度要在1-32位之间。
            :param new_sz: 修改的新数量，newSz和newPx不可同时为空。对于部分成交订单，该数量应包含已成交数量。
            :param new_px: 修改的新价格
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.

        Note: 修改的数量<=该笔订单已成交数量时，该订单的状态会修改为完全成交状态。
        """
        uri = "/api/v5/trade/amend-order"
        if new_px == "" and new_sz == "":
            return
        if cl_ord_id == "" and ord_id == "":
            return
        body = {"instId": symbol}
        if new_sz != "":
            body["newSz"] = new_sz
        if new_px != "":
            body["newPx"] = new_px
        if cxl_on_fail != "":
            body["cxlOnFail"] = cxl_on_fail
        if ord_id != "":
            body["ordId"] = ord_id
        if cl_ord_id != "":
            body["clOrdId"] = cl_ord_id
        if req_id != "":
            body["reqId"] = req_id

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def change_orders(self, order_details):
        """ 修改未完成的订单，一次最多可批量修改20个订单。请求参数应该按数组格式传递。
        Attributes:
            :param order_details:
                instId	String	是	产品ID
                cxlOnFail	Boolean	否	false ：不自动撤单 true：自动撤单 当订单修改失败时，该订单是否需要自动撤销，默认为false
                ordId	String	可选	订单ID， ordId和clOrdId必须传一个，若传两个，以ordId为主
                clOrdId	String	可选	用户自定义order ID
                reqId	String	否	用户自定义修改事件ID 字母（区分大小写）与数字的组合，可以是纯字母、纯数字且长度要在1-32位之间。
                newSz	String	可选	修改的新数量，newSz和newPx不可同时为空。对于部分成交订单，该数量应包含已成交数量。
                newPx	String	可选	修改的新价格
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/amend-batch-orders"
        if len(order_details) > 20:
            order_details = order_details[:20]
        success, error = await self.request("POST", uri, body=order_details, auth=True)
        return success, error

    async def close_all_order(self, symbol, mgn_mode, pos_side="", ccy=""):
        """ 市价平掉某个合约下的全部持仓
        Attributes:
            :param symbol: 交易币对信息
            :param pos_side: 持仓方向
                             单向持仓模式下：可不填写此参数，默认值net，如果填写，仅可以填写net
                             双向持仓模式下： 必须填写此参数，且仅可以填写 long：平多 ，short：平空
            :param mgn_mode: 保证金模式 全仓：cross ； 逐仓： isolated
            :param ccy: 保证金币种 单币种保证金模式的全仓币币杠杆平仓必填
        :returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/close-position"
        body = {
            "instId": symbol,
            "mgnMode": mgn_mode
        }
        if pos_side != "":
            body["posSide"]: pos_side
        if ccy != "":
            body["ccy"] = ccy
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_info(self, symbol, order_ids="", client_order_ids=""):
        """ 获取订单信息

        Attributes:
            :param symbol: such as "BTC".
            :param order_ids: Order ID list.
                                (different IDs are separated by ",", maximum 20 orders can be requested at one time.)
            :param client_order_ids: Client Order ID list.
                                (different IDs are separated by ",", maximum 20 orders can be requested at one time.)

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        if order_ids == "" and client_order_ids == "":
            logger.error("订单ID和用户订单ID不可同时为空")
            return

        uri = "/api/v5/trade/order"
        body = {"instId": symbol}
        if order_ids:
            body["ordId"] = order_ids
        if client_order_ids:
            body["clOrdId"] = client_order_ids

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, inst_type="", symbol="", uly="", ord_type="", state="", after="", before="",
                              limit="100"):
        """ 获取未成交订单列表

        Attributes:
            :param inst_type: 产品类型
                                SPOT：币币
                                MARGIN：币币杠杆
                                SWAP：永续合约
                                FUTURES：交割合约
                                OPTION：期权
            :param symbol: 交易币对信息
            :param uly: 合约标的指数
            :param ord_type: 订单类型
                                market：市价单
                                limit：限价单
                                post_only：只做maker单
                                fok：全部成交或立即取消
                                ioc：立即成交并取消剩余
                                optimal_limit_ioc：市价委托立即成交并取消剩余（仅适用交割、永续）
            :param state: 订单状态
                            live：等待成交
                            partially_filled：部分成交
            :param after: 请求此ID之前（更旧的数据）的分页内容，传的值为对应接口的ordId
            :param before: 请求此ID之后（更新的数据）的分页内容，传的值为对应接口的ordId
            :param limit: 返回结果的数量，默认100条

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        Note：inst_type， symbol， ord_type三个参数都可以独立使用，分别返回对应设置的挂单信息

        """
        uri = "/api/v5/trade/orders-pending"
        params = {}
        if inst_type != "" and inst_type in INST_TYPE:
            params["instType"] = inst_type
        if uly != "":
            params["uly"] = uly
        if symbol != "":
            params["instId"] = symbol
        if ord_type != "":
            params["ordType"] = ord_type
        if state != "":
            params["state"] = state
        if after != "":
            params["after"] = after
        if before != "":
            params["before"] = before
        if float(limit) > 100:
            params["limit"] = "100"

        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def create_algo_order(self, symbol, td_mode, side, sz, ord_type="", pos_side="", tgt_ccy="",
                                reduce_only="", ccy="", **kwargs):
        """提供单向止盈止损委托 、双向止盈止损委托、计划委托、冰山委托、时间加权委托

        Attributes:
            :param symbol: 交易币对
            :param td_mode: 交易模式
                            保证金模式：isolated：逐仓 ；cross：全仓
                            非保证金模式：cash：非保证金
            :param ccy: 保证金币种，仅适用于单币种保证金模式下的全仓杠杆订单
            :param side:订单方向 buy：买 sell：卖
            :param pos_side:持仓方向 在双向持仓模式下必填，且仅可选择 long 或 short
            :param ord_type:订单类型
                            conditional：单向止盈止损
                            oco：双向止盈止损
                            trigger：计划委托
                            iceberg：冰山委托
                            twap：时间加权委托
            :param sz:委托数量
            :param tgt_ccy:委托数量的类型 base_ccy：交易货币 ；quote_ccy：计价货币
            :param reduce_only:是否只减仓 true 或 false

            止盈止损
                参数名	     类型	  是否必须	描述
                tpTriggerPx	 String	  否	   止盈触发价，如果填写此参数，必须填写 止盈委托价
                tpOrdPx	     String	  否	   止盈委托价，如果填写此参数，必须填写 止盈触发价 委托价格为-1时，执行市价止盈
                slTriggerPx	 String	  否	   止损触发价，如果填写此参数，必须填写 止损委托价
                slOrdPx	     String	  否	   止损委托价，如果填写此参数，必须填写 止损触发价 委托价格为-1时，执行市价止损

            计划委托
                参数名	    类型	      是否必须	描述
                triggerPx	String	  否	    计划委托触发价格
                orderPx	    String	  否	    委托价格 委托价格为-1时，执行市价委托

            冰山委托
                参数名	     类型	是否必须	描述
                pxVar	     String	可选	    挂单价距离盘口的比例 pxVar和pxSpread只能传入一个
                pxSpread	 String	可选	    挂单价距离盘口的价距
                szLimit	     String	是	    单笔数量
                pxLimit	     String	是	    挂单限制价

            时间加权
                参数名	       类型	     是否必须	  描述
                pxVar	        String	 可选	  吃单价优于盘口的比例 pxVar和pxSpread只能传入一个
                pxSpread	    String	 可选	  吃单单价优于盘口的价距
                szLimit	        String	 是	      单笔数量
                pxLimit	        String	 是	      挂单限制价
                timeInterval	String	 是	      下单间隔

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        """
        uri = "/api/v5/trade/order-algo"
        body = {
            "instId": symbol,
            "tdMode": td_mode,
            "side": side,
            "sz": sz
        }
        if pos_side in ["long", "short"]:
            body["posSide"] = pos_side
        else:
            logger.error("持仓方向错误，只能为long 或者是 short")

        if tgt_ccy != "":
            body["tgtCcy"] = tgt_ccy
        if isinstance(reduce_only, bool):
            body["reduceOnly"] = reduce_only
        if ccy != "":
            body["ccy"] = ccy
        # 当用户进行单向止盈止损委托（ordType=conditional）时，如果用户同时传了止盈止损四个参数，只进行止损的功能校验，忽略止盈的业务逻辑校验。
        if ord_type in ["conditional", "oco"]:  # 止损止盈设置
            body["ordType"] = ord_type

            tp_trigger_px = kwargs.get("tp_trigger_px")
            tp_ord_px = kwargs.get("tp_ord_px")
            sl_trigger_px = kwargs.get("sl_trigger_px")
            sl_ord_px = kwargs.get("sl_ord_px")

            if tp_trigger_px and tp_ord_px:
                body["tpTriggerPx"] = tp_trigger_px
                body["tpOrdPx"] = tp_ord_px
            else:
                logger.error("止盈参数设置有误")
                return
            if sl_trigger_px and sl_ord_px:
                body["slTriggerPx"] = sl_trigger_px
                body["slOrdPx"] = sl_ord_px
            else:
                logger.error("止损参数设置有误")

        elif ord_type == "trigger":
            body["ordType"] = ord_type
            trigger_px = kwargs.get("trigger_px")
            order_px = kwargs.get("order_px")
            if trigger_px and order_px:
                body["orderPx"] = order_px
                body["triggerPx"] = trigger_px
            else:
                logger.error("计划委托价格和委托触发价格参数错误")

        elif ord_type == "iceberg":
            body["ordType"] = ord_type
            px_var = kwargs.get("px_var")
            px_spread = kwargs.get("px_spread")
            sz_limit = kwargs.get("sz_limit")
            px_limit = kwargs.get("px_limit")
            if px_limit and sz_limit:
                body["pxLimit"] = px_limit
                body["szLimit"] = sz_limit
                if px_var:
                    body["pxVar"] = px_var
                if px_spread:
                    body["pxSpread"] = px_spread
            else:
                logger.error("冰山委托单笔数量和挂单限制价格有误")
        elif ord_type == "twap":
            body["ordType"] = ord_type

            px_var = kwargs.get("px_var")
            px_spread = kwargs.get("px_spread")
            sz_limit = kwargs.get("sz_limit")
            px_limit = kwargs.get("px_limit")
            time_interval = kwargs.get("time_interval")

            if px_limit and sz_limit and time_interval:
                body["pxLimit"] = px_limit
                body["szLimit"] = sz_limit
                body["timeInterval"] = time_interval
                if px_var:
                    body["pxVar"] = px_var
                if px_spread:
                    body["pxSpread"] = px_spread
            else:
                logger.error("冰山委托单笔数量和挂单限制价格有误")

        else:
            logger.error("订单类型错误")
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_algo_order(self, symbol, order_id):
        """ 撤销策略委托订单

        Attributes:
            :param symbol: symbol,such as "BTC".
            :param order_id: order ids.multiple orders need to be joined by ','.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        """
        uri = "/api/v5/trade/cancel-algos"
        body = {
            "instId": symbol,
            "algoId": order_id
        }

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Attributes:
            :param method: HTTP request method. `GET` / `POST` / `DELETE` / `PUT`.
            :param uri: HTTP request uri.
            :param params: HTTP query params.
            :param body: HTTP request body.
            :param headers: HTTP request headers.
            :param auth: If this request requires authentication.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)

        if auth:
            timestamp = datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(
                bytes(self._secret_key, encoding='utf8'),
                bytes(message, encoding='utf8'),
                digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)
            if not headers:
                headers = {}

            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
            headers['x-simulated-trading'] = "0"

        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        return success, error

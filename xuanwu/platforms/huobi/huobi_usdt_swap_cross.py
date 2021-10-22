# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/16 13:39
  @ Description: HuoBi USDT合约全仓
  @ History:
"""
import gzip
import copy
import hmac
import base64
import urllib
import hashlib
import datetime
import time
from urllib.parse import urljoin
from xuanwu.const import USER_AGENT
from urllib.parse import urljoin
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask
from xuanwu.const import HUOBI_USDT_SWAP_CROSS
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.utils.decorator import async_method_locker
from xuanwu.model.order import *

__all__ = ("HuobiUsdtSwapCrossTrade",)


class HuobiUsdtSwapCrossRestAPI:
    """ Huobi USDT Swap REST API Client(Cross Margined Mode).

    Attributes:
        :param host: HTTP request host.
        :param access_key: Account's ACCESS KEY.
        :param secret_key: Account's SECRET KEY.
    """
    def __init__(self, host, access_key, secret_key):
        """ initialize REST API client. """
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_swap_info(self, contract_code=None):
        """ Get Swap Info

        Attributes:
            :param contract_code:  such as "BTC-USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        * Note: 1. If input `contract_code`, only matching this contract code.
                2. If not input 'contract_code', matching all contract_codes.
        """
        uri = "/linear-swap-api/v1/swap_contract_info"
        params = {}
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_price_limit(self, contract_code=None):
        """ Get swap price limit.

        Attributes:
            :param contract_code:  such as "BTC-USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input 'contract_code', matching all contract_codes.
        """
        uri = "/linear-swap-api/v1/swap_price_limit"
        params = {}
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_orderbook(self, contract_code):
        """ Get orderbook information.

        Attributes:
            :param contract_code:  such as "BTC-USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-ex/market/depth"
        params = {
            "contract_code": contract_code,
            "type": "step0"
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_klines(self, contract_code, period, size=None, sfrom=None, to=None):
        """ Get kline information.

        Attributes:
            :param sfrom:
            :param to:
            :param contract_code:  such as "BTC-USDT".
            :param period: 1min, 5min, 15min, 30min, 60min,4hour,1day, 1mon
            :param size: [1,2000]

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-ex/market/history/kline"
        params = {
            "contract_code": contract_code,
            "period": period
        }
        if size:
            params["size"] = size
        if sfrom:
            params["from"] = sfrom
        if to:
            params["to"] = to
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_merged_data(self, contract_code):
        """ Get Merged Data.

        Attributes:
            :param contract_code: such as "BTC-USDT"

        :returns:
            :return success: Success results.
            :return error: Error information.
        """
        uri = "/linear-swap-ex/market/detail/merged"
        params = {
            "contract_code": contract_code
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_funding_rate(self, contract_code):
        """ Get Funding Rate.

        Attributes:
            :param contract_code: such as "BTC-USDT"

        :returns:
            :return success: Success results.
            :return error: Error information.
        """
        uri = "/linear-swap-ex/v1/swap_funding_rate"
        params = {
            "contract_code": contract_code
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_asset_info(self, margin_account=None):
        """ Get account asset information.

        Attributes:
            :param margin_account: such as "USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_account_info"
        body = {}
        if margin_account:
            body["margin_account"] = margin_account
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_position(self, contract_code=None):
        """ Get position information.

        Attributes:
            :param contract_code: such as "BTC-USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_position_info"
        body = {}
        if contract_code:
            body["contract_code"] = contract_code
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_account_position(self, margin_account):
        """ Get position and account information.

        Attributes:
            :param margin_account: Currency name, e.g. USDT.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_account_position_info"
        body = {"margin_account": margin_account}
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_order(self, contract_code, price, quantity, direction, offset, lever_rate, order_price_type,
                           client_order_id=None):
        """ Create an new order.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param price: Order price.
            :param quantity: Order amount.
            :param direction: Transaction direction, `buy` / `sell`.
            :param offset: `open` / `close`.
            :param lever_rate: Leverage rate, 10 or 20.
            :param order_price_type: Order type, `limit` - limit order, `opponent` - market order.
            :param client_order_id:

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_order"
        body = {
            "contract_code": contract_code,
            "price": price,
            "volume": quantity,
            "direction": direction,
            "offset": offset,
            "lever_rate": lever_rate,
            "order_price_type": order_price_type
        }
        if client_order_id:
            body.update({"client_order_id": client_order_id})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_orders(self, orders_data):
        """ Batch Create orders.
            orders_data = {'orders_data': [
               {
                'contract_code':'BTC-USDT',  'client_order_id':'',
                'price':1, 'volume':1, 'direction':'buy', 'offset':'open',
                'lever_rate':20, 'order_price_type':'limit'},
               {
                'contract_code':'BTC-USDT', 'client_order_id':'',
                'price':2, 'volume':2, 'direction':'buy', 'offset':'open',
                'lever_rate':20, 'order_price_type':'limit'}]}
        """
        uri = "/linear-swap-api/v1/swap_cross_batchorder"
        body = orders_data
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, contract_code, order_id=None, client_order_id=None):
        """ Revoke an order.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param order_id: Order ID.
            :param client_order_id:

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_cancel"
        body = {
            "contract_code": contract_code
        }
        if order_id:
            body["order_id"] = order_id
        if client_order_id:
            body["client_order_id"] = client_order_id

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_orders(self, contract_code, order_ids=None, client_order_ids=None):
        """ Revoke multiple orders.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param order_ids: Order ID list.
            :param client_order_ids:

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_cancel"
        body = {
            "contract_code": contract_code
        }
        if order_ids:
            body["order_id"] = ",".join(order_ids)
        if client_order_ids:
            body["client_order_id"] = ",".join(client_order_ids)

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order_all(self, contract_code):
        """ Revoke all orders.

        Attributes:
            :param contract_code: such as "BTC-USDT".

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/linear-swap-api/v1/swap_cross_cancelall"
        body = {
            "contract_code": contract_code,
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_info(self, contract_code, order_ids=None, client_order_ids=None):
        """ Get order information.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param order_ids: Order ID list.
                                (different IDs are separated by ",", maximum 20 orders can be requested at one time.)
            :param client_order_ids: Client Order ID list.
                                (different IDs are separated by ",", maximum 20 orders can be requested at one time.)

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_order_info"
        body = {
            "contract_code": contract_code
        }

        if order_ids:
            body.update({"order_id": ",".join(order_ids)})
        if client_order_ids:
            body.update({"client_order_id": ",".join(client_order_ids)})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_detail(self, contract_code, order_id, created_at=None, order_type=None,
                               page_index=1, page_size=20):
        """ Get Order Detail

        Attributes:
            :param contract_code: such as "BTC-USDT"
            :param order_id: order id.
            :param created_at: create timestamp.
            :param order_type: order type, 1. Quotation; 2. Cancelled order; 3. Forced liquidation; 4. Delivery Order
            :param page_index: page idnex. 1 default.
            :param page_size: page size. 20 default. 50 max.
        Note:
            When getting information on order cancellation via query order detail interface,
            users who type in parameters “created_at” and “order_type” can query last 24-hour data,
            while users who don’t type in parameters “created_at” and “order_type” can only query last 12-hour data.
            created_at should use timestamp of long type as 13 bits (include Millisecond),
            if send the accurate timestamp for "created_at", query performance will be improved.
            eg. the timestamp "2019/10/18 10:26:22" can be changed：1571365582123.It can also directly
            obtain the timestamp（ts) from the returned ordering interface(swap_order) to query the corresponding
            orders.
        """
        uri = "/linear-swap-api/v1/swap_cross_order_detail"
        body = {
            "contract_code": contract_code,
            "order_id": order_id,
            "page_index": page_index,
            "page_size": page_size
        }
        if created_at:
            body.update({"created_at": created_at})
        if order_type:
            body.update({"order_type": order_type})
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, contract_code, index=1, size=50):
        """ Get open order information.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param index: Page index, default 1st page.
            :param size: Page size, Default 20，no more than 50.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/linear-swap-api/v1/swap_cross_openorders"
        body = {
            "contract_code": contract_code,
            "page_index": index,
            "page_size": size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_history_orders(self, contract_code, trade_type, stype, status, create_date,
                                 page_index=0, page_size=50):
        """ Get history orders information.

        Attributes:
            :param contract_code: such as "BTC-USDT".
            :param trade_type: 0:all,
                               1: buy long,
                               2: sell short,
                               3: buy short,
                               4: sell long,
                               5: sell liquidation,
                               6: buy liquidation,
                               7:Delivery long,
                               8: Delivery short
            :param stype: 1:All Orders,2:Order in Finished Status
            :param status: status: 1. Ready to submit the orders;
                                   2. Ready to submit the orders;
                                   3. Have sumbmitted the orders;
                                   4. Orders partially matched;
                                   5. Orders cancelled with partially matched;
                                   6. Orders fully matched;
                                   7. Orders cancelled;
                                   11. Orders cancelling.
            :param create_date: any positive integer available. Requesting data beyond 90 will not be supported,
                        otherwise, system will return trigger history data within the last 90 days by default.
            :param page_index: default 1st page
            :param page_size: default page size 20. 50 max.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        """
        uri = "/linear-swap-api/v1/swap_cross_hisorders"
        body = {
            "contract_code": contract_code,
            "trade_type": trade_type,
            "type": stype,
            "status": status,
            "create_date": create_date,
            "page_index": page_index,
            "page_size": page_size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def transfer_inner(self, asset, from_, to, amount):
        """ Do transfer under the same account
        Attributes:
            :param asset: such as USDT
            :param from_: from_margin_account.such as BTC-USDT
            :param to: to_margin_account.such as BTC-USDT
            :param amount: transfer amount.
        """
        uri = "/linear-swap-api/v1/swap_transfer_inner"
        body = {
            "asset": asset,
            "from_margin_account": from_,
            "to_margin_account": to,
            "amount": amount
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def transfer_between_spot_swap(self, margin_account, amount, from_, to,  currency="USDT"):
        """ Do transfer between spot and future.
        Attributes:
            :param amount: transfer amount.pls note the precision digit is 8.
            :param from_: 'spot' or 'linear-swap'
            :param to: 'spot' or 'linear-swap'
            :param currency: "usdt",
            :param margin_account: "BTC-USDT"
        """
        body = {
            "from": from_,
            "to": to,
            "amount": amount,
            "margin-account": margin_account,
            "currency": currency,
        }

        uri = "https://api.huobi.pro/v2/account/transfer"
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
        if uri.startswith("http://") or uri.startswith("https://"):
            url = uri
        else:
            url = urljoin(self._host, uri)

        if auth:
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            params = params if params else {}
            params.update({"AccessKeyId": self._access_key,
                           "SignatureMethod": "HmacSHA256",
                           "SignatureVersion": "2",
                           "Timestamp": timestamp})

            params["Signature"] = self.generate_signature(method, params, uri)

        if not headers:
            headers = {}
        if method == "GET":
            headers["Content-type"] = "application/x-www-form-urlencoded"
            headers["User-Agent"] = USER_AGENT
            _, success, error = await AsyncHttpRequests.fetch("GET", url, params=params, headers=headers, timeout=10)
        else:
            headers["Accept"] = "application/json"
            headers["Content-type"] = "application/json"
            headers["User-Agent"] = USER_AGENT
            _, success, error = await AsyncHttpRequests.fetch("POST", url, params=params, data=body, headers=headers,
                                                              timeout=10)
        if error:
            return None, error
        if not isinstance(success, dict):
            result = json.loads(success)
        else:
            result = success
        if result.get("status") != "ok":
            return None, result
        return result, None

    def generate_signature(self, method, params, request_path):
        if request_path.startswith("http://") or request_path.startswith("https://"):
            host_url = urllib.parse.urlparse(request_path).hostname.lower()
            request_path = '/' + '/'.join(request_path.split('/')[3:])
        else:
            host_url = urllib.parse.urlparse(self._host).hostname.lower()
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = self._secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature


class HuobiUsdtSwapCrossTrade(Websocket):
    """ Huobi Swap Trade module(Cross Margined Mode). You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. default `https://api.hbdm.com"`.
        wss: Websocket address. default `wss://api.hbdm.com`.
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
        asset_update_callback:
        order_update_callback:
        position_update_callback:
        init_success_callback:
    """
    def __init__(self, **kwargs):
        """Initialize."""
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
            kwargs["host"] = "https://api.hbdm.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://api.hbdm.com"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return
        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = HUOBI_USDT_SWAP_CROSS
        self._symbol = kwargs["symbol"]
        self._contract_type = kwargs["contract_type"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        url = self._wss + "/linear-swap-notification"
        super(HuobiUsdtSwapCrossTrade, self).__init__(url, send_hb_interval=5)

        self._assets = {}  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(self._platform, self._account, self._strategy, self._symbol+'/'+self._contract_type)

        self._order_channel = "orders_cross.{symbol}".format(symbol=self._symbol)
        self._position_channel = "positions_cross.{symbol}".format(symbol=self._symbol)
        self._asset_channel = "accounts_cross.{symbol}".format(symbol="USDT")

        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        self._rest_api = HuobiUsdtSwapCrossRestAPI(self._host, self._access_key, self._secret_key)

        self.initialize()

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def position(self):
        return copy.copy(self._position)

    @property
    def rest_api(self):
        return self._rest_api

    async def _send_heartbeat_msg(self, *args, **kwargs):
        data = {"op": "pong", "ts": str(int(time.time()*1000))}
        if not self.ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self.ws.send_json(data)

    async def connected_callback(self):
        """After connect to Websocket server successfully, send a auth message to server."""
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        data = {
            "AccessKeyId": self._access_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        }
        sign = self._rest_api.generate_signature("GET", data, "/linear-swap-notification")
        data["op"] = "auth"
        data["type"] = "api"
        data["Signature"] = sign
        await self.ws.send_json(data)

    async def auth_callback(self, data):
        if data["err-code"] != 0:
            e = Error("Websocket connection authorized failed: {}".format(data))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        # subscribe order
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._order_channel
        }
        await self.ws.send_json(data)

        # subscribe position
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._position_channel
        }
        await self.ws.send_json(data)

        # subscribe asset
        data = {
            "op": "sub",
            "cid": tools.get_uuid1(),
            "topic": self._asset_channel
        }
        await self.ws.send_json(data)

    async def sub_callback(self, data):
        if data["err-code"] != 0:
            e = Error("subscribe {} failed!".format(data["topic"]))
            logger.error(e, caller=self)
            SingleTask.run(self._init_success_callback, False, e)
            return
        if data["topic"] == self._order_channel:
            self._subscribe_order_ok = True
        elif data["topic"] == self._position_channel:
            self._subscribe_position_ok = True
        elif data["topic"] == self._asset_channel:
            self._subscribe_asset_ok = True
        if self._subscribe_order_ok and self._subscribe_position_ok \
                and self._subscribe_asset_ok:
            success, error = await self._rest_api.get_open_orders(self._symbol)
            if error:
                e = Error("get open orders failed!")
                SingleTask.run(self._init_success_callback, False, e)
            elif "data" in success and "orders" in success["data"]:
                for order_info in success["data"]["orders"]:
                    order_info["ts"] = int(time.time() * 1000)
                    self._update_order(order_info)
                SingleTask.run(self._init_success_callback, True, None)
            else:
                logger.warn("get open orders:", success, caller=self)
                e = Error("Get Open Orders Unknown error")
                SingleTask.run(self._init_success_callback, False, e)

    async def process(self, msg):
        """只继承不实现"""
        pass

    @async_method_locker("HuobiSwapCrossTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        data = json.loads(gzip.decompress(raw).decode())
        logger.debug("data:", data, caller=self)

        op = data.get("op")
        if op == "ping":
            hb_msg = {"op": "pong", "ts": data.get("ts")}
            await self.ws.send_json(hb_msg)

        elif op == "auth":
            await self.auth_callback(data)

        elif op == "sub":
            await self.sub_callback(data)

        elif op == "notify":
            if data["topic"].startswith("orders"):
                self._update_order(data)
            elif data["topic"].startswith("positions"):
                self._update_position(data)
            elif data["topic"].startswith("accounts"):
                self._update_asset(data)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT,
                           client_order_id=None,  **kwargs):
        """ Create an order.

        Attributes:
            :param action: Trade direction, BUY or SELL.
            :param price: Price of each contract.
            :param quantity: The buying or selling quantity.
            :param order_type: Order type, LIMIT or MARKET.
            :param client_order_id:
            kwargs:
                lever_rate: Leverage rate, 10 or 20.

        :returns:
            :return order_no: Order ID if created successfully, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        if int(quantity) > 0:
            if action == ORDER_ACTION_BUY:
                direction = "buy"
                offset = "open"
            elif action == ORDER_ACTION_SELL:
                direction = "sell"
                offset = "close"
            else:
                return None, "action error"
        else:
            if action == ORDER_ACTION_BUY:
                direction = "buy"
                offset = "close"
            elif action == ORDER_ACTION_SELL:
                direction = "sell"
                offset = "open"
            else:
                return None, "action error"

        lever_rate = kwargs.get("lever_rate", 20)
        if order_type == ORDER_TYPE_LIMIT:
            order_price_type = "limit"
        elif order_type == ORDER_TYPE_MARKET:
            order_price_type = "optimal_20"
        elif order_type == ORDER_TYPE_MAKER:
            order_price_type = "post_only"
        elif order_type == ORDER_TYPE_FOK:
            order_price_type = "fok"
        elif order_type == ORDER_TYPE_IOC:
            order_price_type = "ioc"

        else:
            return None, "order type error"

        quantity = abs(int(quantity))
        result, error = await self._rest_api.create_order(self._symbol,
                                                          price, quantity, direction, offset, lever_rate,
                                                          order_price_type, client_order_id)
        if error:
            return None, error
        return str(result["data"]["order_id"]), None

    async def create_orders(self, orders, *args, **kwargs):
        """ batch create orders

        Args:
            :param orders: []
                list item:
                    action: Trade direction, BUY or SELL.
                    price: Price of each contract.
                    quantity: The buying or selling quantity.
                    order_type: Order type, LIMIT or MARKET.
                    lever_rate: leverage.
                kwargs:

        :returns:
            :return success: order info  if created successfully.
            :return error: erros information.
        """
        orders_data = []
        for order in orders:
            if int(order["quantity"]) > 0:
                if order["action"] == ORDER_ACTION_BUY:
                    direction = "buy"
                    offset = "open"
                elif order["action"] == ORDER_ACTION_SELL:
                    direction = "sell"
                    offset = "close"
                else:
                    return None, "action error"
            else:
                if order["action"] == ORDER_ACTION_BUY:
                    direction = "buy"
                    offset = "close"
                elif order["action"] == ORDER_ACTION_SELL:
                    direction = "sell"
                    offset = "open"
                else:
                    return None, "action error"

            lever_rate = order["lever_rate"]
            if order["order_type"] == ORDER_TYPE_LIMIT:
                order_price_type = "limit"
            elif order["order_type"] == ORDER_TYPE_MARKET:
                order_price_type = "optimal_20"
            elif order["order_type"] == ORDER_TYPE_MAKER:
                order_price_type = "post_only"
            elif order["order_type"] == ORDER_TYPE_FOK:
                order_price_type = "fok"
            elif order["order_type"] == ORDER_TYPE_IOC:
                order_price_type = "ioc"
            else:
                return None, "order type error"

            quantity = abs(int(order["quantity"]))

            client_order_id = order.get("client_order_id", "")

            orders_data.append({"contract_code": self._symbol,
                                "client_order_id": client_order_id,
                                "price": order["price"],
                                "volume": quantity,
                                "direction": direction,
                                "offset": offset,
                                "leverRate": lever_rate,
                                "orderPriceType":  order_price_type})

        result, error = await self._rest_api.create_orders({"orders_data": orders_data})
        if error:
            return None, error
        order_nos = [order["order_id"] for order in result.get("data").get("success")]
        return order_nos, result.get("data").get("errors")

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Attributes:
            :param order_nos: Order id list, you can set this param to 0 or multiple items.
                              If you set 0 param, you can cancel all orders for this
                              symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                              If you set multiple param, you can cancel multiple orders.
                              Do not set param length more than 100.

        Returns:
            Success or error, see bellow.
        """
        if len(order_nos) == 0:
            success, error = await self._rest_api.revoke_order_all(self._symbol)
            if error:
                return False, error
            if success.get("errors"):
                return False, success["errors"]
            return True, None

        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(self._symbol, order_nos[0])
            if error:
                return order_nos[0], error
            if success.get("errors"):
                return False, success["errors"]
            else:
                return order_nos[0], None

        if len(order_nos) > 1:
            success, error = await self._rest_api.revoke_orders(self._symbol, order_nos)
            if error:
                return order_nos[0], error
            if success.get("errors"):
                return False, success["errors"]
            return success, error

    async def get_open_order_nos(self):
        """ Get open order id list.

        Attributes:
            None.

        :returns:
            :return order_nos: Open order id list, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(self._symbol)
        if error:
            return None, error
        else:
            order_nos = []
            for order_info in success["data"]["orders"]:
                if order_info["contract_code"] != self._symbol:
                    continue
                order_nos.append(str(order_info["order_id"]))
            return order_nos, None

    def _update_order(self, order_info):
        """ Order update.

        Attributes:
            order_info: Order information.
        """
        if order_info["contract_code"] != self._symbol:
            return
        order_no = str(order_info["order_id"])
        status = order_info["status"]

        order = self._orders.get(order_no)
        if not order:
            if order_info["direction"] == "buy":
                if order_info["offset"] == "open":
                    trade_type = TRADE_TYPE_BUY_OPEN
                else:
                    trade_type = TRADE_TYPE_BUY_CLOSE
            else:
                if order_info["offset"] == "close":
                    trade_type = TRADE_TYPE_SELL_CLOSE
                else:
                    trade_type = TRADE_TYPE_SELL_OPEN

            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "client_order_id": order_info.get("client_order_id"),
                "order_price_type": order_info.get("order_price_type"),
                "order_type": order_info["order_type"],
                "action": ORDER_ACTION_BUY if order_info["direction"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol + '/' + self._contract_type,
                "price": order_info["price"],
                "quantity": order_info["volume"],
                "trade_type": trade_type
            }
            order = Order(**info)
            self._orders[order_no] = order

        order.trade_quantity = None
        order.trade_price = None
        if order_info.get("trade"):
            quantity = 0
            amount = 0
            for trade in order_info.get("trade"):
                order.role = trade.get("role")
                quantity += float(trade.get("trade_volume"))
                amount += float(trade.get("trade_volume")*trade.get("trade_price"))
            price = amount/quantity
            order.trade_quantity = int(quantity)
            order.trade_price = price

        if status in [1, 2, 3]:
            order.status = ORDER_STATUS_SUBMITTED
        elif status == 4:
            order.status = ORDER_STATUS_PARTIAL_FILLED
            order.remain = int(order.quantity) - int(order_info["trade_volume"])
        elif status == 6:
            order.status = ORDER_STATUS_FILLED
            order.remain = 0
        elif status in [5, 7]:
            order.status = ORDER_STATUS_CANCELED
            order.remain = int(order.quantity) - int(order_info["trade_volume"])
        else:
            return

        order.avg_price = order_info["trade_avg_price"]
        order.ctime = order_info["created_at"]
        order.utime = order_info["ts"]

        SingleTask.run(self._order_update_callback, copy.copy(order))

        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

    def _update_position(self, data):
        """ Position update.

        Attributes:
            :param data: Position information.

        :return:
            None.
        """
        for position_info in data["data"]:
            if position_info["contract_code"] != self._symbol:
                continue
            if position_info["direction"] == "buy":
                self._position.long_quantity = int(position_info["volume"])
                self._position.long_avg_price = position_info["cost_open"]
            else:
                self._position.short_quantity = int(position_info["volume"])
                self._position.short_avg_price = position_info["cost_open"]
            self._position.utime = data["ts"]
            SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def _update_asset(self, data):
        """ Asset update.

        Attributes:
            :param data: asset data.

        :returns:
            None.
        """
        assets = {}
        for item in data["data"]:
            symbol = item["margin_account"].upper()
            total = float(item["margin_balance"])
            locked = float(item["margin_frozen"])+float(item["margin_position"])
            free = total - locked
            if total > 0:
                assets[symbol] = {
                    "total": "%.8f" % total,
                    "free": "%.8f" % free,
                    "locked": "%.8f" % locked
                }
        if assets == self._assets:
            update = False
        else:
            update = True
        if hasattr(self._assets, "assets") is False:
            info = {
                "platform": self._platform,
                "account": self._account,
                "assets": assets,
                "timestamp": tools.get_cur_timestamp_ms(),
                "update": update
            }
            asset = Asset(**info)
            self._assets = asset
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))
        else:
            for symbol in assets:
                self._assets.assets.update({
                    symbol: assets[symbol]
                })
            self._assets.timestamp = tools.get_cur_timestamp_ms()
            SingleTask.run(self._asset_update_callback, copy.copy(self._assets))

# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/4/16 11:21
  @ Description: 
  @ History:
"""
import hmac
import base64
import urllib
import hashlib
import datetime
import gzip
import time
import copy
from urllib.parse import urljoin
from xuanwu.const import USER_AGENT
from urllib.parse import urljoin
from xuanwu.const import MARKET_TYPE_KLINE, HUOBI_FUTURE
from xuanwu.model.market import Orderbook, Kline, Trade
from xuanwu.model.order import *
from xuanwu.model.asset import Asset
from xuanwu.model.position import Position
from xuanwu.error import Error
from xuanwu.utils import logger
from xuanwu.tasks import SingleTask
from xuanwu.utils.websocket import Websocket
from xuanwu.utils.http_client import AsyncHttpRequests
from xuanwu.utils.decorator import async_method_locker

__all__ = ("HuobiFutureMarket", "HuobiFutureTrade",)


class HuobiFutureRestAPI:
    """ Huobi Swap REST API client.

    Attributes:
        :param host: HTTP request host.
        :param access_key: Account's ACCESS KEY.
        :param secret_key: Account's SECRET KEY.
    """

    def __init__(self, host, access_key, secret_key):
        """initialize REST API client."""
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

    async def get_contract_info(self, symbol=None, contract_type=None, contract_code=None):
        """ Get contract information.
        Attributes:
            :param symbol: Trade pair, default `None` will return all symbols.
            :param contract_type: Contract type,`this_week`/`next_week`/`quarter`, default `None` will return all types.
            :param contract_code: Contract code, e.g. BTC180914.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_contract_info"
        params = {}
        if symbol:
            params["symbol"] = symbol
        if contract_type:
            params["contract_type"] = contract_type
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params)
        return success, error

    async def get_price_limit(self, symbol=None, contract_type=None, contract_code=None):
        """ Get contract price limit.

        Attributes:
            :param symbol: Trade pair, default `None` will return all symbols.
            :param contract_type: Contract type,`this_week`/`next_week`/`quarter`, default `None` will return all types.
            :param contract_code: Contract code, e.g. BTC180914.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_price_limit"
        params = {}
        if symbol:
            params["symbol"] = symbol
        if contract_type:
            params["contract_type"] = contract_type
        if contract_code:
            params["contract_code"] = contract_code
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_orderbook(self, symbol):
        """ Get orderbook information.

        Attributes:
            :param symbol: Symbol name, `BTC_CW` - current week, `BTC_NW` next week, `BTC_CQ` current quarter.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/market/depth"
        params = {
            "symbol": symbol,
            "type": "step0"
        }
        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def get_klines(self, symbol, period, size=None, sfrom=None, to=None):
        """ Get kline information.

        Attributes:
            :param symbol: Symbol name, `BTC_CW` - current week, `BTC_NW` next week, `BTC_CQ` current quarter.
            :param period: 1min, 5min, 15min, 30min, 60min,4hour,1day, 1mon
            :param size: [1,2000]
            :param sfrom:
            :param to:

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/market/history/kline"
        params = {
            "symbol": symbol,
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

    async def get_asset_info(self):
        """ Get account asset information.

        :returns:
            :return Success results, otherwise it's None.
            :return Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_account_info"
        success, error = await self.request("POST", uri, auth=True)
        return success, error

    async def get_position(self, symbol=None):
        """ Get position information.

        Attributes:
            :param symbol: Currency name, e.g. BTC. default `None` will return all types.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error:   Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_position_info"
        body = {}
        if symbol:
            body["symbol"] = symbol
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_account_position(self, symbol=None):
        """ Get position and account information.

        Attributes:
            :param symbol: Currency name, e.g. BTC. default `None` will return all types.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_account_position_info"
        body = {}
        if symbol:
            body["symbol"] = symbol
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_order_info(self, symbol, order_ids=None, client_order_ids=None):
        """ Get order information.

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
        if client_order_ids is None:
            client_order_ids = []
        if order_ids is None:
            order_ids = []
        uri = "/api/v1/contract_order_info"
        body = {
            "symbol": symbol
        }

        if order_ids:
            body.update({"order_id": ",".join(order_ids)})
        if client_order_ids:
            body.update({"client_order_id": ",".join(client_order_ids)})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_order(self, symbol, contract_type, contract_code, price, quantity, direction, offset, lever_rate,
                           order_price_type, client_order_id=None):
        """ Create an new order.

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param contract_type: Contract type, `this_week` / `next_week` / `quarter`.
            :param contract_code: Contract code, e.g. BTC180914.
            :param price: Order price.
            :param quantity: Order amount.
            :param direction: Transaction direction, `buy` / `sell`.
            :param offset: `open` / `close`.
            :param lever_rate: Leverage rate, 10 or 20.
            :param order_price_type: Order type, `limit` - limit order, `opponent` - market order.
            :param client_order_id: client order id

        :returns :
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_order"
        body = {
            "symbol": symbol,
            "contract_type": contract_type,
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
            :param orders_data:
                {
                    'orders_data':
                        [
                           {'symbol': 'BTC', 'contract_type': 'quarter',
                            'contract_code':'BTC181228',  'client_order_id':'',
                            'price':1, 'volume':1, 'direction':'buy', 'offset':'open',
                            'leverRate':20, 'orderPriceType':'limit'},
                           {'symbol': 'BTC','contract_type': 'quarter',
                            'contract_code':'BTC181228', 'client_order_id':'',
                            'price':2, 'volume':2, 'direction':'buy', 'offset':'open',
                            'leverRate':20, 'orderPriceType':'limit'}
                        ]
                }
            :returns :
                :return success: Success results, otherwise it's None.
                :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_batchorder"
        body = orders_data
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, symbol, order_id=None, client_order_id=None):
        """ Revoke an order.

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param order_id: Order ID.
            :param client_order_id: Custom Order ID.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_cancel"
        body = {
            "symbol": symbol
        }
        if order_id:
            body["order_id"] = order_id
        if client_order_id:
            body["client_order_id"] = client_order_id

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_orders(self, symbol, order_ids=None, client_order_ids=None):
        """ Revoke multiple orders.

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param order_ids: Order ID list.
            :param client_order_ids: Client Order Ids.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_cancel"
        body = {
            "symbol": symbol
        }
        if order_ids:
            body["order_id"] = ",".join(order_ids)
        if client_order_ids:
            body["client_order_id"] = ",".join(client_order_ids)

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order_all(self, symbol, contract_code=None, contract_type=None):
        """ Revoke all orders.

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param contract_type: Contract type,`this_week`/`next_week`/`quarter`,default `None` will return all types.
            :param contract_code: Contract code, e.g. BTC180914.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.

        * NOTE: 1. If input `contract_code`, only matching this contract code.
                2. If not input `contract_code`, matching by `symbol + contract_type`.
        """
        uri = "/api/v1/contract_cancelall"
        body = {
            "symbol": symbol,
        }
        if contract_code:
            body["contract_code"] = contract_code
        if contract_type:
            body["contract_type"] = contract_type
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_open_orders(self, symbol, index=1, size=50):
        """ Get open order information.

        Attributes:
            :param symbol: Currency name, e.g. BTC.
            :param index: Page index, default 1st page.
            :param size: Page size, Default 20，no more than 50.

        :returns:
            :return success: Success results, otherwise it's None.
            :return error: Error information, otherwise it's None.
        """
        uri = "/api/v1/contract_openorders"
        body = {
            "symbol": symbol,
            "page_index": index,
            "page_size": size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_api_trading_status(self):
        """ Get api trading status.
        Attributes:
            None.
        :returns:
            refer to https://huobiapi.github.io/docs/dm/v1/cn/#api-5
        """
        uri = "/api/v1/contract_api_trading_status"
        success, error = await self.request("GET", uri, body=None, auth=True)
        return success, error

    async def create_trigger_order(self, symbol, contract_type, trigger_type, trigger_price, order_price,
                                   order_price_type, volume, direction, offset, lever_rate, contract_code=None):
        """ Create trigger order

        Attributes:
            :param symbol: symbol,such as BTC.
            :param contract_type: contract type,such as this_week,next_week,quarter
            :param contract_code: contract code,such as BTC190903.
                                  If filled,the above symbol and contract_type will be ignored.
            :param trigger_type: trigger type,such as ge,le.
            :param trigger_price: trigger price.
            :param order_price: order price.
            :param order_price_type: "limit" by default."optimal_5"\"optimal_10"\"optimal_20"
            :param volume: volume.
            :param direction: "buy" or "sell".
            :param offset: "open" or "close".
            :param lever_rate: lever rate.

        :returns:
            refer to https://huobiapi.github.io/docs/dm/v1/cn/#97a9bd626d

        """
        uri = "/api/v1/contract_trigger_order"
        body = {
            "symbol": symbol,
            "contract_type": contract_type,
            "trigger_type": trigger_type,
            "trigger_price": trigger_price,
            "order_price": order_price,
            "order_price_type": order_price_type,
            "volume": volume,
            "direction": direction,
            "offset": offset,
            "lever_rate": lever_rate
        }
        if contract_code:
            body.update({"contract_code": contract_code})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_trigger_order(self, symbol, order_id):
        """ Revoke trigger order

        Attributes:
            :param symbol: symbol,such as "BTC".
            :param order_id: order ids.multiple orders need to be joined by ','.

        :returns:
            refer to https://huobiapi.github.io/docs/dm/v1/cn/#0d42beab34

        """
        uri = "/api/v1/contract_trigger_cancel"
        body = {
            "symbol": symbol,
            "order_id": order_id
        }

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_all_trigger_orders(self, symbol, contract_code=None, contract_type=None):
        """ Revoke all trigger orders

        Attributes:
            :param symbol: symbol, such as "BTC"
            :param contract_code: contract_code, such as BTC180914.
            :param contract_type: contract_type, such as this_week, next_week, quarter.

        :returns:
            refer to https://huobiapi.github.io/docs/dm/v1/cn/#3d2471d520

        """
        uri = "/api/v1/contract_trigger_cancelall"
        body = {
            "symbol": symbol
        }
        if contract_code:
            body.update({"contract_code": contract_code})
        if contract_type:
            body.update({"contract_type": contract_type})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_trigger_openorders(self, symbol, contract_code=None, page_index=None, page_size=None):
        """ Get trigger openorders
        Attributes:
            :param symbol: symbol, such as "BTC"
            :param contract_code: contract code, such as BTC180914.
            :param page_index: page index.1 by default.
            :param page_size: page size.20 by default.

        :returns:
            refer to https://huobiapi.github.io/docs/dm/v1/cn/#b5280a27b3
        """

        uri = "/api/v1/contract_trigger_openorders"
        body = {
            "symbol": symbol,
        }
        if contract_code:
            body.update({"contract_code": contract_code})
        if page_index:
            body.update({"page_index": page_index})
        if page_size:
            body.update({"page_size": page_size})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def get_trigger_hisorders(self, symbol, trade_type, status, create_date, contract_code=None, page_index=None,
                                    page_size=None):
        """ Get trigger hisorders

        Attributes:
            :param symbol: symbol,such as "BTC"
            :param contract_code: contract code.
            :param trade_type: trade type. 0:all 1:open buy 2:open sell 3:close buy 4:close sell
            :param status: status. 0: orders finished. 4: orders submitted. 5: order filled. 6:order cancelled.
                                   multiple status is joined by ','
            :param create_date: days. such as 1-90.
            :param page_index: 1 by default.
            :param page_size: 20 by default.50 at most.

        :returns:
            https://huobiapi.github.io/docs/dm/v1/cn/#37aeb9f3bd

        """

        uri = "/api/v1/contract_trigger_hisorders"
        body = {
            "symbol": symbol,
            "trade_type": trade_type,
            "status": status,
            "create_date": create_date,
        }
        if contract_code:
            body.update({"contract_code": contract_code})
        if page_index:
            body.update({"page_index": page_index})
        if page_size:
            body.update({"page_size": page_size})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def lightning_close_position(self, symbol, contract_type, contract_code, volume, direction, client_order_id,
                                       order_price_type):
        """ Close position.

        Attributes:
            :param symbol: string.  eg: 'BTC'
            :param contract_type: string. eg: 'this_week'\'next_week'\'quarter'
            :param contract_code: string. eg: 'BTC190903'
            :param volume: int. eg: 1
            :param direction: string. eg: 'buy' or 'sell'
            :param client_order_id: int. eg: 11
            :param order_price_type: string. eg: "lightning"\"lightning_fok"\"lightning_ioc"

        :returns:
            https://docs.huobigroup.com/docs/dm/v1/cn/#669c2a2e3d

        """
        uri = "/api/v1/lightning_close_position"
        body = {
            "volume": volume,
            "direction": direction,
        }

        if symbol:
            body.update({"symbol": symbol})

        if contract_type:
            body.update({"contract_type": contract_type})

        if contract_code:
            body.update({"contract_code": contract_code})

        if client_order_id:
            body.update({"client_order_id": client_order_id})

        if order_price_type:
            body.update({"order_price_type": order_price_type})

        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def transfer_between_spot_future(self, symbol, amount, type_s):
        """ Do transfer between spot and future.
        Attributes:
            :param symbol: currency,such as btc,eth,etc.
            :param amount: transfer amount.pls note the precision digit is 8.
            :param type_s: "pro-to-futures","futures-to-pro"

        """
        body = {
            "currency": symbol,
            "amount": amount,
            "type": type_s
        }

        uri = 'https://api.huobi.pro/v1/futures/transfer'

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


class HuobiFutureMarket(Websocket):
    """ Huobi Swap Market Server.

    Attributes:
        :param platform: Exchange platform name, must be `huobi_future`.
        :param wss: Exchange Websocket host address.
        :param symbols: Trade pair list, e.g. ["BTC-CQ"].
        :param channels: channel list, only `orderbook`, `kline` and `trade` to be enabled.
        :param orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    async def process(self, msg):
        """处于继承目的继承此方法，不做任何实现"""
        pass

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://www.hbdm.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbooks_length = kwargs.get("orderbooks_length", 100)
        self._klines_length = kwargs.get("klines_length", 100)
        self._trades_length = kwargs.get("trades_length", 100)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")

        self._c_to_s = {}  # {"channel": "symbol"}

        url = self._wss + "/ws"
        super(HuobiFutureMarket, self).__init__(url, send_hb_interval=5)
        self.initialize()

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        data = {"pong": int(time.time()*1000)}
        try:
            await self.ws.send_json(data)
        except ConnectionResetError:
            SingleTask.run(self._reconnect)

    async def connected_callback(self):
        """ After create Websocket connection successfully, we will subscribing orderbook/trade events.
        """
        for ch in self._channels:
            if ch == "kline":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "kline")
                    if not channel:
                        continue
                    kline = {
                        "sub": channel
                    }
                    await self.ws.send_json(kline)
            elif ch == "orderbook":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "depth")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            elif ch == "trade":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "trade")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self.ws.send_json(data)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process_binary(self, msg):
        """ Process binary message that received from Websocket connection.
        """
        data = json.loads(gzip.decompress(msg).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self.ws.send_json(hb_msg)
            return

        if channel.find("kline") != -1:
            await self.process_kline(data)

        elif channel.find("depth") != -1:
            await self.process_orderbook(data)

        elif channel.find("trade") != -1:
            await self.process_trade(data)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        """ Convert symbol to channel.

        Args:
            symbol: Trade pair name.such as BTC-USD
            channel_type: channel name, kline / ticker / depth.
        """
        if channel_type == "kline":
            channel = "market.{s}.kline.1min".format(s=symbol.upper())
        elif channel_type == "depth":
            channel = "market.{s}.depth.step6".format(s=symbol.upper())
        elif channel_type == "trade":
            channel = "market.{s}.trade.detail".format(s=symbol.upper())
        else:
            logger.error("channel type error! channel type:", channel_type, caller=self)
            return None
        self._c_to_s[channel] = symbol
        return channel

    async def process_kline(self, data):
        """ process kline data
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        d = data.get("tick")
        info = {
            "platform": self._platform,
            "symbol": symbol,
            "open": "%.8f" % d["open"],
            "high": "%.8f" % d["high"],
            "low": "%.8f" % d["low"],
            "close": "%.8f" % d["close"],
            "volume": "%.8f" % d["amount"],
            "timestamp": int(data.get("ts")),
            "kline_type": MARKET_TYPE_KLINE
        }
        kline = Kline(**info)
        SingleTask.run(self._kline_update_callback, copy.copy(kline))

    async def process_orderbook(self, data):
        """ process orderbook data
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        d = data.get("tick")
        info = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": d.get("asks")[:self._orderbook_length],
            "bids": d.get("bids")[:self._orderbook_length],
            "timestamp": d.get("ts")
        }
        orderbook = Orderbook(**info)
        SingleTask.run(self._orderbook_update_callback, copy.copy(orderbook))

    async def process_trade(self, data):
        """ process trade
        """
        channel = data.get("ch")
        symbol = self._c_to_s[channel]
        ticks = data.get("tick")
        for tick in ticks["data"]:
            direction = tick.get("direction")
            price = tick.get("price")
            quantity = tick.get("amount")
            info = {
                "platform": self._platform,
                "symbol": symbol,
                "action": ORDER_ACTION_BUY if direction == "buy" else ORDER_ACTION_SELL,
                "price": "%.8f" % price,
                "quantity": "%.8f" % quantity,
                "timestamp": tick.get("ts")
            }
            trade = Trade(**info)
            SingleTask.run(self._trade_update_callback, copy.copy(trade))


class HuobiFutureTrade(Websocket):
    """ Huobi Future Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        :param account: Account name for this trade exchange.
        :param strategy: What's name would you want to created for you strategy.
        :param symbol: Symbol name for your trade.
        :param host: HTTP request host. default `https://api.hbdm.com"`.
        :param wss: Websocket address. default `wss://www.hbdm.com`.
        :param access_key: Account's ACCESS KEY.
        :param secret_key Account's SECRET KEY.
        :param asset_update_callback: You can use this param to specific a async callback function when you initializing
                                      Trade object. `asset_update_callback` is like `async def
                                      on_asset_update_callback(asset: Asset): pass` and this callback function will be
                                      executed asynchronous when received AssetEvent.
        :param order_update_callback: You can use this param to specific a async callback function when you
                                      Trade object. `order_update_callback` is like `async def
                                      on_order_update_callback(order: Order): pass` and this callback function will be
                                      executed asynchronous when some order state updated.
        :param position_update_callback: You can use this param to specific a async callback function when you
                                         initializing Trade object. `position_update_callback` is like `async def
                                         on_position_update_callback(order: Position): pass` and this callback function
                                          will be executed asynchronous when some position state updated.
        :param init_success_callback: You can use this param to specific a async callback function when you
                                      initializing Trade object. `init_success_callback` is like `async def
                                      on_init_success_callback (success: bool, error: Error, **kwargs): pass` and this
                                      callback function will be executed asynchronous after Trade module object
                                      initialized successfully.
    """

    async def process(self, msg):
        """只是继承，不做任何实现"""
        pass

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
        self._platform = HUOBI_FUTURE
        self._symbol = kwargs["symbol"].split('_')[0]
        self._contract_type = kwargs["contract_type"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._position_update_callback = kwargs.get("position_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        url = self._wss + "/notification"
        super(HuobiFutureTrade, self).__init__(url, send_hb_interval=5)

        self._assets = {}  # Asset detail, {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }.
        self._orders = {}  # Order objects, {"order_id": order, ...}.
        self._position = Position(self._platform, self._account, self._strategy, self._symbol+'/'+self._contract_type)

        self._order_channel = "orders.{symbol}".format(symbol=self._symbol.lower())
        self._position_channel = "positions.{symbol}".format(symbol=self._symbol.lower())
        self._asset_channel = "accounts.{symbol}".format(symbol=self._symbol.lower())

        self._subscribe_order_ok = False
        self._subscribe_position_ok = False
        self._subscribe_asset_ok = False

        self._rest_api = HuobiFutureRestAPI(self._host, self._access_key, self._secret_key)

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
        sign = self._rest_api.generate_signature("GET", data, "/notification")
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
        if self._subscribe_order_ok and self._subscribe_position_ok:
            success, error = await self._rest_api.get_open_orders(self._symbol)
            if error:
                e = Error("get open orders failed!")
                SingleTask.run(self._init_success_callback, False, e)
            elif "data" in success and "orders" in success["data"]:
                for order_info in success["data"]["orders"]:
                    order_info["ts"] = order_info["created_at"]
                    self._update_order(order_info)
                SingleTask.run(self._init_success_callback, True, None)
            else:
                logger.warn("get open orders:", success, caller=self)
                e = Error("Get Open Orders Unknown error")
                SingleTask.run(self._init_success_callback, False, e)

    @async_method_locker("HuobiFutureTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        :param raw 原始的压缩数据
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
            if data["topic"] == self._order_channel:
                self._update_order(data)
            elif data["topic"].startswith("positions"):
                self._update_position(data)
            elif data["topic"].startswith("accounts"):
                self._update_asset(data)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, client_order_id=None, **kwargs):
        """ Create an order.

        Attributes:
            :param client_order_id:
            :param action: Trade direction, BUY or SELL.
            :param price: Price of each contract.
            :param quantity: The buying or selling quantity.
            :param order_type: Order type, LIMIT or MARKET.
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
        result, error = await self._rest_api.create_order(self._symbol, self._contract_type, '',
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

            orders_data.append({
                "symbol": self._symbol,
                "contract_type": self._contract_type,
                "contract_code": "",
                "client_order_id": client_order_id,
                "price": order["price"],
                "volume": quantity,
                "direction": direction,
                "offset": offset,
                "leverRate": lever_rate,
                "orderPriceType":  order_price_type
            })

        result, error = await self._rest_api.create_orders({"orders_data": orders_data})
        if error:
            return None, error
        order_nos = [order["order_id"] for order in result.get("data").get("success")]
        return order_nos, result.get("data").get("errors")

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Args:
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            Success or error, see bellow.
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            success, error = await self._rest_api.revoke_order_all(self._symbol, '', self._contract_type)
            if error:
                return False, error
            if success.get("errors"):
                return False, success["errors"]
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(self._symbol, order_nos[0])
            if error:
                return order_nos[0], error
            if success.get("errors"):
                return False, success["errors"]
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
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
                if order_info["contract_type"] != self._contract_type or order_info["symbol"] != self._symbol:
                    continue
                order_nos.append(str(order_info["order_id"]))
            return order_nos, None

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.
        """
        if order_info["contract_type"] != self._contract_type or order_info["symbol"] != self._symbol:
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

        # Delete order that already completed.
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

    def _update_position(self, data):
        """ Position update.

        Args:

        Returns:
            None.
        """
        for position_info in data["data"]:
            if position_info["contract_type"] != self._contract_type or position_info["symbol"] != self._symbol:
                continue
            if position_info["direction"] == "buy":
                self._position.long_quantity = int(position_info["volume"])
                self._position.long_avg_price = position_info["cost_open"]
            else:
                self._position.short_quantity = int(position_info["volume"])
                self._position.short_avg_price = position_info["cost_open"]
            # self._position.liquid_price = None
            self._position.utime = data["ts"]
            SingleTask.run(self._position_update_callback, copy.copy(self._position))

    def _update_asset(self, data):
        """ Asset update.

        Args:
            data: asset data.

        Returns:
            None.
        """
        assets = {}
        for item in data["data"]:
            symbol = item["symbol"].upper()
            total = float(item["margin_balance"])
            free = float(item["margin_available"])
            locked = float(item["margin_frozen"])
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

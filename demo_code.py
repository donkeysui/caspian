
class OKEXSwapMarket:
    """ OKEX websocket接口封装 """
    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = "wss://wsaws.okex.com:8443"
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._orderbook_update_callback = kwargs.get("orderbook_update_callback")
        self._kline_update_callback = kwargs.get("kline_update_callback")
        self._trade_update_callback = kwargs.get("trade_update_callback")
        self._init_callback = kwargs.get("init_callback")
        self._error_callback = kwargs.get("error_callback")

        self._orderbook = Orderbook(platform=self._platform)

        public_url = self._wss + "/ws/v5/public"
        self._ws_public = Websocket(
            public_url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )
        private_url = self._wss + "/ws/v5/private"
        self._ws_private = Websocket(
            private_url,
            connected_callback=self.connected_callback,
            process_binary_callback=self.process_binary
        )

        LoopRunTask.register(self._send_heartbeat_msg, 5)

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """发送心跳数据"""
        if not self._ws_public:
            logger.warn("websocket public connection not connected yet!", caller=self)
            return
        if not self._ws_private:
            logger.warn("websocket private connection not established!", caller=self)
            return

        data = "ping"
        try:
            await self._ws_public.send(data)
            await self._ws_private.send(data)
        except ConnectionResetError:
            SingleTask.run(self._ws_public.reconnect)
            SingleTask.run(self._ws_private.reconnect)

    async def connected_callback(self):
        """ 连接成功之后进行行情订阅
        :return:
        """
        for ch in self._channels:
            if ch == "kline":
                if self._kline_update_callback is None:
                    logger.error("Kline callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Kline callback is None")
                    return
                for symbol in self._symbols:
                    kline_sub = {
                        "op": "subscribe",
                        "args":
                        [
                            {
                                 "channel": "candle1m",
                                 "instId": f"{symbol}"
                            }
                        ]
                    }
                    await self._ws_public.send(kline_sub)
                    SingleTask.run(self._init_callback, True, "Sub kline success")

            elif ch == "orderbook":
                if self._orderbook_update_callback is None:
                    logger.error("Orderbook callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Orderbook callback is None")
                    return
                for symbol in self._symbols:
                    depth_sub = {
                        "op": "subscribe",
                        "args": [{"channel": "books50-l2-tbt",
                                  "instId": f"{symbol}"}]
                    }
                    await self._ws_public.send(depth_sub)
                    SingleTask.run(self._init_callback, True, "Sub orderbook success")

            elif ch == "trade":
                if self._trade_update_callback is None:
                    logger.error("Trade callback is None", caller=self)
                    SingleTask.run(self._error_callback, False, "Trade callback is None")
                    return
                for symbol in self._symbols:
                    trade_sub = {
                        "op": "subscribe",
                        "args": [{"channel": "trades",
                                  "instId": f"{symbol}"}]
                    }
                    await self._ws_public.send(trade_sub)
                    SingleTask.run(self._init_callback, True, "Sub trade success")
            else:
                logger.error("channel error! channel:", ch, caller=self)
                SingleTask.run(self._error_callback, False, f"channel error! channel: {ch}")

    @async_method_locker("OKEXSwapMarket.process_binary.locker")
    async def process_binary(self, raw):
        """ 接受来自websocket的数据.
        Args:
            raw: 来自websocket的数据流.

        Returns:
            None.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":
            return

        msg = json.loads(msg)
        # Authorization message received.
        if msg.get("event") == "login":
            if not msg.get("success"):
                e = Error("Websocket连接用户认证失败: {}".format(msg))
                logger.error(e, caller=self)
                SingleTask.run(self._error_callback, "auth error", e)
                return
            logger.info("Websocket 连接用户认证成功.", caller=self)
            return

        # 订阅消息反馈
        if msg.get("event") == "error":
            logger.info(msg, caller=self)
            return

        if "table" in msg.keys():
            channel = msg["table"].split("/")[1]
            # 调用市场交易数据解析函数
            if channel == "trade":
                await self._process_trade(msg["data"])
            # 调用市场深度数据解析函数
            elif channel == "depth_l2_tbt":
                await self._process_orderbook(msg)
            elif channel == "candle60s":
                await self._process_kline(msg["data"])

    async def _process_orderbook(self, data):
        """ orderbook 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        action = data["action"]

        instrument_id = data["arg"]['instId']
        if instrument_id not in self._symbols:
            return
        bids = data["data"][0]["bids"]
        asks = data["data"][0]["asks"]

        if action == "snapshot":
            self._orderbook.symbol = instrument_id
            self._orderbook.bids = bids
            self._orderbook.asks = asks
            self._orderbook.timestamp = data['data'][0]['ts']

            checksum = data['data'][0]['checksum']
            # print(timestamp + '推送数据的checksum为：' + str(checksum))
            check_num = self.check(bids, asks)
            # print(timestamp + '校验后的checksum为：' + str(check_num))
            if check_num == checksum:
                logger.info("订单簿首次推送校验结果为：True", caller=self)
            else:
                # 发送订阅
                logger.info("校验错误，重新连接WS......", caller=self)
                SingleTask.run(self._ws.reconnect)

            d = copy.copy(self._orderbook)
            asks = d.asks[:self._orderbook_length]
            bids = d.bids[:self._orderbook_length]

            d.asks = asks
            d.bids = bids

            SingleTask.run(self._orderbook_update_callback, d)

        if action == "update":
            # 获取全量数据
            bids_p = self._orderbook.bids
            asks_p = self._orderbook.asks
            bids_p = self.update_bids(bids, bids_p)
            asks_p = self.update_asks(asks, asks_p)

            self._orderbook.bids = bids_p
            self._orderbook.asks = asks_p
            self._orderbook.timestamp = data['data'][0]['ts']

            checksum = data['data'][0]['checksum']
            check_num = self.check(bids_p, asks_p)

            if check_num == checksum:
                pass
            else:
                logger.info(f"{instrument_id}, Update 校验结果为：False，正在重新订阅……", caller=self)

                # 发送订阅
                SingleTask.run(self._ws.reconnect)
            d = copy.copy(self._orderbook)
            asks = d.asks[:self._orderbook_length]
            bids = d.bids[:self._orderbook_length]

            d.asks = asks
            d.bids = bids

            SingleTask.run(self._orderbook_update_callback, d)

    async def _process_trade(self, data):
        """ trade 数据解析、封装、回调
        Args:
            :param data: WS推送数据

        Return:
            :return: None
        """

        # 返回数据封装，加上时间戳和品种交易所信息
        for dt in data:
            instrument_id = dt.get("instId")
            if instrument_id not in self._symbols:
                return
            trade = Trade()
            trade.platform = self._platform
            trade.symbol = dt["instId"]
            trade.price = dt["px"]
            if dt.get("sz"):
                trade.quantity = dt["sz"]
            trade.side = dt["side"]
            trade.trade_id = dt["tradeId"]
            trade.timestamp = dt['ts']

            # 异步回调
            SingleTask.run(self._trade_update_callback, trade)

    async def _process_kline(self, data):
        """ kline 数据解析、封装、回调
         Args:
            :param data: WS推送数据

        Return:
            :return: None
        """
        for k in data:
            instrument_id = k.get("instrument_id")
            if instrument_id not in self._symbols:
                return
            kline = Kline(platform=self._platform)
            kline.symbol = k["instrument_id"]
            kline.open = k["candle"][1]
            kline.high = k["candle"][2]
            kline.low = k["candle"][3]
            kline.close = k["candle"][4]
            kline.volume = k["candle"][5]
            kline.coin_volume = k["candle"][6]
            kline.timestamp = k["candle"][0]

            SingleTask.run(self._kline_update_callback, kline)

    # 订单簿增量数据相关校验、拼接等方法
    def update_bids(self, res, bids_p):
        """ 更新全量Bids数据
        Args:
            :param res: 原始Bids数据
            :param bids_p: 更新的Bids数据
        Return:
            :return: 返回新的全量Bids数据
        """
        # 获取增量bids数据
        bids_u = res
        # bids合并
        for i in bids_u:
            bid_price = i[0]
            for j in bids_p:
                if bid_price == j[0]:
                    if i[1] == '0':
                        bids_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    bids_p.append(i)
        else:
            bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
        return bids_p

    def update_asks(self, res, asks_p):
        """ 更新全量Asks数据
        Args:
            :param res: 原始Asks数据
            :param asks_p: 更新的Asks数据
        Return:
            :return: 返回新的全量Asks数据
        """
        # 获取增量asks数据
        asks_u = res
        # asks合并
        for i in asks_u:
            ask_price = i[0]
            for j in asks_p:
                if ask_price == j[0]:
                    if i[1] == '0':
                        asks_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    asks_p.append(i)
        else:
            asks_p.sort(key=lambda price: self.sort_num(price[0]))
        return asks_p

    def sort_num(self, n):
        """ 排序函数
        Args:
            :param n: 数据对象
        Return:
            :return: 排序结果
        """
        if n.isdigit():
            return int(n)
        else:
            return float(n)

    def check(self, bids, asks):
        """ 首次接受订单簿对订单簿数据进行校验
        Args:
            :param bids: 全量bids数据
            :param asks: 全量asks数据
        Return:
            :return: 返回校验结果
        """
        # 获取bid档str
        bids_l = []
        bid_l = []
        count_bid = 1
        while count_bid <= 25:
            if count_bid > len(bids):
                break
            bids_l.append(bids[count_bid - 1])
            count_bid += 1
        for j in bids_l:
            str_bid = ':'.join(j[0: 2])
            bid_l.append(str_bid)
        # 获取ask档str
        asks_l = []
        ask_l = []
        count_ask = 1
        while count_ask <= 25:
            if count_ask > len(asks):
                break
            asks_l.append(asks[count_ask - 1])
            count_ask += 1
        for k in asks_l:
            str_ask = ':'.join(k[0: 2])
            ask_l.append(str_ask)
        # 拼接str
        num = ''
        if len(bid_l) == len(ask_l):
            for m in range(len(bid_l)):
                num += bid_l[m] + ':' + ask_l[m] + ':'
        elif len(bid_l) > len(ask_l):
            # bid档比ask档多
            for n in range(len(ask_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(ask_l), len(bid_l)):
                num += bid_l[l] + ':'
        elif len(bid_l) < len(ask_l):
            # ask档比bid档多
            for n in range(len(bid_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(bid_l), len(ask_l)):
                num += ask_l[l] + ':'

        new_num = num[:-1]
        int_checksum = zlib.crc32(new_num.encode())
        fina = self.change(int_checksum)
        return fina

    def change(self, num_old):
        """ 生成checksum验证数据
        Args:
            :param num_old: 校验数据
        Return:
            :return: 返回校验数据
        """
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out


class OKEXSwapRestApi:
    """ OKEx永续合约REST API客户端.

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
        2020年11月26日 更新:
            添加:公共-获取全部ticker信息，为了统计每天成交比较活跃的品种做高频从策略
    """

    def __init__(self, access_key="", secret_key="", passphrase=""):
        """initialize REST API client."""
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase

    async def get_all_ticker(self):
        """ 获取全部ticker信息
        :return:
        """
        success, error = await self.request("GET", "/api/v5/market/tickers")

        if error:
            return None, error
        return success, error

    async def get_contract_info(self, contract_code=None):
        """ 获取合约信息
        Args：
            :param contract_code: 合约合约代码. e.g. BTC-USDT-SWAP.

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.

        * 注意: 1. 如果`contract_code`参数不为None, 仅仅匹配contract_code所有信息.
                2. 如果`contract_code`参数为None, 匹配`symbol + contract_type`所有信息.
        """
        success, error = await self.request("GET", "/api/v5/public/instruments", auth=False)
        if error:
            return None, error
        if contract_code:
            for i in success:
                if contract_code == i["instrument_id"]:
                    return i, error
        else:
            return success, error

    async def get_price_limit(self, symbol):
        """ 获取合约当前交易的最高买价和最低卖价
        Args：
            :param symbol: 交易对信息.

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.
        """
        success, error = await self.request("GET", f"/api/v5/public/price-limit?instId={symbol}", auth=False)
        return success, error

    async def get_kline(self, symbol, before='', after='', bar='1m'):
        """ 获取合约信息
                Args：
                    :param instId: 交易对信息. 默认为None。
                    :param bar: 如[1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y]
                    :param after: 结束时间（ISO 8601标准，例如：2018-06-20T02:31:00Z）
                    :param before: 开始时间（ISO 8601标准，例如：2018-06-20T02:31:00Z）

                Return:
                    :return: success: 成功结果, 否则返回None.
                    :return error: 错误信息, 否则为None None.

                * 注意: 1. 如果`contract_code`参数不为None, 仅仅匹配contract_code所有信息.
                        2. 如果`contract_code`参数为None, 匹配`symbol + contract_type`所有信息.
                """
        uri = f'/api/v5/market/candles'
        params = {}
        params['instId'] = symbol

        if before:
            params['before'] = before
        if after:
            params['after'] = after
        if bar:
            params['bar'] = bar
        success, error = await self.request("GET", uri, params)
        if error:
            return None, error
        return success, error

    async def get_orderbook(self, instId, sz=10):
        """ 获取订单簿信息
        Args：
            :param limit: 返回深度数量，最大值可传200，即买卖深度共400条
            :param depth: 按价格合并深度，例如：0.1，0.001
            :param symbol: 合约名称

        Return:
            :return: success: 成功结果, 否则返回None.
            :return error: 错误信息, 否则为None None.
        """
        params = {
            "sz": sz,
            "instId": instId
        }
        success, error = await self.request("GET", f"/api/v5/market/books", params=params)

        return success, error


    async def get_asset_info(self):
        """ 获取所有合约的账户资产信息，当用户没有持仓时，保证金率为10000
        Args:
            NONE
        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        success, error = await self.request("GET", '/api/swap/v3/accounts', auth=True)
        return success, error

    async def get_position(self, instType=None, instId=None):
        """ 获取某个合约的持仓信息.
        Args:
            :param instType: instrument种类
            :param instId: 合约名称ID, e.g. BTC-USD-SWAP.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        uri = f"/api/v5/account/positions"
        params = {}
        if instType:
            params['instType'] = instType
        if instId:
            params['instId'] = instId

        success, error = await self.request("GET", uri, auth=True, params=params)

        return success, error

    async def create_order(self, instId, tdMode, side, ordType, sz, ccy=None, clOrdId=None, tag=None, posSide=None, px=None, reduceOnly=None, tgtCcy=None):
        """

        Args:
            instId:     产品ID,如 BTC-USD-190927-5000-C

            tdMode:     交易模式
                        保证金模式：isolated：逐仓 ；cross：全仓
                        非保证金模式：cash：非保证金

            side:       订单方向 buy：买 sell：卖

            ordType:    订单类型
                        market：市价单
                        limit：限价单
                        post_only：只做maker单
                        fok：全部成交或立即取消
                        ioc：立即成交并取消剩余
                        optimal_limit_ioc：市价委托立即成交并取消剩余（仅适用交割、永续）

            sz:         委托数量

            ccy:        保证金币种，仅适用于单币种保证金模式下的全仓杠杆订单

            clOrdId:    客户自定义订单ID
                        字母（区分大小写）与数字的组合，可以是纯字母、纯数字且长度要在1-32位之间。

            tag:        订单标签
                        字母（区分大小写）与数字的组合，可以是纯字母、纯数字，且长度在1-8位之间。

            posSide:    持仓方向 在双向持仓模式下必填，且仅可选择 long 或 short

            px:         委托价格，仅适用于limit、post_only、fok、ioc类型的订单

            reduceOnly:	是否只减仓，true 或 false，默认false
                            仅适用于币币杠杆订单

            tgtCcy:     市价单委托数量的类型
                        base_ccy：交易货币 ；quote_ccy：计价货币
                        仅适用于币币订单

        Returns:

        """
        order_info = {
            'instId':instId,
            'tdMode':tdMode,
            'side':side,
            'ordType':ordType,
            'sz':sz,
            }

        if ccy:
            order_info['ccy'] = ccy
        if clOrdId:
            order_info['clOrdId'] = clOrdId
        if tag:
            order_info['tag'] = tag
        if posSide:
            order_info['posSide'] = posSide
        if px:
            order_info['px'] = px
        if reduceOnly:
            order_info['reduceOnly'] = reduceOnly
        if tgtCcy:
            order_info['tgtCcy'] = tgtCcy

        result, error = await self.request("POST", "/api/v5/trade/order", body=order_info, auth=True)
        return result, error

    async def create_orders(self, order_datas):
        """ 批量进行合约下单操作。每个合约可批量下10个单。
        Args:
            :param symbol: 合约代码
            :param order_datas: 订单List:
                                    JSON类型的字符串 例：[{order_type:"0",price:"5",size:"2",type:"1",match_price:"1"},
                                    {order_type:"0",price:"2",size:"3",type:"1",match_price:"1"}]
                                    最大下单量为10，当以对手价下单，order_type只能选择0（普通委托）。
                                    price, size, type, match_price 参数参考future_trade接口中的说明
        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.

        Note:
            限速规则：300次/2s
                    1）不同合约之间限速不累计；
                    2）同一合约的当周次周季度之间限速累计；
                    3）同一合约的币本位和USDT保证金之间限速不累计
        """
        if len(order_datas) > 20:
            return {"error", "order_data is to long, limit 20"}

        params = order_datas

        success, error = await self.request("POST", "/api/v5/trade/batch-orders", body=params, auth=True)

        return success, error

    async def revoke_order(self, instId, ordId=None, clOrdId=None):
        """ 撤销指定订单.
        Args:
            :param symbol: 交易币对, e.g. BTC-USDT-SWAP.
            :param order_no: 订单ID或者是客户端ID

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        params = {
            'instId': instId,
        }
        if ordId:
            params['ordId'] = ordId
        if clOrdId:
            params['clOrdId'] = clOrdId

        uri = "/api/v5/trade/cancel-order"
        result, error = await self.request("POST", uri, auth=True)
        if error:
            return ordId, error
        if result["result"]:
            return ordId, None
        return ordId, result

    async def revoke_orders(self, order_datas):
        """ 批量撤单, 根据指定的order_id或者是client_id列表撤销某个合约的未完成订单，
            order_id和client_id只能选择其一

        Args:
            :param client_oids: 客户ID列表
            :param ids: 订单列表
            :param symbol: 交易币对, e.g. BTC-USDT-SWAP.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """
        uri = "/api/v5/trade/cancel-batch-orders"
        result, error = await self.request("POST", uri, body=order_datas, auth=True)
        return result, error

    async def revoke_orders_all(self, instId, mgnMode, posSide=None, ccy=None):
        """ 撤销所有订单
        Args:
            :param symbol: 合约名称ID, e.g. BTC-USDT-SWAP.

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.
        """

        body = {
            'instId': instId,
            'mgnMode': mgnMode,
        }

        if posSide:
            body['posSide'] = posSide
        if ccy:
            body['ccy'] = ccy
        uri = '/api/v5/trade/close-position'

        result, error = await self.request("POST", uri, body=body)
        if error:
            return None, None
        if result['result']:
            return result, None

    async def get_order_info(self, instId, ordId=None, clOrdId=None):
        """ 列出您当前所有的订单信息。本接口能查询最近三个月的数据。这个请求支持分页，
            并且按委托时间倒序排序和存储，最新的排在最前面

        Args:
            :param before:
            :param after:
            :param instrument_id: Contract ID, e.g. BTC-USD-SWAP.
            :param state: Order state for filter. ("-2": Failed, "-1": Cancelled, "0": Open , "1": Partially Filled,
                    "2": Fully Filled, "3": Submitting, "4": Cancelling, "6": Incomplete(open + partially filled),
                    "7": Complete(cancelled + fully filled)).
            :param limit: Number of results per request. Maximum 100. (default 100)

        Returns:
            :return success: 成功但会否则返回None.
            :return error: 有错误时返回错误信息否则返回None.

        TODO: Add args `from` & `to`.
        """
        uri = "/api/v5/trade/order"
        params = {"instId": instId}
        if ordId:
            params['ordId'] = ordId
        if clOrdId:
            params['clOrdId'] = clOrdId

        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_order_list(self, instType=None, uly=None, instId=None, ordType=None, state=None, after=None, before=None, limit=None):
        """ Get order detail by order ID. Canceled unfilled orders will be kept in record for 2 hours only.
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/v5/trade/orders-pending"
        params = {}

        if instType:
            params['instType'] = instType
        if uly:
            params['uly'] = uly
        if instId:
            params['instId'] = instId
        if ordType:
            params['ordType'] = ordType
        if state:
            params['state'] = state
        if after:
            params['after'] = after
        if before:
            params['before'] = before
        if limit:
            params['limit'] = limit

        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_funding(self, instId, after=None, before=None, limit=None):
        """ 获取资金费率

        Args:
            :param symbol: 交易币对

        Returns:
            success: 返回成功数据结果, 其他为None.
            error: 返回错误数据结果, 其他为None.
        """
        uri = f"/api/v5/public/funding-rate-history"
        params = {'instId': instId}
        if after:
            params['after'] = after
        if before:
            params['before'] = before
        if limit:
            params['limit'] = limit

        success, error = await self.request("GET", uri, params=params)
        return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(HOST, uri)

        if auth:
            timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
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

        _, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10)
        return success, error


## LAST MODIFIED HERE

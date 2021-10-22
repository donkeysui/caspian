# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2021/9/29 21:31
  @ Description: 
  @ History:
"""

import importlib

from xuanwu import const


class RestAPIClient:
    """ Rest API Client Module.

    Attributes:
        platform: Exchange platform name, e.g. `binance_spot` / `okex_spot` / `bitmex`.
        host: HTTP request host.
        wss: Websocket address.
        account: Account name for this trade exchange.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase. (Only for `OKEx` / `KuCoin` / `Bitcoke`)
    """

    def __init__(self, platform: str = None, host: str = None, account: str = None, access_key: str = None,
                 secret_key: str = None, passphrase: str = None, **kwargs):
        kwargs["platform"] = platform
        kwargs["host"] = host
        kwargs["account"] = account
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key
        kwargs["passphrase"] = passphrase
        if platform not in const.EXCHANGES:
            raise Exception("platform error: {}".format(platform))
        x, y = const.EXCHANGES[platform]["rest_api"].rsplit(".", 1)
        m = importlib.import_module(x)
        self._rest_api = getattr(m, y)(**kwargs)

    async def status(self, *args, **kwargs):
        """Get event status of system upgrade and maintenance."""
        success, error = await self._rest_api.status(*args, **kwargs)
        return success, error

    async def get_symbols(self, *args, **kwargs):
        """Get symbol informations."""
        success, error = await self._rest_api.get_symbols(*args, **kwargs)
        return success, error

    async def get_kline(self, *args, **kwargs):
        """Get kline information."""
        klines, error = await self._rest_api.get_kline(*args, **kwargs)
        return klines, error

    async def get_trade(self, *args, **kwargs):
        """Get trade information."""
        trades, error = await self._rest_api.get_trade(*args, **kwargs)
        return trades, error

    async def get_orderbook(self, *args, **kwargs):
        """Get orderbook information."""
        orderbook, error = await self._rest_api.get_orderbook(*args, **kwargs)
        return orderbook, error

    async def get_asset(self, *args, **kwargs):
        """Get account's asset information."""
        asset, error = await self._rest_api.get_asset(*args, **kwargs)
        return asset, error

    async def get_position(self, *args, **kwargs):
        """Get the information of holding positions of a contract."""
        order_id, error = await self._rest_api.get_position(*args, **kwargs)
        return order_id, error

    async def create_order(self, *args, **kwargs):
        """Create an order."""
        order_id, error = await self._rest_api.create_order(*args, **kwargs)
        return order_id, error

    async def edit_order(self, *args, **kwargs):
        """Editing an opening order."""
        success, error = await self._rest_api.edit_order(*args, **kwargs)
        return success, error

    async def revoke_order(self, *args, **kwargs):
        """Revoke (an) order(s)."""
        order_id, error = await self._rest_api.revoke_order(*args, **kwargs)
        return order_id, error

    async def revoke_orders(self, *args, **kwargs):
        """Revoke mupliple orders."""
        success, error = await self._rest_api.revoke_orders(*args, **kwargs)
        return success, error

    async def get_order_info(self, *args, **kwargs):
        """Get order information."""
        success, error = await self._rest_api.get_order_info(*args, **kwargs)
        return success, error

    async def get_open_orders(self, *args, **kwargs):
        """Get active order informations."""
        success, error = await self._rest_api.get_open_orders(*args, **kwargs)
        return success, error

    async def set_margin_mode(self, *args, **kwargs):
        """Set position margin mode."""
        success, error = await self._rest_api.set_margin_mode(*args, **kwargs)
        return success, error

    async def set_leverage(self, *args, **kwargs):
        """Set contract order leverage."""
        success, error = await self._rest_api.set_leverage(*args, **kwargs)
        return success, error

    async def transfer(self, *args, **kwargs):
        """Transfer asset."""
        success, error = await self._rest_api.transfer(*args, **kwargs)
        return success, error

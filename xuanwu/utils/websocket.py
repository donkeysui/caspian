# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/9/27 17:17
  @ Description:
  @ History:
"""
import json
import asyncio
import aiohttp
import traceback
from xuanwu.utils import logger
from xuanwu.configure import config
from xuanwu.heartbeat import heartbeat

__all__ = ("Websocket",)


class Websocket:
    """Websocket connection.

    Attributes:
        url: Websocket 连接地址.
        connected_callback: Asynchronous callback function will be called after connected to Websocket server successfully.
        process_callback: Asynchronous callback function will be called if any stream data receive from Websocket
            connection, this function only callback `text/json` message. e.g.
                async def process_callback(json_message): pass
        process_binary_callback: Asynchronous callback function will be called if any stream data receive from Websocket
            connection, this function only callback `binary` message. e.g.
                async def process_binary_callback(binary_message): pass
        check_conn_interval: Check Websocket connection interval time(seconds), default is 10s.
    """

    def __init__(self, url, check_conn_interval=10, send_hb_interval=15, **kwargs):
        """Initialize."""
        self._url = url
        self._check_conn_interval = check_conn_interval
        self._send_hb_interval = send_hb_interval
        self.ws = None  # websocket连接对象
        self.heartbeat_msg = None  # 心跳消息

    def initialize(self):
        """ 初始化
        """
        # 注册服务 检查连接是否正常
        heartbeat.register(self._check_connection, self._check_conn_interval)
        # 注册服务 发送心跳
        if self._send_hb_interval > 0:
            heartbeat.register(self._send_heartbeat_msg, self._send_hb_interval)
        # 建立websocket连接
        asyncio.get_event_loop().create_task(self._connect())

    async def _connect(self):
        logger.info("url:", self._url, caller=self)
        proxy = config.proxy
        session = aiohttp.ClientSession()
        try:
            self.ws = await session.ws_connect(self._url, proxy=proxy)
        except aiohttp.ClientConnectorError:
            logger.error("connect to server error! url:", self._url, caller=self)
            return
        asyncio.get_event_loop().create_task(self.connected_callback())
        asyncio.get_event_loop().create_task(self.receive())

    async def _reconnect(self):
        """ 重新建立websockets连接
        """
        logger.warn("reconnecting websocket right now!", caller=self)
        await self._connect()

    async def connected_callback(self):
        """ 连接建立成功的回调函数
        * NOTE: 子类继承实现
        """
        pass

    async def receive(self):
        """ 接收消息
        """
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    data = msg.data
                await asyncio.get_event_loop().create_task(self.process(data))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await asyncio.get_event_loop().create_task(self.process_binary(msg.data))
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warn("receive event CLOSED:", msg, caller=self)
                await asyncio.get_event_loop().create_task(self._reconnect())
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                logger.warn("receive event CLOSE:", msg, caller=self)
                await asyncio.get_event_loop().create_task(self._reconnect())
            elif msg.type == aiohttp.WSMsgType.CLOSING:
                logger.warn("receive event CLOSING:", msg, caller=self)
                await asyncio.get_event_loop().create_task(self._reconnect())
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error("receive event ERROR:", msg, caller=self)
                await asyncio.get_event_loop().create_task(self._reconnect())
            else:
                logger.warn("unhandled msg:", msg, caller=self)

    async def process(self, msg):
        """ 处理websocket上接收到的消息 text 类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息 binary类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def _check_connection(self, *args, **kwargs):
        """ 检查连接是否正常
        """
        # 检查websocket连接是否关闭，如果关闭，那么立即重连
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        if self.ws.closed:
            await asyncio.get_event_loop().create_task(self._reconnect())
            return

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        if self.heartbeat_msg:
            try:
                if isinstance(self.heartbeat_msg, dict):
                    await self.ws.send_json(self.heartbeat_msg)
                elif isinstance(self.heartbeat_msg, str):
                    await self.ws.send_str(self.heartbeat_msg)
                else:
                    logger.error("send heartbeat msg failed! heartbeat msg:", self.heartbeat_msg, caller=self)
                    return
                logger.debug("send ping message:", self.heartbeat_msg, caller=self)
            except ConnectionResetError:
                traceback.print_exc()
                await asyncio.get_event_loop().create_task(self._reconnect())

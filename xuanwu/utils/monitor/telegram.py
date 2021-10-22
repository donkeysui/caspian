# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/9/27 17:17
  @ Description: Telegram报警封装
  @ History:
"""

from xuanwu.utils import logger
from xuanwu.utils.connect_tool import AsyncHttpRequests


class TelegramBot:
    """ Telegram机器人接口
    """
    BASE_URL = "https://api.telegram.org"

    @classmethod
    async def send_text_msg(cls, token, chat_id, content, proxy=None):
        """ 发送文本消息
        @param token Telegram机器人token
        @param chat_id Telegram的chat_id
        @param content 消息内容
        @param proxy HTTP代理
        """
        url = "{base_url}/bot{token}/sendMessage?chat_id={chat_id}&text={content}".format(
            base_url=cls.BASE_URL,
            token=token,
            chat_id=chat_id,
            content=content
        )
        result = await AsyncHttpRequests.get(url, proxy=proxy)
        logger.info("url:", url, "result:", result, caller=cls)

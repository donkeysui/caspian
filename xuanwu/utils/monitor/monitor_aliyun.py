# -*- coding: utf-8 -*-
"""
  @ Author:   Turkey
  @ Email:    suiminyan@gmail.com
  @ Date:     2021/9/22 10:40
  @ Description: 
  @ History:
"""
import hmac
import hashlib
import base64
import json
import requests
import urllib.parse
import time
from xuanwu.configure import config
from xuanwu.utils import logger
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest


class AliyunMonitoring:
    def __init__(self, api_key='', secret_key='', called_show_number='', tts_code='', action_mode='', called_Number=""):
        '''
        参数:
            CalledNumber(必要):被通知号码

            api_key(默认):公钥
            secret_key(默认):私钥
            CalledShowNumber(默认):购买的电话(阿里云端)
            TtlCode(默认):语音模板ID(可选)
            Action_mode(默认):文本转语音通知模式接口
        '''
        self._api_key = api_key
        self._secret_key = secret_key
        self._called_show_number = called_show_number
        self._tts_code = tts_code
        self._action_mode = action_mode
        self._called_number = called_Number

    def voice_by_call(self, num, msg):
        """
        语音电话通知功能
        :param num:必要参数， “CTA01”， 账户名称
        :param msg:必要参数， 具体消息内容
        :return:
        """
        client = AcsClient(self._api_key, self._secret_key, 'default')

        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain('dyvmsapi.aliyuncs.com')
        request.set_method('POST')
        request.set_protocol_type('https')
        request.set_version('2017-05-25')
        request.set_action_name(self._action_mode)

        request.add_query_param('CalledShowNumber', self._called_show_number)
        request.add_query_param('CalledNumber', "{}".format(self._called_number))
        request.add_query_param('TtsCode', self._tts_code)
        request.add_query_param('TtsParam', "{\"code\":\"%s\",\"problem\":\"%s\"}" % (num, msg))
        response = client.do_action_with_exception(request)

        return response


class DingDingMonitoring:
    def __init__(self, secret_key="", dingding_api=""):
        self._secret_key = secret_key
        self._dingding_api = dingding_api
        self._timestamp = ""
        self._sign = ""

    def _get_sgin(self):
        ''' 钉钉签名设置
        :return:  返回时间戳和签名
        '''
        timestamp = str(round(time.time() * 1000))
        secret = self._secret_key
        secret_enc = str(secret).encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = str(string_to_sign).encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        self._timestamp = timestamp
        self._sign = sign

    def sendmessage(self, message):
        ''' 消息发送
        :param message: 需要发送的消息内容
        :param timestamp: 时间戳
        :param sign: 签名
        :return:
        '''
        self._get_sgin()
        sign = self._sign  # 获取签名
        timestamp = self._timestamp
        uri = "{}&timestamp={}&sign={}".format(self._dingding_api, timestamp, sign)
        HEADERS = {
            "Content-Type": "application/json ;charset=utf-8"
        }
        message = message
        String_textMsg = {
            "msgtype": "text",
            "text": {"content": message}
        }
        String_textMsg = json.dumps(String_textMsg)
        try:
            res = requests.post(uri, data=String_textMsg, headers=HEADERS)
            return res
        except:
            pass


class Monitoring:
    def __init__(self):
        self._dingding = None
        self._phone_call = None

        self._init_monitoring()

    def _init_monitoring(self):
        if config.phone_call:
            phone_config = config.phone_call
            self._phone_call = AliyunMonitoring(
                api_key=phone_config["api_key"],
                secret_key=phone_config["secret_key"],
                called_show_number=phone_config["called_show_number"],
                tts_code=phone_config["tts_code"],
                action_mode=phone_config["action_mode"],
                called_Number=phone_config["called_Number"]
            )

        if config.dingding:
            dingding_config = config.dingding

            self._dingding = DingDingMonitoring(
                dingding_api=dingding_config["dingding_api"],
                secret_key=dingding_config["secret_key"]
            )

    def phone_call(self, tip, msg):
        """ 电话报警提示功能
        :param tip: 需要提示的电话类型
        :param msg: 具体报警内容
        :return:
        """
        if self._phone_call is None:
            logger.error("电话报警配置错误，请设置电话报警相关参数", caller=self)
            return "电话报警配置错误，请设置电话报警相关参数"
        result = self._phone_call.voice_by_call(tip, msg)
        return result

    def dingding_monitor(self, msg):
        """ 钉钉报警消息发送
        :param msg: 具体需要发送的消息内容
        :return:
        """
        if self._dingding is None:
            logger.error("钉钉报警配置错误，请设置钉钉报警相关参数", caller=self)
            return "钉钉报警配置错误，请设置钉钉报警相关参数"
        result = self._dingding.sendmessage(msg)
        return result

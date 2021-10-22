# -*- coding: utf-8 -*-
'''
  @ FileName: configure
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/5/14 19:32
  @ Description: 读取策略总的配置文件，按照规定的类型读取配置文件
  @ History:
  @ <author>          <time>          <version>          <desc>
  @ 作者姓名           修改时间           版本号            描述
'''

import json
from xuanwu.utils import logger
from xuanwu.utils import tools


class Configure:
    """加载`config.json`配置文件并读取成对应的json对象.
        1. 配置内容必须是键值对，并且“key”将被设置为配置模块的属性;
        2. 调用配置模块的属性get获取这些值;
        3、对应的键名称需要大写内置，所有的内置的键需要小写配置
            SERVER_ID: 服务id, 每一个运行的进程有唯一的ID标记.
            LOG: 日志打印配置.
            DINGDING: 钉钉账户报警配置.
            PHONE_CALL：电话报警配置
            ACCOUNTS: 交易账户配置列表, 默认是 [].
            HEARTBEAT: 服务心跳配置, 默认是 {}.
    """

    def __init__(self):
        self.dingding = {}
        self.phone_call = {}

        self.server_id = None
        self.run_time_update = False
        self.log = {}
        self.rabbitmq = {}
        self.mongodb = {}
        self.redis = {}
        self.platforms = []
        self.accounts = []
        self.markets = {}
        self.heartbeat = {}
        self.proxy = None

    def loads(self, config_file=None) -> None:
        """Load config file.

        Args:
            config_file: config json file.
        """
        configures = {}
        if config_file:
            try:
                with open(config_file) as f:
                    data = f.read()
                    configures = json.loads(data)
            except Exception as e:
                print(e)
                exit(0)
            if not configures:
                print("config json file error!")
                exit(0)
        self._update(configures)

    def _on_event_config(self, data):
        """ 配置中心更新事件
        :param data:
        :return:
        """
        server_id = data["server_id"]
        params = data["params"]
        if server_id != self.server_id:
            logger.error("Server id error:", server_id, caller=self)
            return
        if not isinstance(params, dict):
            logger.error("params format error:", params, caller=self)
            return

        params["SERVER_ID"] = self.server_id
        params["RUN_TIME_UPDATE"] = self.run_time_update
        self._update(params)
        logger.info("config update success!", caller=self)

    def _update(self, update_fields) -> None:
        """ Update config attributes.

        Args:
            update_fields: Update fields.
        """
        self.dingding = update_fields.get("DINGDING", {})
        self.phone_call = update_fields.get("PHONE_CALL", {})

        self.server_id = update_fields.get("SERVER_ID", tools.get_uuid1())
        self.run_time_update = update_fields.get("RUN_TIME_UPDATE", False)
        self.log = update_fields.get("LOG", {})
        self.rabbitmq = update_fields.get("RABBITMQ", {})
        self.mongodb = update_fields.get("MONGODB", {})
        self.redis = update_fields.get("REDIS", {})
        self.platforms = update_fields.get("PLATFORMS", [])
        self.accounts = update_fields.get("ACCOUNTS", [])
        self.markets = update_fields.get("MARKETS", {})
        self.heartbeat = update_fields.get("HEARTBEAT", {})
        self.proxy = update_fields.get("PROXY", None)

        for k, v in update_fields.items():
            setattr(self, k, v)

        for k, v in update_fields.items():
            setattr(self, k, v)


config = Configure()

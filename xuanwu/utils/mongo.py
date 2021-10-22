# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/9/27 17:17
  @ Description:
  @ History:
  Update: 2018/12/11  1. 取消初始化使用类变量 DB 和 COLLECTION，直接在 self.__init__ 函数传入 db 和 collection;
                      2. 修改名称 self.conn 到 self._conn;
                      3. 修改名称 self.cursor 到 self._cursor;
"""

import copy

import pymongo
import motor.motor_asyncio
from bson.objectid import ObjectId
from urllib.parse import quote_plus
from functools import wraps

from xuanwu.utils import logger
from xuanwu.tasks import SingleTask

__all__ = ("MongoDB",)

DELETE_FLAG = "delete"  # Delete flag, `True` is deleted, otherwise is not deleted.


def forestall(fn):
    """
    装饰器函数
    """

    @wraps(fn)
    async def wrap(self, *args, **kwargs):
        if not self._connected:
            return None, Exception("mongodb connection lost")
        try:
            return await fn(self, *args, **kwargs)
        except Exception as e:
            return None, e

    return wrap


class MongoDB(object):
    """ Create a MongoDB connection cursor.

    Args:
        db: DB name.
        collection: Collection name.
    """

    _mongo_client = None
    _connected = False
    _state_cbs = []

    @classmethod
    def mongodb_init(cls, host="127.0.0.1", port=27017, username="", password="", dbname="admin"):
        """ Initialize a connection pool for MongoDB.

        Args:
            host: Host for MongoDB server.
            port: Port for MongoDB server.
            username: Username for MongoDB server.
            password: Password for MongoDB server.
            dbname: DB name to connect for, default is `admin`.
        """
        if username and password:
            uri = "mongodb://{username}:{password}@{host}:{port}/{dbname}".format(username=quote_plus(username),
                                                                                  password=quote_plus(password),
                                                                                  host=quote_plus(host),
                                                                                  port=port,
                                                                                  dbname=dbname)
        else:
            uri = "mongodb://{host}:{port}/{dbname}".format(host=host, port=port, dbname=dbname)
        cls._mongo_client = motor.motor_asyncio.AsyncIOMotorClient(uri, connectTimeoutMS=5000,
                                                                   serverSelectionTimeoutMS=5000)
        # LoopRunTask.register(cls._check_connection, 2)
        SingleTask.call_later(cls._check_connection, 2)  # 模拟串行定时器,避免并发
        logger.info("create mongodb connection pool.")

    @classmethod
    async def _check_connection(cls, *args, **kwargs):
        try:
            ns = await cls._mongo_client.list_database_names()
            if ns and isinstance(ns, list) and "admin" in ns:
                cls._connected = True
        except Exception as e:
            cls._connected = False
            logger.error("mongodb connection ERROR:", e)
        finally:
            SingleTask.call_later(cls._check_connection, 2)  # 开启下一轮检测

    @classmethod
    def is_connected(cls):
        return cls._connected

    @classmethod
    def register_state_callback(cls, func):
        cls._state_cbs.append(func)

    def __init__(self, db, collection):
        """ 初始化. """
        if self._mongo_client is None:
            raise Exception("mongo_client is None")
        self._db = db
        self._collection = collection
        self._cursor = self._mongo_client[db][collection]

    def new_cursor(self, db, collection):
        """ 创建新的游标.

        Args:
            :param db: 新的数据库名称.
            :param collection: 新的连接名称.

        Return:
            :return cursor: New cursor.
        """
        if self._mongo_client is None:
            raise Exception("mongo_client is None")
        cursor = self._mongo_client[db][collection]
        return cursor

    @forestall
    async def get_list(self, spec=None, fields=None, sort=None, skip=0, limit=99999, cursor=None):
        """ 批量获取数据
        Args:
            :param spec 查询条件
            :param fields 返回数据的字段
            :param sort 排序规则
            :param skip 查询起点
            :param limit 返回数据条数
            :param cursor 查询游标，如不指定默认使用self._cursor
        Return:
            :return datas: 字典集合.

        NOTE:
            必须传入limit，否则默认返回数据条数可能因为pymongo的默认值而改变
        """
        if not spec:
            spec = {}
        if not sort:
            sort = []
        if not cursor:
            cursor = self._cursor
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        spec[DELETE_FLAG] = {"$ne": True}
        datas = []
        result = cursor.find(spec, fields, sort=sort, skip=skip, limit=limit)
        async for item in result:
            datas.append(item)
        return datas, None

    @forestall
    async def find_one(self, spec=None, fields=None, sort=None, cursor=None):
        """ 查找单条数据
        Args:
            :param spec 查询条件
            :param fields 返回数据的字段
            :param sort 排序规则
            :param cursor 查询游标，如不指定默认使用self._cursor
        Return:
            :return data: 返回数据字典或者None.
        """
        data, e = await self.get_list(spec, fields, sort, limit=1, cursor=cursor)
        if e:
            return None, e
        if data:
            return data[0], None
        else:
            return None, None

    @forestall
    async def count(self, spec=None, cursor=None):
        """ 计算数据条数

        Args:
            :param spec 查询条件

            :param cursor 查询游标，如不指定默认使用self._cursor
        Return:
            :return n 返回查询的条数

        """
        if not cursor:
            cursor = self._cursor
        if not spec:
            spec = {}
        spec[DELETE_FLAG] = {"$ne": True}
        n = await cursor.count_documents(spec)
        return n, None

    @forestall
    async def insert(self, docs, cursor=None):
        """ 插入数据
        Args:
            :param docs 插入数据 dict或list
            :param cursor 查询游标，如不指定默认使用self._cursor
        Return:
            :return ret_ids 插入数据的id列表
        """
        if not cursor:
            cursor = self._cursor
        docs_data = copy.deepcopy(docs)
        is_one = False
        if not isinstance(docs_data, list):
            docs_data = [docs_data]
            is_one = True
        result = await cursor.insert_many(docs_data)
        if is_one:
            return result.inserted_ids[0], None
        else:
            return result.inserted_ids, None

    @forestall
    async def update(self, spec, update_fields, upsert=False, multi=False, cursor=None):
        """ 更新
        Args:
            :param spec 更新条件
            :param update_fields 更新字段
            :param upsert 如果不满足条件，是否插入新数据
            :param multi 是否批量更新
            :param cursor 查询游标，如不指定默认使用self._cursor
        Return:
            :return modified_count 更新数据条数
        """
        if not cursor:
            cursor = self._cursor
        update_fields = copy.deepcopy(update_fields)
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        if not multi:
            result = await cursor.update_one(spec, update_fields, upsert=upsert)
            return result.modified_count, None
        else:
            result = await cursor.update_many(spec, update_fields, upsert=upsert)
            return result.modified_count, None

    @forestall
    async def delete(self, spec, cursor=None):
        """ 软删除

        Args:
            :param spec 删除条件
            :param cursor 查询游标，如不指定默认使用self._cursor

        Return:
            :return delete_count 删除数据的条数
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        update_fields = {"$set": {DELETE_FLAG: True}}
        delete_count = await self.update(spec, update_fields, multi=True, cursor=cursor)
        return delete_count, None

    @forestall
    async def remove(self, spec, multi=False, cursor=None):
        """ 彻底删除数据

        Args:
            :param spec 删除条件
            :param multi 是否全部删除
            :param cursor 查询游标，如不指定默认使用self._cursor

        Return:
            :return deleted_count 删除数据的条数
        """
        if not cursor:
            cursor = self._cursor
        if not multi:
            result = await cursor.delete_one(spec)
            return result.deleted_count, None
        else:
            result = await cursor.delete_many(spec)
            return result.deleted_count, None

    @forestall
    async def distinct(self, key, spec=None, cursor=None):
        """ distinct查询

        Args:
            :param key 查询的key
            :param spec 查询条件
            :param cursor 查询游标，如不指定默认使用self._cursor

        Return:
            :return result 过滤结果list
        """
        if not spec:
            spec = {}
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.distinct(key, spec)
        return result, None

    @forestall
    async def find_one_and_update(self, spec, update_fields, upsert=False, return_document=False, fields=None,
                                  cursor=None):
        """ 查询一条指定数据，并修改这条数据

        Args:
            :param spec 查询条件
            :param update_fields 更新字段
            :param upsert 如果不满足条件，是否插入新数据，默认False
            :param return_document 返回修改之前数据或修改之后数据，默认False为修改之前数据
            :param fields 需要返回的字段，默认None为返回全部数据
            :param cursor 查询游标，如不指定默认使用self._cursor

        Return:
            :return result 修改之前或之后的数据
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.find_one_and_update(spec, update_fields, projection=fields, upsert=upsert,
                                                  return_document=return_document)
        # if result and "_id" in result:
        #    result["_id"] = str(result["_id"])
        return result, None

    @forestall
    async def find_one_and_delete(self, spec, fields=None, cursor=None):
        """ 查询一条指定数据，并删除这条数据

        Args:
            :param spec 删除条件
            :param fields 需要返回的字段，默认None为返回全部数据
            :param cursor 查询游标，如不指定默认使用self._cursor

        Return:
            result: 删除之前的数据.
        """
        if not cursor:
            cursor = self._cursor
        spec[DELETE_FLAG] = {"$ne": True}
        if "_id" in spec:
            spec["_id"] = self._convert_id_object(spec["_id"])
        result = await cursor.find_one_and_delete(spec, projection=fields)
        return result, None

    @forestall
    async def create_index(self, fields, cursor=None):
        """ Creates an index on this collection.

        Args:
            fields: The fields to be create.
            cursor: Query cursor, default is `self._cursor`.

        Return:
            result: Result.
        """
        if not cursor:
            cursor = self._cursor
        param = []
        for (k, v) in fields.items():
            if v == 1:
                x = (k, pymongo.ASCENDING)
            else:
                x = (k, pymongo.DESCENDING)
            param.append(x)
        result = await cursor.create_index(param, background=True)
        return result, None

    def _convert_id_object(self, origin):
        """ 将字符串的_id转换成ObjectId类型
        """
        if isinstance(origin, str):
            return ObjectId(origin)
        elif isinstance(origin, (list, set)):
            return [ObjectId(item) for item in origin]
        elif isinstance(origin, dict):
            for key, value in origin.items():
                origin[key] = self._convert_id_object(value)
        return origin

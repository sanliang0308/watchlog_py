#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import ast
import json
import logging
import time
import datetime

logger = logging.getLogger('watchlog_py.pluging.sender')


class Upload(object):
    """
    处理数据通道
    消费数据。上传数据至数据库
    """

    def __init__(self, conf: dict):
        """初始化
        conf: 配置项字典

        "schema": self.schema,
                    "table": self.tables_real,
                    "deviceid": self.deviceids,
        """
        self.url = conf.get('sender', dict())['url']
        self.schema = conf.get('sender', dict())['schema']
        self.table = conf.get('sender', dict())['table']
        self.deviceid = conf.get('sender', dict())['deviceid']

        self.device_data_path = conf.get('collector', dict())['device_data_path']
        # 获取文件编码配置
        self.encoding = conf.get('collector', dict())['encoding']

    def read_first_line(self):
        """
        读取第一行，用于消费
        """
        with open(self.device_data_path, 'r', encoding=self.encoding) as fw:
            return fw.readline()

    def delete_first_line(self):
        """
        对已经消费的数据，删除第一行
        """

        with open(self.device_data_path, 'r', encoding=self.encoding) as old_file:
            with open(self.device_data_path, 'r+', encoding=self.encoding) as new_file:

                current_line = 0
                # 定位到需要删除的行
                while current_line < (1 - 1):
                    old_file.readline()
                    current_line += 1
                # 当前光标在被删除行的行首，记录该位置
                seek_point = old_file.tell()

                # 设置光标位置
                new_file.seek(seek_point, 0)

                # 读需要删除的行，光标移到下一行行首
                old_file.readline()

                # 被删除行的下一行读给 next_line
                next_line = old_file.readline()

                # 连续覆盖剩余行，后面所有行上移一行
                while next_line:
                    new_file.write(next_line)
                    next_line = old_file.readline()

                # 写完最后一行后截断文件，因为删除操作，文件整体少了一行，原文件最后一行需要去掉
                new_file.truncate()

    def gendate(self):

        # return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f2')

    def payload(self):

        # print(self.read_device_data(), type(self.read_device_data()))

        res = self.read_first_line()

        if len(res) > 0:

            payload = ast.literal_eval(res)

            mqttinfo = {"timestamp": self.gendate(),
                        "schema": self.schema,
                        "table": self.table,
                        "deviceid": self.deviceid,
                        "fields": payload
                        }
        else:
            mqttinfo = {}

        return mqttinfo

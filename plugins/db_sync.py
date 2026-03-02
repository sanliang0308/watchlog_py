#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import time
import json
import sqlite3
import asyncio
from datetime import datetime

logger = logging.getLogger('watchlog_py.plugin.db_sync')


class Db_sync(object):
    """
    数据库同步插件
    定时读取第三方数据库文件
    """

    def __init__(self, conf: dict):
        """
        初始化
        conf: 配置项字典
        """
        self.db_sync_path = conf.get('db_sync', dict()).get('db_sync_path', '')
        self.db_sync_isEnable = conf.get('db_sync', dict()).get('db_sync_isEnable', False)
        self.db_sync_interval_time = conf.get('db_sync', dict()).get('db_sync_interval_time', 3600)
        self.db_sync_query = conf.get('db_sync', dict()).get('db_sync_query', '')
        
        # 从collector配置中获取数据文件路径和最大行数
        self.device_data_path = conf.get('collector', dict()).get('device_data_path', './data/data.txt')
        self.MAX_LINES = conf.get('collector', dict()).get('MAX_LINES', 200)
        self.encoding = conf.get('collector', dict()).get('encoding', 'utf-8')
        
        # 从mqtt配置中获取发布主题
        self.pub_topics = conf.get('mqtt', dict()).get('pub_topics', [])
        
        # logger.info(f"数据库同步插件初始化成功，路径: {self.db_sync_path}, 启用状态: {self.db_sync_isEnable}, 间隔时间: {self.db_sync_interval_time}秒")
        # logger.debug(f"数据库查询语句: {self.db_sync_query}")
        # logger.debug(f"数据文件路径: {self.device_data_path}, 最大行数: {self.MAX_LINES}, 编码: {self.encoding}")
        # logger.debug(f"MQTT发布主题: {self.pub_topics}")

    def read_database(self):
        """
        读取数据库文件
        """
        try:
            if not os.path.exists(self.db_sync_path):
                logger.error(f"数据库文件不存在: {self.db_sync_path}")
                return None
            
            # 连接SQLite数据库
            conn = sqlite3.connect(self.db_sync_path)
            cursor = conn.cursor()
            
            # 使用配置文件中的SQL查询语句
            if self.db_sync_query:
                cursor.execute(self.db_sync_query)
                rows = cursor.fetchall()
                
                # 获取查询结果的列名
                column_names = [description[0] for description in cursor.description]
                
                # 格式化数据
                data = []
                for row in rows:
                    row_data = dict(zip(column_names, row))
                    data.append(row_data)
                    # print(f"查询结果: {row_data}")  # 打印查询结果
                
                logger.info(f"成功执行SQL查询，获取到 {len(data)} 条记录")
            
            conn.close()
            return data
            
        except Exception as e:
            logger.error(f"读取数据库文件时发生错误: {e}")
            return None

    def add_data(self, msg: str):
        """
        追加一行数据；若行数 > MAX_LINES，则删除最早的一行。
        """
        try:
            # 确保文件所在目录存在
            os.makedirs(os.path.dirname(self.device_data_path), exist_ok=True)
            
            lines = []
            if os.path.exists(self.device_data_path):
                # 只读取最后MAX_LINES行，避免处理整个文件
                with open(self.device_data_path, 'r', encoding=self.encoding) as f:
                    # 使用deque来高效获取最后MAX_LINES行
                    from collections import deque
                    lines = list(deque(f, maxlen=self.MAX_LINES))

            # 2️⃣ 追加最新一行
            lines.append(msg + '\n')

            # 3️⃣ 若超限，截取最后 MAX_LINES 行 (deque已经处理了这个情况，但为了保险再检查一次)
            if len(lines) > self.MAX_LINES:
                lines = lines[-self.MAX_LINES:]

            # 4️⃣ 覆写文件
            with open(self.device_data_path, 'w', encoding=self.encoding) as f:
                f.writelines(lines)

            logger.debug(f'add_data: 写入成功，当前缓存行数={len(lines)}')
        except Exception as e:
            logger.error(f'add_data: 写入失败 -> {e}')

    async def sync_database(self):
        """
        定时同步数据库
        """
        while True:
            try:
                if self.db_sync_isEnable and self.db_sync_path:
                    data = self.read_database()
                    if data:
                        # 参考collector.py的格式，为每条记录添加元数据
                        try:
                            # 格式化消息，添加详细的元数据
                            mqtt_topic = self.pub_topics[0] + '/db_sync/' + os.path.basename(self.db_sync_path) if self.pub_topics else ''
                            msg = {
                                "Mqtt_Topic": mqtt_topic,
                                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                "database_path": self.db_sync_path,
                                "contents": data
                            }
                            # 将消息转换为字符串，与collector.py保持一致
                            msg_str = str(msg)
                            # 写入数据到文件，使用与collector.py相同的方法名
                            self.add_data(msg_str)
                            
                            logger.debug('记录数据文件成功: {data}'.format(data=msg))
                        except Exception as e:
                            logger.error('记录数据文件失败: {data}'.format(data=e))
                                                    
                        logger.info(f"数据库同步完成，成功写入 {len(data)} 条记录到 {self.device_data_path}")
                
                # 等待下一次同步
                await asyncio.sleep(self.db_sync_interval_time)
                
            except Exception as e:
                logger.error(f"数据库同步过程中发生错误: {e}")
                # 发生错误时，短暂等待后重试
                await asyncio.sleep(60)


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import platform
import re
from watchgod import awatch, Change, AllWatcher,RegExpWatcher,watch
import logging
import toml
import asyncio
import json
import time
import uuid
from datetime import datetime
import subprocess
import socket
import struct
import queue
import urllib.request
import os
import shutil

import psutil
import platform
import socket

import os, random, glob
from shutil import copyfile, copy, rmtree
from utils.log_wrapper import setup_logging

from plugins.sender import Upload
from plugins.mqttclient import MQTTclient
from plugins.collector import Collector
from plugins.db_sync import Db_sync


# 加载配置文件
confile = 'conf/config.toml'
config = toml.load(confile)

log_conf = config.get('log', dict())
setup_logging(log_conf)
logger = logging.getLogger('watchlog_py.file-watch')

watch_path = config.get('collector', dict())['watch_path']
scan_cycle = config.get('collector', dict()).get('Scan_Cycle', 0.01)  # 扫描周期

# 创建对象
collector_er = Collector(conf=config)

uploader = Upload(conf=config)

# 创建数据库同步对象
db_sync_er = Db_sync(conf=config)
#
mqttobj = MQTTclient(conf=config)

# EMQX配置
mqtt_conf = config.get('mqtt', dict())
publish_cycle = mqtt_conf.get('publish_cycle', 0.01)  # 发布数据的循环间隔时间

async def monitor_file_changes():
    """
    监控主线程，解析数据写入data文件
    """
    ignore_regexes = [
        re.compile(r'.*\.jpg$'),  # 忽略所有 .tmp 文件
        re.compile(r'.*\.log$'),  # 忽略所有 .log 文件
        re.compile(r'.*__pycache__.*'),  # 忽略所有 __pycache__ 目录内容
        re.compile(r'.*\.pyc$'),  # 忽略所有 .pyc 文件
        re.compile(r'\.git/'),  # 忽略 .git 目录下的所有内容（注意路径）
        re.compile(r'.*/temp_[^/]*'),  # 忽略任何名为 temp_xxx 的文件或目录
    ]
    # async for changes in watch(watch_path, watcher_cls=RegExpWatcher, watcher_kwargs=dict(re_files=r'^.*(\.mp3)$')):
    async for changes in awatch(watch_path):
        for item in changes:
            await asyncio.sleep(scan_cycle)
            logger.debug('文件变化 : {data}'.format(data=item))
            filepath = item[1].replace('\\', '/')

            collector_er.watcher(item, filepath)

async def publish_pending_data(mqclient):
    """
    消费数据，将处理好的数据发送至 mqtt broker
    """
    while True:
        # try:
        await asyncio.sleep(publish_cycle)
        # 获取文件最后一行
        mqttinfo = uploader.payload()

        if len(mqttinfo) > 0:
            # 发布的消息质量是 1
            try:
                # 从mqttinfo的fields中获取主题名
                topic = mqttinfo.get('fields', {}).get('Mqtt_Topic', mqtt_conf['pub_topics'][0]) if mqttinfo else mqtt_conf['pub_topics'][0]
                result = mqclient.publish(topic, json.dumps(mqttinfo), qos=1)
                status = result[0]
                if status == 0:
                    logger.debug(
                        '发布消息成功: 主题：{topics},数据:{data}'.format(topics=mqtt_conf['pub_topics'][0],
                                                                         data=mqttinfo))
                    #   如果发布数据没有异常，删除文件第一行
                    uploader.delete_first_line()

            except Exception as e:
                logger.error('发布消息失败: {data}'.format(data=e))
                await asyncio.sleep(10)
                logger.error('10s后重试')

def get_local_ip() -> str:
    """返回首个非 127.* 的本机 IP。如果都失败则回退 127.0.0.1。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 连接到一个不会真的用到的地址，只为获取本地 IP
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


async def check_for_updates():
    """
    检查并下载程序更新（支持定时更新）
    """
    app_config = config.get('app', dict())
    update_enabled = app_config.get('update_enabled', False)
    update_url = app_config.get('update_url', '')
    main_exe_path = app_config.get('main_exe_path', './main.exe')
    update_interval = app_config.get('update_interval', 3600)  # 默认3600秒（1小时）
    
    if not update_enabled:
        logger.debug('更新功能已关闭')
        return
    
    if not update_url:
        logger.error('更新地址未配置')
        return
    
    logger.info(f'定时更新功能已开启，更新间隔: {update_interval}秒')
    
    while True:
        try:
            logger.info(f'开始检查更新: {update_url}')
            
            # 下载更新文件到临时位置
            temp_path = main_exe_path + '.tmp'
            urllib.request.urlretrieve(update_url, temp_path)
            logger.info(f'更新文件下载成功: {temp_path}')
            
            # 备份当前主程序
            backup_path = main_exe_path + '.bak'
            if os.path.exists(main_exe_path):
                shutil.copy2(main_exe_path, backup_path)
                logger.info(f'主程序备份成功: {backup_path}')
            
            # 替换主程序
            shutil.copy2(temp_path, main_exe_path)
            logger.info(f'主程序更新成功: {main_exe_path}')
            
            # 删除临时文件
            os.remove(temp_path)
            logger.info('临时文件已删除')
            
            # 重启程序（可选）
            logger.info('程序更新完成，建议重启程序以应用更新')
            
        except Exception as e:
            logger.error(f'更新失败: {e}')
            # 清理临时文件
            temp_path = main_exe_path + '.tmp'
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.info('临时文件已清理')
        
        # 等待下一次更新检查
        logger.debug(f'等待{update_interval}秒后进行下一次更新检查')
        await asyncio.sleep(update_interval)

async def send_system_status():
    """
    每指定时间发布一次：设备 ID、时间戳、CPU/内存占用、IP、系统信息。
    """
    heartbeat_cycle = config.get('collector', dict()).get('Heartbeat_Cycle', 300)  # 默认300秒
    while True:
        heartbeat = {
            "deviceid": uploader.deviceid,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "os": platform.system(),
            "os_version": platform.version(),
            "cpu_percent": psutil.cpu_percent(interval=None),  # 全核平均
            "mem_percent": psutil.virtual_memory().percent,
            "ip": get_local_ip()
        }

        try:
            rc, _ = mqclient.publish(
                mqtt_conf['pub_topics'][1],
                json.dumps(heartbeat),
                qos=mqtt_conf['qos']
            )
            if rc == 0:
                logger.debug(f'心跳发送成功 → {heartbeat}')
            else:
                logger.error(f'心跳发送失败 rc={rc}，5 s 重试')
                await asyncio.sleep(5)
                continue  # 立即重试连接
        except Exception as e:
            logger.error(f'心跳异常：{e}；5 s 后重试')
            await asyncio.sleep(5)
            continue

        await asyncio.sleep(heartbeat_cycle)  # 使用配置的心跳时间


if __name__ == "__main__":
    logger.debug(f'主程序启动!')
    
    # 检查更新
    app_config = config.get('app', dict())
    if app_config.get('update_enabled', False):
        # 在后台线程中检查更新，不阻塞主程序
        import threading
        update_thread = threading.Thread(target=lambda: asyncio.run(check_for_updates()))
        update_thread.daemon = True
        update_thread.start()
    
    mqclient = mqttobj.genMQTTClient()
    mqclient.username_pw_set(mqttobj.username, mqttobj.password)

    mqclient.on_connect = mqttobj.on_connect
    mqclient.on_message = mqttobj.on_message

    mqclient.connect_async(mqttobj.host, mqttobj.port, mqttobj.keepalive)
    mqclient.loop_start()

    looper = asyncio.get_event_loop()
    
    # 根据collector.enabled配置决定是否启动文件监控任务
    collector_enabled = config.get('collector', dict()).get('enabled', True)
    if collector_enabled:
        looper.create_task(monitor_file_changes())
    else:
        logger.info('收集器功能已关闭')
        
    looper.create_task(publish_pending_data(mqclient))
    looper.create_task(send_system_status())
    
    # 启动数据库同步任务
    if db_sync_er.db_sync_isEnable:
        looper.create_task(db_sync_er.sync_database())

    # mqclient.loop_start()
    looper.run_forever()

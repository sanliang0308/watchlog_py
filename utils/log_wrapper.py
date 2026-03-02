#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: logtool.py
Author: YJ
Email: yj1516268@outlook.com
Created Time: 2020-11-02 15:10:19

Description: 配置logger
"""
from sys import version_info
import logging
import logging.handlers
import os
import toml


def setup_logging(conf):
    """Initialize the logging module settings

    :conf: initialize parameters
    :return: logger

    """
    level = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }

    console = conf['console']  # console output?
    console_level = conf['console_level']  # console log level
    file = conf['file']  # file output?
    file_level = conf['file_level']  # file log level
    logfile = conf['log_file']  # log file save position
    max_size = conf['max_size']  # size of each log file
    backup_count = conf['backup_count']  # count of log files
    log_format = conf['format']  # log format

    logger = logging.getLogger('watchlog_py')
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')

    if file:
        # 如果 log 文本不存在，创建文本
        dir_path = os.path.dirname(logfile)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # 实例化一个 rotate file 的处理器，让日志文件旋转生成
        fh = logging.handlers.RotatingFileHandler(filename=logfile,
                                                  mode='a',
                                                  maxBytes=max_size,
                                                  backupCount=backup_count,
                                                  encoding='utf-8')
        fh.setLevel(level[file_level])
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if console:
        # 实例化一个流式处理器，将日志输出到终端
        ch = logging.StreamHandler()
        ch.setLevel(level[console_level])
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def get_conf(conf_file_path=r'conf/config.toml'):
    """read toml conf file for latter use.


    :param conf_file_path: absolute path of conf file.
    :return:a dict contains configured infomation.
    """
    if version_info[0] == 3:
        with open(conf_file_path, encoding='utf-8') as conf_file:
            config = toml.loads(conf_file.read())
            # print(config)
    else:
        with open(conf_file_path) as conf_file:
            config = toml.loads(conf_file.read())

    return config

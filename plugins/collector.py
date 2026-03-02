# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import ast
import json
import logging
import time
import os
import struct
import datetime
# from watchgod import awatch, Change, AllWatcher
# from datetime import datetime
from watchgod import awatch, Change, AllWatcher
import zipfile
import fnmatch

logger = logging.getLogger('watchlog_py.pluging.collector')


class Collector(object):
    """
    处理数据通道
    收集数据，存入本地文件；
    """

    def __init__(self, conf: dict):
        """初始化
        conf: 配置项字典
        """

        self.watch_path = conf.get('collector', dict())['watch_path']
        self.target_files = conf.get('collector', dict())['target_files']

        self.records_path = conf.get('collector', dict())['records_path']
        self.device_data_path = conf.get('collector', dict())['device_data_path']

        self.FILE_PUSH_LIMIT = conf.get('collector', dict())['FILE_PUSH_LIMIT']
        self.MAX_LINES = conf.get('collector', dict())['MAX_LINES']
        self.MAX_RECORDS = conf.get('collector', dict())['MAX_RECORDS']
        # 增加配置，是否读取全部文件
        self.full_read = conf.get('collector', dict())['full_read']
        self.extract_dir = conf.get('collector', dict())['extract_dir']
        # 获取文件编码配置
        self.encoding = conf.get('collector', dict())['encoding']
        # 获取MQTT发布主题
        self.mqtt_conf = conf.get('mqtt', dict())
        self.pub_topics = self.mqtt_conf.get('pub_topics', [])

    def read_records(self):
        """
        读取配置文件，返回json字典，如不存在，新建配置文件
        """

        try:
            with open(self.records_path, 'r', encoding=self.encoding) as fw:
                history_records = json.load(fw)
            return history_records
        except Exception as e:
            print(e)
            msg = {"records": []}
            with open(self.records_path, 'w', encoding=self.encoding) as fw:
                json.dump(msg, fw, ensure_ascii=False)
            with open(self.records_path, 'r', encoding=self.encoding) as fw:
                history_records = json.load(fw)
            return history_records

    def update_records(self, filepath: str, start_bit: int) -> None:
        """
        更新文件的 start_bit；并把 records 列表裁剪到 MAX_RECORDS。
        """
        # ① 读文件 —— 失败就返回空列表
        try:
            with open(self.records_path, 'r', encoding=self.encoding) as f:
                records = json.load(f).get('records', [])
        except (FileNotFoundError, json.JSONDecodeError):
            records = []

        # ② 去重：删除旧条目
        records = [r for r in records if r.get('file') != filepath]

        # ③ 插入最新条目到列表头
        records.insert(0, {'file': filepath, 'start_bit': start_bit})

        # ④ 截断到 MAX_RECORDS
        records = records[:self.MAX_RECORDS]

        # ⑤ 写回文件
        with open(self.records_path, 'w', encoding=self.encoding) as f:
            json.dump({'records': records}, f, ensure_ascii=False, indent=4)

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

    def add_records(self, file_info):
        """
        更新记录文件，确保记录数量不超过 MAX_RECORDS

        参数:
            file_info (dict): 包含文件记录的字典
        """
        try:
            # 确保记录数量不超过 MAX_RECORDS
            if len(file_info.get('records', [])) > self.MAX_RECORDS:
                file_info['records'] = file_info['records'][:self.MAX_RECORDS]
                logger.info(f"记录数量超过限制 {self.MAX_RECORDS}，已截断")

            # 使用配置的编码写入文件
            with open(self.records_path, 'w', encoding=self.encoding) as fw:
                json.dump(file_info, fw, indent=4, ensure_ascii=False)

            logger.debug(f"records.json 文件更新成功，当前记录数: {len(file_info['records'])}")
        except Exception as e:
            logger.error(f"更新 records.json 文件时发生错误: {e}")
            # 尝试创建基本文件作为回退
            try:
                with open(self.records_path, 'w', encoding=self.encoding) as fw:
                    json.dump({"records": []}, fw)
                logger.warning("已创建初始 records.json 文件")
            except Exception as fallback_error:
                logger.critical(f"无法创建 records.json 文件: {fallback_error}")

    def read_new_lines(self, filepath, rows):
        """
        输入：
            filepath (str): 文件路径
            rows (int): 当前行数
        输出：
            dict: 包含时间戳、文件名、新行数、更新内容的字典
        """
        # 参数验证
        if not isinstance(filepath, str):
            logging.error("文件路径必须是字符串类型")
            raise TypeError("文件路径必须是字符串类型")
        if not isinstance(rows, int) or rows < 0:
            logging.error("行数必须是非负整数")
            raise ValueError("行数必须是非负整数")

        content = []
        new_rows = rows

        # 尝试使用 gbk 编码读取文件
        try:
            with open(filepath, 'r', encoding='gbk') as f:
                for line in f.readlines()[rows:]:
                    content.append(line)
                new_rows += len(content)
        except UnicodeDecodeError:
            # 如果 gbk 失败，尝试使用 utf-8 编码
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f.readlines()[rows:]:
                        content.append(line)
                    new_rows += len(content)
            except Exception as e:
                logging.error(f"无法使用 gbk 或 utf-8 编码读取文件: {e}")
                raise e
        except FileNotFoundError:
            logging.error(f"文件未找到: {filepath}")
            raise FileNotFoundError(f"文件未找到: {filepath}")
        except Exception as e:
            logging.error(f"读取文件时发生错误: {e}")
            raise e

        msg = {
            "time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "filename": filepath,
            "new_rows": new_rows,
            "updatecontent": content
        }

        logging.info(f"读取文件成功: {msg}")

        return msg

    def read_file_lines_count(self, file_path):
        """
        计算文件的行数，优先使用 gbk 编码，如果失败则使用 utf-8 编码。

        参数:
            file_path (str): 文件的路径。

        返回:
            int: 文件的总行数。
        """
        # 参数验证
        if not isinstance(file_path, str):
            logging.error("文件路径必须是字符串类型")
            raise TypeError("文件路径必须是字符串类型")
        if not os.path.isfile(file_path):
            logging.error(f"文件不存在: {file_path}")
            raise FileNotFoundError(f"文件不存在: {file_path}")

        time.sleep(0.01)  # 模拟延迟

        encodings_to_try = ['gbk', 'utf-8']
        count = 0

        for encoding in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=encoding) as fw:
                    buffer_size = 1024 * 8192  # 缓冲区大小
                    while True:
                        buffer = fw.read(buffer_size)
                        if not buffer:
                            break
                        count += buffer.count('\n')
                # 如果成功读取，退出循环
                break
            except UnicodeDecodeError:
                logging.warning(f"使用 {encoding} 编码读取文件失败，尝试下一个编码")
                if encoding == encodings_to_try[-1]:
                    logging.error("所有编码尝试失败，无法读取文件")
                    raise
            except FileNotFoundError:
                logging.error(f"文件未找到: {file_path}")
                raise
            except Exception as e:
                logging.error(f"读取文件时发生错误: {e}")
                raise

        return count

    def handle_data(self, payload):
        """
        新里程机文本数据，
        {'time': '2022-11-14 14:36:16', 'filename': 'D:/desktop/佳通/数据采集/04里程机/14-40-05_1 - 副本', 'new_rows': 1786, 'updatecontent': ['36\t1\t7\t1\t88:31:46\t24:0:0\t7:58:13\t16:1:46\t88:31:46\t24:0:0\t7:58:13\t16:1:46\t30.1\t9562.5\t600.9\t25.7\t50.4\t35.2\t36.7\t31.1\t28.6\t126.7\t0.0\t0.0\t600.9\t601.2\t596.6\t0.0\t0.0\t0.0\t0.0\t2676.611\t9562.500\t30.000\t0.000\t487.576\t0.000\t900.000\t900.000\t2676.611\t2022-05-24  08:04:52\t0.000000\t\t0.0\n']}

        """
        resss = []
        msg = payload['updatecontent']

        try:
            for row in msg:
                one_row = row.split('\t')

                logger.debug('one_row: {data}'.format(data=one_row))

                res = one_row
                resss.append(res)
        except Exception as e:
            logger.error('解析错误: {data}'.format(data=e))

        return resss

    # --------------------------- Zip 处理 ---------------------------
    def handle_zip_file(self, zip_path):
        """解压 zip 文件到 extract_dir 下的同名子目录，并返回所有解压后的文件路径列表。"""
        try:
            base_name = os.path.splitext(os.path.basename(zip_path))[0]
            target_dir = os.path.join(self.extract_dir, base_name)
            os.makedirs(target_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = zf.namelist()
                zf.extractall(target_dir)
            files = [os.path.join(target_dir, n) for n in names if not n.endswith('/')]
            logger.info(f'zip 解压完成: {zip_path} -> {target_dir}, 共 {len(files)} 个文件')
            # return files
        except Exception as e:
            logger.error(f'解压 zip 文件失败: {zip_path} -> {e}')
            # return []

    def watcher(self, item, filepath):
        """

        """
        try:

            # 首先检查文件是否与目标模式匹配
            filename = os.path.basename(filepath)
            match_found = False
            for pattern in self.target_files:
                if fnmatch.fnmatch(filename, pattern):
                    match_found = True
                    break
            
            if not match_found:
                # 文件不在目标范围内，跳过处理
                return

            # 文件修改逻辑
            if item[0] == Change.modified:

                if filepath.lower().endswith('.zip'):
                    extracted_files = self.handle_zip_file(filepath)
                    pass

                file_stats = os.stat(filepath)
                inode = file_stats.st_ino

                file_size = self.read_file_lines_count(filepath)
                logger.debug('文件修改：filename: {data}'.format(data=filepath))
                
                if match_found:
                    # 调用文件读取模块，从配置文件中读取文件当前的行数，从当前行数开始，读取以后部分，以list形式输出；
                    # 读取配置文件
                        history_records = self.read_records()['records']

                        # 文件标识位，初始0，认为不存在记录，1，存在记录
                        set_file_exis = 0
                        for record in history_records:
                            if filepath == record['file']:
                                set_file_exis = 1

                                if self.full_read:
                                    row = 0
                                else:
                                    # 开始字节位，
                                    row = record['start_bit']
                                # 读取数据流、或者新增的行数
                                payload = self.read_new_lines(filepath, row)

                                contents = self.handle_data(payload)

                                logger.debug('读取数据文件内容: {data}'.format(data=payload))

                                # 更新记录文件
                                self.update_records(filepath, payload['new_rows'])

                                try:
                                    # for content in contents:
                                    filename = os.path.basename(filepath)
                                    mqtt_topic = self.pub_topics[0] + '/' + filename if self.pub_topics else ''
                                    msg = {
                                        "Mqtt_Topic": mqtt_topic,
                                        "time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "filepath": filepath,
                                        'inode': file_stats.st_ino,
                                        'size': file_stats.st_size,
                                        'modification_time': datetime.datetime.fromtimestamp(
                                            file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                                        'access_time': datetime.datetime.fromtimestamp(
                                            file_stats.st_atime).strftime('%Y-%m-%d %H:%M:%S'),
                                        'creation_time': datetime.datetime.fromtimestamp(
                                            file_stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                                        "new_startbit": row,
                                        "contents": contents
                                           }
                                    self.add_data(str(msg))

                                    logger.debug('记录数据文件成功: {data}'.format(data=msg))

                                except Exception as e:
                                    logger.error('记录数据文件成功失败: {data}'.format(data=e))

                                break
                        if set_file_exis == 0:
                            # 追加新文件名到record.json目录中

                            file_info = self.read_records()

                            # 插入第一行，减少遍历
                            file_info['records'].insert(0, {"file": filepath, "start_bit": file_size, "inode": inode})

                            self.add_records(file_info)
                            logger.debug('records.json文件更新成功: {data} '.format(
                                data={"file": filepath, "start_bit": file_size, "inode": inode}))

            # 文件创建逻辑
            elif item[0] == Change.added:

                if filepath.lower().endswith('.zip'):
                    extracted_files = self.handle_zip_file(filepath)
                    pass

                file_stats = os.stat(filepath)
                inode = file_stats.st_ino

                file_size_rows = self.read_file_lines_count(filepath)

                file_size_bytes = os.path.getsize(filepath)

                logger.debug('文件新增：filename: {data}'.format(data=filepath))
                # 判断文件新增，后将文件名称和行数，新增到json文件中，
                
                if match_found:
                    file_info = self.read_records()
                    # 插入第一行，减少遍历
                    file_info['records'].insert(0, {"file": filepath, "start_bit": file_size_rows, "inode": inode})
                    self.add_records(file_info)

                    logger.debug('records.json文件更新成功: {data} '.format(
                        data={"file": filepath, "start_bit": file_size_rows, "inode": inode}))

                    if file_size_bytes < self.FILE_PUSH_LIMIT:
                        # 把整个文件作为一次“新增”来处理
                        payload = self.read_new_lines(filepath, 0)
                        all_contents = self.handle_data(payload)

                        # 构造MQTT主题：pub_topics第一个元素 + '/' + 文件名
                        mqtt_topic = self.pub_topics[0] + '/' + filename if self.pub_topics else ''
                        msg = {
                            "time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "filepath": filepath,
                            'inode': file_stats.st_ino,
                            'size': file_stats.st_size,
                            'modification_time': datetime.datetime.fromtimestamp(file_stats.st_mtime).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            'access_time': datetime.datetime.fromtimestamp(file_stats.st_atime).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            'creation_time': datetime.datetime.fromtimestamp(file_stats.st_ctime).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            "new_startbit": 0,
                            "contents": all_contents,
                            "Mqtt_Topic": mqtt_topic
                        }
                        try:
                            self.add_data(str(msg))
                            logger.debug(f'已推送整文件内容：{filepath}')
                        except Exception as e:
                            logger.error(f'推送失败：{e}')

        except Exception as e:
            logger.error('监控数据主线程错误!: {data}'.format(data=e))

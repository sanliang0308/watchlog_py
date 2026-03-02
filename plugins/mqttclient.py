# -*-coding:utf-8-*-
import queue
import json
import logging
import paho.mqtt.client as mqtt
import random
import uuid
import string

logger = logging.getLogger('watchlog_py.pluging.mqttclient')

# 定义队列OBJ对象
queobj = {
    "topic": None,
    "msg": None
}
waitque = queue.Queue(0)


class MQTTclient(object):
    def __init__(self, conf: dict):
        '''
        初始化，从配置文件读取MQTT参数
        '''
        try:

            self.host = conf.get('mqtt', dict())['host']
            self.port = conf.get('mqtt', dict())['port']
            self.client_id = conf.get('mqtt', dict())['client_id'] + '_' + ''.join(
                random.choices(string.ascii_letters + string.digits, k=6))
            # str(uuid.uuid4()).split('-')[0]
            self.clean_session = conf.get('mqtt', dict())['clean_session']
            self.keepalive = conf.get('mqtt', dict())['keepalive']
            self.username = conf.get('mqtt', dict())['username']
            self.password = conf.get('mqtt', dict())['password']
            self.qos = conf.get('mqtt', dict())['qos']
            self.sub_topics = conf.get('mqtt', dict())['sub_topics']
            self.pub_topics = conf.get('mqtt', dict())['pub_topics']

        except Exception as e:
            logger.error('MQTT组件初始化失败: {data}'.format(data=e))

    # 生成MQTT客户端
    def genMQTTClient(self):

        self.client = mqtt.Client(client_id=self.client_id, clean_session=False)

        # 连接丢失后让 Paho 自动指数回连（1s → … → 60s）
        self.client.reconnect_delay_set(min_delay=2, max_delay=60)
        # 出错时不抛异常，由发布端 rc 值自行判断
        # self.client.enable_logger(logger)

        return self.client

    # 定义队列OBJ对象

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.debug('MQTT连接成功-> {data}'.format(data=rc))
        # for topic in subtopics:
        #     mqclient.subscribe(topic)

    def on_message(self, client, userdata, msg):
        try:
            msgstr = str(msg.payload, 'utf-8')

            queobj['topic'] = msg.topic
            queobj['msg'] = msgstr
            waitque.put(json.dumps(queobj))
        except Exception as e:
            logger.error('获取msg失败 : {data}'.format(data=e))

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.error('MQTT连接断开 : {data}'.format(data=rc))

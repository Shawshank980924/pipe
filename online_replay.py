import datetime
from kafka import KafkaConsumer,KafkaAdminClient, KafkaProducer
import json
import time
import sys
import cls_pb2 as pb2
import threading
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# 创建日志处理器（Handler）并设置日志级别
handler = TimedRotatingFileHandler(filename='logs/ethlog', when='midnight', interval=1, backupCount=7)
handler.suffix = '%Y-%m-%d.log'  # 日志文件名格式，例如：2023-08-16.log

# 创建格式化器（Formatter）并设置格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# 将处理器添加到 Logger
logger.addHandler(handler)

dic = {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
        "0xb8c77482e45f1f44de1745f52c74426c631bdd52": "BNB",
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "UNI",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
        "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",
        "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0": "MATIC",
        "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39": "HEX",
        "0x4fabb145d64652a948d72533023f6e7a623c7c53": "BUSD",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
        "0x63d958d765f5bd88efdbd8afd32445393b24907f": "ACA",
        "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "AAVE",
        "0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359": "SAI",
        "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
        "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": "SHIB",
        "0x6f259637dcd74c767781e37bc6133cd6a68aa161": "HT",
        "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": "MKR",
        "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b": "CRO",
        "0xc00e94cb662c3520282e6f5717214004a7f26888": "COMP",
        "0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3": "LEO",
        "0x956f47f50a910163d8bf957cf5846d573e7f87ca": "FEI"}
def start_consumer(end_time):
    # p1 = KafkaProducer(bootstrap_servers='server-1:30812',
    #                      security_protocol='SASL_PLAINTEXT',
    #                      sasl_mechanism='SCRAM-SHA-512',
    #                      sasl_plain_username='admin',
    #                      sasl_plain_password='starryDev',
    #                      api_version=(2,8,2),acks='all')
    p1 = KafkaProducer(bootstrap_servers='10.75.75.200:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2),acks='all')
    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    # name = "ETH_"+current_time
    name = "ETH_test"
    print(name)
    print(f'consume {name}\n')
    consumer = KafkaConsumer(
        name,
        # bootstrap_servers='server-1:30812',
        #                  security_protocol='SASL_PLAINTEXT',
        #                  sasl_mechanism='SCRAM-SHA-512',
        #                  sasl_plain_username='admin',
        #                  sasl_plain_password='starryDev',
        #                  api_version=(2,8,2),
        bootstrap_servers='10.75.75.200:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2),
        auto_offset_reset='earliest',
        group_id="eth-consumer"
        )
    running= True  # 声明使用全局变量
    while running:
        # print('running')
        # 通过设置超时时间来等待新消息
        records = consumer.poll(timeout_ms=500)  # 每次轮询等待500毫秒

        if len(records)==0:
            # 未收到新消息，进行外部判断
            if datetime.datetime.now() >= end_time:
                running = False
            # print('no records')
            logger.info("no records")
            time.sleep(2)
        else:
            for message_list in records.values():
                for record in message_list:
                    # 提取消息记录
                    value = record.value
                    key = record.key
                    offset = record.offset
                    topic = record.topic
                    partition = record.partition
                    # print(pb2.StreamField.FromString(value))
                    from_ =  pb2.StreamField.FromString(value).from_
                    to = pb2.StreamField.FromString(value).to
                    sf = pb2.StreamField.FromString(value).specialField
                    # print(pb2.StreamField.FromString(value))
                    edge= pb2.Edge.FromString(sf)
                    # value = edge.value
                    
                    if from_=='0x0000000000000000000000000000000000000000' or to =='0x0000000000000000000000000000000000000000' or value==0:
                        # print(f'from_ {from_} to {to} value {value}')
                        continue
                    # print(dir(edge))
                    field_values = []
                    # field_exists = edge.ListFields()
                    # print(edge.DESCRIPTOR.fields)
                    for field in edge.DESCRIPTOR.fields:
                        field_value = getattr(edge, field.name)
                        
                        if field_value is None and field.default_value is not None:
                            field_value = field.default_value
                        field_name = field.name
                        # print(field_name)
                        if field_name == "rate" or field_name == "usd_price":
                            continue
                        if field_name == '_from':
                            field_name = 'from'
                        if field_name == 'timestamp':
                            field_value = field_value*1000
                        if field_name == 'coin' and not field_value.startswith('0x'):
                            field_value =field_value.upper()
    # 排除特殊字段和方法
                        if field_name == 'coin' and field_value in dic:
                                value = dic[field_value]
                            
                        if not field_name.startswith("_") and not callable(field_value):
                            # 字段拼接
                            field_values.append(f"{field_value}")
                    if len(field_values) != 16:
                        logger.error(f"Received field_values: {field_values}")
                    edge_str = "edge,"+",".join(field_values)+',0,1924963199000,0,0,0'
                    node1_str = "node,"+from_+',0,1924963199000,0,0,0'
                    node2_str = "node,"+to+',0,1924963199000,0,0,0'
                    logger.info(f"Received edge: {edge_str}")
                    p1.send('coin-18a213409e7f02c55b04d8d4e17bc5-real-time',node1_str.encode())
                    p1.send('coin-18a213409e7f02c55b04d8d4e17bc5-real-time',edge_str.encode())
                    p1.send('coin-18a213409e7f02c55b04d8d4e17bc5-real-time',node2_str.encode())
                    # print(r)
                    # print(f"Received node: {node1_str}")
                    # print(f"Received node: {node2_str}")
                '''
                edge,1641021369,0xeb34311b306ef0005ea12fcab4a463708617eaab,0x28c6c06298d514db089934071355e5743bf21d60,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,5782000000.0,0xc30a9879cb2b4d5f7ea1ce953c3749c66a509f756b171a1d53cb1266957a3c6b,43737.0,97608.0,3280275000000000.0,normal,normal,ERC20,1,17,171641021369,0,1924963199000,0,0,0
                edge,1690732795000,0x1a55a5aaa3b462e8273bee6bd813fea4744f651a,0x2adc599999112ae5665b1a33b94f0dfea6866120,0x7479857a1d7b8f0b9b2b925ad1dd423cff6c657e,7122.0,0xafd0014a8a7c322bc0854a7e29dd7b6b58893467963f08063b6b8e2a152dcf7d,86053.0,86831.0,860530000000000,normal,normal,ERC721,0,1,67985918,67985919659196795,0,1924963199000,0,0,0
                '''
def run_consumer():
    while(True):
        
        # 获取当前日期和时间
        now = datetime.datetime.now()

        # 设置开始时间为每天的00:05分
        start_time = now.replace(hour=0, minute=1, second=0, microsecond=0)

        # 设置结束时间为次日的00:30分
        end_time = now.replace(day=now.day+1, hour=0, minute=30, second=0, microsecond=0)
        while now < end_time:
            if now >= start_time:
                consumer_thread = threading.Thread(target=start_consumer,args=(end_time,))
                consumer_thread.start()
                print(f'start thread {start_time}')
                start_time = start_time.replace(day = start_time.day+1,minute=1, second=0, microsecond=0)
                print(start_time)
                logger.info(start_time)
            time.sleep(60)  # 每隔60秒检查一次时间
            now = datetime.datetime.now()


if __name__ == "__main__":
    
    # adminKafka = KafkaAdminClient(bootstrap_servers=['10.160.196.2:30002'],api_version=(2,6,0))
    # print(adminKafka.list_topics())
    run_consumer()
    # name = sys.argv[1]
    
        #time.sleep(3)

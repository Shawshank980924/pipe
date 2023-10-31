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
# 币种地址和具体币种的映射关系
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
    # 生产者客户端配置
    p1 = KafkaProducer(bootstrap_servers='10.75.75.200:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2),acks='all')
    # 上游区块链实时爬取模块的kafka topic名称是区块链名称拼接日期的方式
    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    # name = "ETH_"+current_time
    # 调试阶段暂时使用该topic名称
    name = "ETH_test"
    print(name)
    print(f'consume {name}\n')
    # kafka消费者客户端配置
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
        group_id="eth-consumer" # 消费者组可以保证进程中止后下次启动从上次消费的地方开始
        )
    running= True  
    while running:
        # print('running')
        # 通过设置超时时间来等待新消息
        # 消费者执行长轮询，长轮询可以配置两个参数，一个是超时时间，一个是最大消息数量
        # 只要轮询条件中一个达到就会返回
        # 我们这里只配置一个超时时间，轮询500ms即返回
        records = consumer.poll(timeout_ms=500) 

        if len(records)==0:
            # 轮询没有消息返回
            # 首先进行时间判断
            if datetime.datetime.now() >= end_time:
                # 超过下一天的00:30该线程终止跳出
                running = False
            # print('no records')
            logger.info("no records")
            time.sleep(2)
        else:
            # 存在消息返回，进行遍历消息
            for message_list in records.values():
                for record in message_list:
                    # 提取消息记录
                    value = record.value
                    # 以下是kafka消息的一些固有属性
                    key = record.key # 消息key
                    offset = record.offset # 偏移量
                    topic = record.topic # topic名
                    partition = record.partition # 消费的topic分区号
                    # print(pb2.StreamField.FromString(value))
                    # 对kafka中的消息进行反序列化取出字段
                    from_ =  pb2.StreamField.FromString(value).from_
                    to = pb2.StreamField.FromString(value).to
                    # sf是真实的交易数据，本身也序列化成字节，也需要反序列化
                    sf = pb2.StreamField.FromString(value).specialField
                    # print(pb2.StreamField.FromString(value))
                    edge= pb2.Edge.FromString(sf)
                    value = edge.value
                    # 对交易发起方地址和交易对方地址进行过滤
                    # 对交易值为0 的交易进行过滤
                    if from_=='0x0000000000000000000000000000000000000000' or to =='0x0000000000000000000000000000000000000000' or value==0:
                        logger.info(f"filter edge: from: {edge._from} to: {edge.to} value : {edge.value} rank: {edge.rank}")
                        # print(f'from_ {from_} to {to} value {value}')
                        continue
                    # print(dir(edge))
                    field_values = []
                    # field_exists = edge.ListFields()
                    # print(edge.DESCRIPTOR.fields)
                    # 取出交易数据定义的所有字段
                    for field in edge.DESCRIPTOR.fields:
                        # 从该条交易消息中取出每个字段的值
                        field_value = getattr(edge, field.name)
                        # proto3有个特性，若字段值赋默认值会自动将消息中的该字段的值去除
                        # 所以需要进行特判，赋默认值
                        if field_value is None and field.default_value is not None:
                            field_value = field.default_value
                        field_name = field.name
                        # print(field_name)
                        # 一些字段不需要记录
                        if field_name == "rate" or field_name == "usd_price":
                            continue
                        # from的字段名去掉_
                        if field_name == '_from':
                            field_name = 'from'
                        # 时间戳换成毫秒单位
                        if field_name == 'timestamp':
                            field_value = field_value*1000
                        # 交易币种原始记录中有以16进制的地址表示，需要转换成真实的币种
                        # 原始记录中已经映射的币种，转成大写
                        if field_name == 'coin' and not field_value.startswith('0x'):
                            field_value =field_value.upper()
                        # 16进制的地址需要做一次映射为真实币种名
                        if field_name == 'coin' and field_value in dic:
                                value = dic[field_value]
                        # _开头的字段是序列化协议自己生成的字段，非用户定义的字段
                        if not field_name.startswith("_") and not callable(field_value):
                            # 按下游的要求做字段的拼接
                            field_values.append(f"{field_value}")
                        # 对于拼接字段不为16个多交易数据打错误日志
                    if len(field_values) != 16:
                        logger.error(f"Received field_values: {field_values}")
                    # 按下游的要求拼接edge 以及0,1924963199000,0,0,0 再发送交易边和交易账号的消息
                    edge_str = "edge,"+",".join(field_values)+',0,1924963199000,0,0,0'
                    node1_str = "node,"+from_+',0,1924963199000,0,0,0'
                    node2_str = "node,"+to+',0,1924963199000,0,0,0'
                    logger.info(f"Received edge: {edge_str}")
                    # 通过生产者客户端序列化后发送到下游指定的topic
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

        # 设置开始时间为每天的00:01分
        start_time = now.replace(hour=0, minute=1, second=0, microsecond=0)

        # 设置结束时间为次日的00:30分
        end_time = now+datetime.timedelta(days=1) 
        end_time = end_time.replace(hour=0, minute=30, second=0, microsecond=0)
        while now < end_time:
            # 每天超过start_time时开启一个消费线程，每天只启动一个消费线程
            # 该消费线程的生命周期时当天直到次日00.30，由消费线程内部逻辑控制
            if now >= start_time:
                consumer_thread = threading.Thread(target=start_consumer,args=(end_time,))
                consumer_thread.start()
                print(f'start thread {start_time}')
                # 启动以后将start_time调整为次日00.01分，用于下次启动新的线程时判断
                start_time = start_time + datetime.timedelta(days=1)
                # start_time = start_time.replace(day = start_time.day+1,minute=1, second=0, microsecond=0)
                # print(start_time)
                # logger.info(start_time)
            time.sleep(60)  # 每隔60秒检查一次时间
            now = datetime.datetime.now()


if __name__ == "__main__":
    
    # adminKafka = KafkaAdminClient(bootstrap_servers=['10.160.196.2:30002'],api_version=(2,6,0))
    # print(adminKafka.list_topics())
    run_consumer()
    # name = sys.argv[1]
    
        #time.sleep(3)

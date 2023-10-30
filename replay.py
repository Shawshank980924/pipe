import functools
import random
from pyspark.sql import SparkSession
import datetime
from kafka import KafkaProducer,KafkaAdminClient
from kafka.admin import NewTopic
import time

import logging
spark= SparkSession.builder \
    .appName('dataReplay') \
    .master('local') \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:///root/sxx/log4j.properties") \
    .getOrCreate()
    # df.show(20)
log_level = "INFO"
spark.sparkContext.setLogLevel(log_level)
logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# logger = logging.getLogger(__name__)

total = spark.sparkContext.accumulator(0)
def DataReplay(begin_date,end_date,topic_name):
    # adminKafka = KafkaAdminClient(bootstrap_servers=['hpc01:9092','hpc02:9092','hpc03:9092','hpc04:9092','hpc05:9092','hpc06:9092','hpc07:9092','hpc08:9092','hpc09:9092','hpc10:9092'],api_version=(2,6,0))
    # if topic_name in adminKafka.list_topics():    
    #     dl = []
    #     dl.append(topic_name)
    #     adminKafka.delete_topics(dl)
    #     time.sleep(10)
    #     # print('===========ok2')
    # l = []
    # l.append(NewTopic(topic_name,30,3))
    # adminKafka.create_topics(l)
    logger.info("loading datareplay")
    begin_date = datetime.datetime.strptime(begin_date, "%Y_%m_%d")
    end_date = datetime.datetime.strptime(end_date, "%Y_%m_%d")
    delta = datetime.timedelta(days=1)
    
    while begin_date <= end_date:
        y,m,d = begin_date.strftime("%Y_%m_%d").split('_')
        path = 'hdfs:///sxx/replay/trans/{}/{}/{}.orc'.format(y,m,d)
        orc2kafka(path,topic_name)
        logger.info(f'begin_date {begin_date}')
        # logger.info(begin_date)
        begin_date+=delta
        remain = remain_seconds_today()
        logger.info(f'wait {remain} for tomorrow')
        # time.sleep(remain)

def orc2kafka(path,topic):
    # producer = KafkaProducer(acks=1,bootstrap_servers=['hpc02:9092','hpc03:9092','hpc04:9092','hpc05:9092','hpc06:9092','hpc07:9092','hpc08:9092','hpc09:9092','hpc10:9092'],api_version=(2,6,0))
    global total
    
    df =spark.read.orc(path)
    total_records = df.count()
    remain_seconds = remain_seconds_today()-30*60
    sleep_time = 1.0*remain_seconds / total_records
    
    # print(sleep_time)
    # df = df.limit(1000)
    df.foreachPartition(functools.partial(go2kafka,topic=topic,sleep_time = sleep_time))
    
    logger.info(f'total records for {path} {total}')
    # for row in df.collect():
        # producer.send(topic,bytes(row[0]))
def generate_random_number():
    return random.randint(0, 599)
def go2kafka(partition,topic,sleep_time):
    p1 = KafkaProducer(acks=1,bootstrap_servers=['hpc02:9092'],api_version=(2,6,0))
    global total
    for row in partition:
        rate = generate_random_number()
        if(rate>=0 and rate<10):
            p1.send(topic,bytes(row[1]))
            total += 1
            # print(total)
            # if(total%100==0):
            #     print(f'total {total}')
            
        # print(f"sleep_time: {sleep_time}")
        # time.sleep(sleep_time)
        
    p1.close()
    return 0
# def print_accumulator_value():
#     # global total
#     with lock:
#         print("Accumulator value:", total.value)
def remain_seconds_today():
    now = datetime.datetime.now()

    # 获取明天的日期
    tomorrow_date = now.date() + datetime.timedelta(days=1)

    # 获取明天开始的时间（即今天结束的时间）
    tomorrow_start = datetime.datetime.combine(tomorrow_date, datetime.time(0, 0))

    # 计算剩余时间
    remaining_time = tomorrow_start - now
    #多预留半小时时间
    remaining_seconds = remaining_time.total_seconds()
    return remaining_seconds

DataReplay("2022_01_01","2022_01_01","ethpro")
from kafka import KafkaConsumer
import time
# import cls_pb2 as cls
from kafka import KafkaAdminClient
import re
import cls_pb2 as pb2

def start_consumer():
    
    consumer = KafkaConsumer("coin-18a213409e7f02c55b04d8d4e17bc5",
       bootstrap_servers='10.75.75.200:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2),acks='all',
    # bootstrap_servers='server-1:30812',
    # security_protocol='SASL_PLAINTEXT',
    # sasl_mechanism='SCRAM-SHA-512',
    # sasl_plain_username='admin',
    # sasl_plain_password='starryDev',

    # api_version=(2,8,2),
    auto_offset_reset='earliest',
    # group_id="eth-consumer-test"
        )
    count = 0
    for msg in consumer:
        data = msg.value 
        print(msg)
        print("topic = %s" % msg.topic) # topic default is string
        print("partition = %d" % msg.offset)
        # obj = pb2.StreamField.FromString(msg.value).specialField
        # print(pb2.Edge.FromString(obj))

        # print("value = %s" % msg.value.decode()) # bytes to string
        print("timestamp = %d" % msg.timestamp)
        print("time = ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime( msg.timestamp/1000 )) )
        print("#################")
        count+=1
        print(count)
        # msg.value
def run_admin():
    # admin = KafkaAdminClient(bootstrap_servers='server-1:30812',
    #                         security_protocol='SASL_PLAINTEXT',
    #                         sasl_mechanism='SCRAM-SHA-512',
    #                         sasl_plain_username='admin',
    #                         sasl_plain_password='starryDev',
    #                         api_version=(2,8,2))
    admin = KafkaAdminClient(bootstrap_servers='10.75.75.200:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2))
    # print(type(admin.list_topics()))
    topic_list = admin.list_topics()
    # print(topic_list)
    #匹配topic
    remove_list = []
    for topic in topic_list:
        if re.match('ETH_+',topic) !=None:
            remove_list.append(topic)
        
            
    print(remove_list)
if __name__ == '__main__':
    start_consumer()
    # run_admin()
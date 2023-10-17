from kafka import KafkaProducer
from time import sleep


def start_producer():
    producer = KafkaProducer(bootstrap_servers='10.75.75.200:30178',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2))
    for i in range(0,100000):
        msg = 'msg is ' + str(i)
        res = producer.send('my_favorite_topic3', msg.encode('utf-8'))
        print(res.get(timeout=10))
        sleep(3)
        # break
def get_producer():
    return KafkaProducer(bootstrap_servers='10.75.75.200:30178',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJDljd5Ggdhkka$',
                         api_version=(2,8,2))

if __name__ == '__main__':
    start_producer()
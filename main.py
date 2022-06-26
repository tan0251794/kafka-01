from socket import timeout
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
from loguru import logger

admin_client = KafkaAdminClient(
    bootstrap_servers = "localhost:9092"
)

# show tất cả topic
topics = KafkaConsumer(
    # group_id='test', 
    bootstrap_servers=['localhost:9092']
).topics()
print(topics)

new_topic_name = "topic_1"
if new_topic_name not in topics:
    admin_client.create_topics(
        new_topics=[NewTopic(name=new_topic_name, num_partitions=1, replication_factor=1)], 
        validate_only=False
    )
    logger.info('Tạo Topic thành công!')
else:
    logger.info('Topic đã tồn tại!')

# send() send and forget
# Block for 'synchronous' sends
from kafka import KafkaProducer
from kafka.errors import KafkaError
producer = KafkaProducer(
        bootstrap_servers='localhost:9092'
    )

# produce asynchronously with callbacks
def on_send_success(record_metadata):
    print(f'{record_metadata.topic}, {record_metadata.partition}, {record_metadata.offset}')

def on_send_error(excp):
    logger.error('I am an errback', exc_info=excp)

producer.send('topic_1', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
producer = KafkaProducer(retries=5) # configure multiple retries
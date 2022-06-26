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
from kafka.errors import KafkaError
producer = KafkaProducer(
        bootstrap_servers='localhost:9092'
    )

# try:
#     future = producer.send('topic_1', b'From program')
#     record_metadata = future.get(timeout=60)
#     producer.flush()
# except KafkaError as exc:
#     print("Exception during getting assigned partitions - {}".format(exc))
#     # Decide what to do if produce request failed...
#     pass

for _ in range(100):
    future = producer.send('topic_1', b'From program')
    future.get()
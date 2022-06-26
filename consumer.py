from kafka import KafkaConsumer

consumer = KafkaConsumer('topic_1',
                        #  group_id='my-group',
                         bootstrap_servers='localhost:9092')
for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
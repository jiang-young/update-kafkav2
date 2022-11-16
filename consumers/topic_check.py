from confluent_kafka.admin import AdminClient


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    logger.info("My test", msg)
    ## tester
    topic_metadataTest = client.list_topics(timeout=5)
    logger.info("My test", msg)
    ## tester

    return topic in set(t.topic for t in iter(topic_metadataTest.topics.values()))

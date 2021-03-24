import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from AivenService import AivenService

class KafkaService:
    """
    Helper methods for sending/receiving messages through Aiven's Kafka

    Attributes
    ----------
    m_aiven_service : AivenService
        Contains configurations for various Aiven services.
    m_postgres_service : PostgresService
        Helper functions for database manipulation.
    m_kafka_producer : KafkaProducer
        The Kafka Producer
    m_kafka_consumer : KafkaConsumer
        The Kafka Consumer
    """

    def __init__(self, aiven_service, postgres_service):
        self.m_aiven_service = aiven_service
        self.m_postgres_service = postgres_service
        self.m_kafka_producer = None
        self.m_kafka_consumer = None

    def kafka_create_topic(self, topic):
        """
        Creates the kafka topic if it doesn't exist yet.
        """
        if not isinstance(self.m_aiven_service, AivenService):
            print("aiven service is not available")
            return 1

        for topic_in_kafka in self.m_aiven_service.m_aiven_handle_kafka["topics"]:
            if topic == topic_in_kafka['topic_name']:
                print("kafka topic already exists.")
                return 0

        self.m_aiven_service.m_aiven_client.create_service_topic(
            project=self.m_aiven_service.m_aiven_project_name,
            service=self.m_aiven_service.m_service_config_kafka.m_serviceName,
            topic=topic,
            partitions=1,
            replication=2,
            min_insync_replicas=1,
            retention_bytes=-1,
            retention_hours=24,
            cleanup_policy="delete"
        )
        return 0

    def kafka_create_producer(self):
        """
        Creates the kafka producer.
        """
        if self.m_aiven_service is None:
            print("aiven service is not available")
            return 1

        self.m_kafka_producer = KafkaProducer(
            bootstrap_servers=self.m_aiven_service.m_service_config_kafka.m_hostname + ":" + str(self.m_aiven_service.m_service_config_kafka.m_port),
            security_protocol="SSL",
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key"
        )
        return 0

    def kafka_send(self, topic, message):
        """
        Helper function for sending a message through the kafka producer.

        Parameters
        ----------
        topic : str
            The Kafka topic
        message : str
            The message to be sent through Kafka for consumption by the Kafka Consumer.
        """
        if self.m_kafka_producer is None:
            print("kafka producer not available")
            return 1
        else:
            self.m_kafka_producer.send(topic, message.encode("utf-8"))
            return 0

    def kafka_create_consumer(self, topic):
        """
        Creates the kafka consumer.
        """
        self.m_kafka_consumer = KafkaConsumer(
            topic,
            auto_offset_reset="earliest",
            bootstrap_servers=self.m_aiven_service.m_service_config_kafka.m_hostname + ":" + str(self.m_aiven_service.m_service_config_kafka.m_port),
            client_id="demo-client-1",
            group_id="demo-group",
            security_protocol="SSL",
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key"
        )

        self.m_postgres_service.connect()
        self.m_postgres_service.create_table_if_not_exist()
        self.m_postgres_service.disconnect()

        return 0

    def kafka_run_consumer(self):
        """
        Loop through the messaging queue and process messages as they are received.
        """
        for msg in self.m_kafka_consumer:
            m = msg.value.decode("utf-8")
            json_message = json.loads(m)

            datetime = json_message["datetime"]
            url = json_message["url"]
            status_code = json_message["status_code"]
            filtered_text = json.dumps(json_message["filtered_text"])

            self.m_postgres_service.connect()
            self.m_postgres_service.store_to_database(datetime,url,status_code,filtered_text)
            self.m_postgres_service.disconnect()

            self.m_kafka_consumer.commit()
        return 0

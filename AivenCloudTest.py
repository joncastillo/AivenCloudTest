import argparse
import threading

from AivenService import AivenService
from ServiceConfigKafka import ServiceConfigKafka
from ServiceConfigPostGres import ServiceConfigPostGres
from KafkaService import KafkaService
from MonitoringTool import MonitoringTool
from PostgresService import PostgresService


def main():
    """
    Main function for testing Aiven's Kafka and Postgres Databases

    Usage: python AivenCloudTest.py --url https://www.cnn.com --regex "news.+\." --delay 30
           This samples https://www.cnn.com every 30 seconds for snippets starting with news and ends with a period.

    References:
        https://github.com/aiven/aiven-examples
        https://help.aiven.io/en/articles/489572-getting-started-with-aiven-for-apache-kafka
        https://kafka.apache.org/intro
    """

    parser = argparse.ArgumentParser(description="Test Aiven-Kafka Producer+Consumer and Aiven-Postgres.")
    parser.add_argument('--url', dest='url', type=str, help='the url to monitor', required=True)
    parser.add_argument('--regex', dest='regex', type=str, help='a regex to filter out contents of the webpage',
                        required=True)
    parser.add_argument('--delay', dest='delay', type=int, help='the delay between scans in seconds', required=True)
    args = parser.parse_args()

    # Topic name for Kafka
    topic = "topic_monitor_message"

    # Kafka configuration, passwords are of no value outside my Aiven projects
    config_kafka = ServiceConfigKafka(
        "aiven-assignment-kafka",
        "jonathan-9d93",
        "password0!",
        17044,
        "accesskey.key",
        "access.crt",
        "ca.crt")

    # Posgres configuration, passwords are of no value outside my Aiven projects
    config_postgres = ServiceConfigPostGres(
        "aiven-assignment-postgres",
        "avnadmin",
        "rv64oul8g05h4ebm",
        17042,
        "defaultdb")

    # Aiven.io service credentials and configuration, passwords are of no value outside my Aiven projects
    aiven_service = AivenService(
        "jonathan-9d93",
        "jonathan.chioco.castillo@gmail.com",
        "aiven81611",
        config_kafka,
        config_postgres)

    # login
    aiven_service.login()

    # create Aiven kafka service if missing
    aiven_service.kafka_init()

    # create Aiven postgres service if missing
    aiven_service.postgres_init()

    # Initialise internal Postgres helper class
    postgres_service = PostgresService(aiven_service)

    # Initialise internal Kafka helper class
    kafka_service = KafkaService(aiven_service, postgres_service)

    # Create Kafka Topic if missing
    kafka_service.kafka_create_topic(topic)

    # Create Kafka Producer
    kafka_service.kafka_create_producer()

    # Create Kafka Consumer (Supposed to be in another machine)
    kafka_service.kafka_create_consumer(topic)

    # Launch Kafka Consumer Thread
    thread_kafka_consumer = threading.Thread(target=kafka_service.kafka_run_consumer, name="kafka consumer", args=())
    thread_kafka_consumer.start()

    # Launch Monitoring Tool (Kafka Producer)
    monitoringTool = MonitoringTool(args.url, args.regex, args.delay, kafka_service, topic)
    thread_kafka_monitoringTool = threading.Thread(target=monitoringTool.execute, name="monitoring tool", args=())
    thread_kafka_monitoringTool.start()

    # ensure that the main thread doesn't exit
    thread_kafka_consumer.join()
    thread_kafka_monitoringTool.join()


if __name__ == "__main__":
    main()

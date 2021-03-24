import json
import re
import requests
import time

from datetime import datetime


class MonitoringTool(object):
    """
    This monitoring tool polls a website for its status and snippets of its contents

     Attributes
    ----------
    m_url : str
        the url of website to be polled. For example: https://google.com
    m_regex : str
        the regex to be applied to the contents for filtering. Like grep.
    m_delay : float
        delay between polls in seconds
    m_kafka_service : KafkaService
        Kafka service helper for producer/consumer methods.
    m_kafka_topic : str
        Kafka topic, the kafka channel where messages are transmitted.
    """

    def __init__(self, url, regex, delay, kafka_service, kafka_topic):
        self.m_url = url
        self.m_regex = regex
        self.m_delay = delay
        self.m_kafka_service = kafka_service
        self.m_kafka_topic = kafka_topic

    def __execute_regex(self, string):
        """
        Executes a regular expression on a specified string.
        All matches are contained as a list of dictionaries with each entry containing the location and the matched text.

        Parameters
        ----------
        string : str
            The string to be processed

        Returns
        ----------
        list
            list of matches
        """

        output = []

        p = re.compile(self.m_regex)
        for m in p.finditer(string):
            location = m.start()
            text = m.group()
            entry = {"location": location, "text": text}
            output.append(entry)

        return output

    def execute(self):
        """
        Continuously polls a url for status_code and contents. This is meant to be run asynchronously.
        """

        while True:
            r = requests.get(self.m_url, allow_redirects=True)
            status_code = r.status_code
            content = str(r.content)
            regex_out = self.__execute_regex(content)
            now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            output = {"datetime": now, "url": self.m_url, "status_code": status_code, "filtered_text": regex_out}
            output_string = json.dumps(output)
            self.m_kafka_service.kafka_send(self.m_kafka_topic, output_string)
            time.sleep(self.m_delay)

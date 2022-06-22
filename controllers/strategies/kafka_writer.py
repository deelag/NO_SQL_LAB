import os
import json

from azure.eventhub import EventHubProducerClient, EventData


class KafkaWriter:
    def __init__(self):
        self.client = EventHubProducerClient.from_connection_string(
            conn_str=os.getenv("EVENT_HUB__CONNECTION_STRING"), eventhub_name=os.getenv("EVENT_HUB__NAME")
        )

    def write(self, data):
        for item in data:
            print(item)
            self.client.send_event(EventData(json.dumps(item)))

        self.client.close()
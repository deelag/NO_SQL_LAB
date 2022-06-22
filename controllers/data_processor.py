import asyncio
from sodapy import Socrata
from controllers.redis.redis_dao import RedisDAO
from utils.utils import parse_endpoint
from .strategies.console_writer import ConsoleWriter
from .strategies.kafka_writer import KafkaWriter

STRATEGIES = {
    "console": ConsoleWriter,
    "kafka": KafkaWriter,
}

class DataProcessor:
    def __init__(self, strategy, endpoint):
        self.strategy = strategy
        self.url, self.identifier = parse_endpoint(endpoint)
        self.writer = STRATEGIES[self.strategy]()
        self.redis = RedisDAO(self.identifier, strategy)

    async def _get_data(self):
        return Socrata(self.url, None).get_all(self.identifier)

    async def _upload(self, data):
        try:
            self.redis.set_in_progress_status()
            
            self.writer.write(data)

            self.redis.set_completed_status()
        except Exception as e:
            raise ProcessingError("Error while writing to {}, reason: {}".format(
                self.strategy, str(e)
            ))
        return "Content successfully written to {}".format(self.strategy)

    def process(self):
        if self.redis.already_processed():
            return "Content was already written to {}".format(self.strategy)
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._get_data())
        result = loop.run_until_complete(self._upload(data))
        return result

class ProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg
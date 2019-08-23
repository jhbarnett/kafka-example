import os
from pydash import get
import structlog
from kafka import KafkaConsumer
import json

logger = structlog.getLogger(__name__)


def json_deserializer(v): return json.loads(v, encoding='utf-8')


class SyncConsumer:
    group = "sync"
    topic = 'sync.users.events'

    def __init__(self):
        self._consumer = KafkaConsumer(
            self.topic,
            group_id=self.group,
            bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            key_deserializer=json_deserializer,
            value_deserializer=json_deserializer,
        )

    def subscribe(self, handler):
        for message in self._consumer:
            handler(message)


if __name__ == '__main__':
    try:
        def handler(message):
            # logger.info("Received message: {}".format(**message.value))
            payload = message.value
            if get(payload, 'resource') == 'activity':
                logger.info("Handling Activity Sync for... {}".format(
                    get(payload, 'user')))
            elif get(payload, 'resource') == 'profile':
                logger.info("Handling Profile Sync for... {}".format(
                    get(payload, 'user')))
            else:
                logger.warning("Message not handled", **message)

        consumer = SyncConsumer()
        consumer.subscribe(handler)
    except KeyboardInterrupt:
        pass

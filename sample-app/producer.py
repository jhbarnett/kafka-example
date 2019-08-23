import os
import random
import uuid
from datetime import datetime

import json
import structlog
from kafka import KafkaProducer

logger = structlog.getLogger(__name__)


def json_serializer(v): return json.dumps(v).encode('utf-8')


class SyncProducer:
    topic = "sync.users.events"

    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            key_serializer=json_serializer,
            value_serializer=json_serializer,
        )

    def publish(self, id, message):
        logger.info("Sending message: {}".format(message))
        self._producer.send(
            self.topic,
            key=id,
            value=message,
            timestamp_ms=int(datetime.now().timestamp() * 1000)
        )


if __name__ == '__main__':
    try:
        users = [str(uuid.uuid4())[:6] for _ in range(25)]
        producer = SyncProducer()
        while True:
            try:
                user = random.choice(users)
                message = dict(
                    user=user,
                    resource="activity",
                )

                producer.publish(user, message)
            except Exception as e:
                logger.exception(
                    "Failed to produce SyncRequest event.... {}".format(e))
                break

    except KeyboardInterrupt:
        pass

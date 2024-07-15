"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        auto_offset_reset = "earliest" if offset_earliest else "latest"
        self.broker_properties = {
                "bootstrap.servers" : "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094", 
                "group.id": f"{self.topic_name_pattern}",
                "default.topic.config": 
                {
                    "acks": "all",
                    "auto.offset.reset": f"{auto_offset_reset}"
                },
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        self.consumer.subscribe( [self.topic_name_pattern], on_assign = self.on_assign )


    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
           if self.offset_earliest is True:
                partitions.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(timeout=1.0)
            if message is not None:
                if message.error() is not None:
                    self.message_handler(message)
                    return 1
                else:
                    logger.error(message.error())
                    return 0
            else:
                logger.debug("no message received by consumer")
                return 0
        except SerializerError as e:
            logger.info("_consume is incomplete - skipping")
            logger.error(f"Message deserialization failed for {message} : {e}")
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("Closing down consumer to commit final offsets")
        self.consumer.close()

import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.eofs_by_client_id = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data message for client_id {client_id}")
        client_fruits = self.fruit_top.setdefault(client_id, {})
        client_fruits[fruit] = (client_fruits.get(fruit, fruit_item.FruitItem(fruit, 0))
                                + fruit_item.FruitItem(fruit, int(amount)))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client_id: {client_id}")

        self.eofs_by_client_id[client_id] = self.eofs_by_client_id.get(client_id, 0) + 1
        if self.eofs_by_client_id[client_id] < SUM_AMOUNT:
            return

        client_fruits = self.fruit_top.pop(client_id, {})
        sorted_fruits = sorted(client_fruits.values())
        fruit_chunk = list(sorted_fruits[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

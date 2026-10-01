import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_message_count = {}
        self.partial_info_by_client= {}

    def process_messsage(self, message, ack, nack):
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        logging.info(f"Received top for {client_id}")

        top_candidates = self.partial_info_by_client.setdefault(client_id, [])
        top_candidates.extend(fruit_top)
        self.client_message_count[client_id] = (self.client_message_count.get(client_id, 0) + 1)
        if self.client_message_count[client_id] < AGGREGATION_AMOUNT:
            ack()
            return

        top_candidates.sort(key=lambda fruit_amount: fruit_amount[1], reverse=True)
        fruit_top = top_candidates[:TOP_SIZE]
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))

        self.partial_info_by_client.pop(client_id, None)
        self.client_message_count.pop(client_id, None)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

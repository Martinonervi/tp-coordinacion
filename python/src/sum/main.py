import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.sums_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        )
        self.sums_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{ID}"]
        )

        self.amount_by_fruit = {}

        self.records_read = {} #lo que leyo este sum de cada cliente
        self.total_count = {} #cantidad total de paquetes del cliente (viene con el EOF)
        self.global_count = {} #la cantidad de paquetes leidos por todos los sum (que este sum se entero)
        self.lock = threading.Lock()

    def process_sums_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "CLOSE":
            self._process_close(*fields[1:])
        elif fields[0] == "COUNT":
            self._process_count(*fields[1:])
        ack()

    def _process_close(self, client_id, total_count):
        logging.info(f"Received CLOSE for client_id: {client_id}, total_count: {total_count}")
        with self.lock:
            self.total_count[client_id] = total_count
            self._notify_count(client_id, self.records_read.get(client_id, 0))

    def _process_count(self, client_id, count):
        with self.lock:
            self.global_count[client_id] = self.global_count.get(client_id, 0) + count
            self._try_finish(client_id)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data for client_id: {client_id}")
        with self.lock:
            client_fruits = self.amount_by_fruit.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            #guardo cuanto lei de ese cliente
            self.records_read[client_id] = self.records_read.get(client_id, 0) + 1

            #si este cliente estaba terminando aviso que lei cosas nuevas
            if client_id in self.total_count:
                self._notify_count(client_id, 1)

    def _process_eof(self, client_id, total_count):
        logging.info(f"Received EOF for client_id: {client_id}, total_count: {total_count}")
        with self.lock:
            self.total_count[client_id] = total_count
            self.sums_output_exchange.send(
                message_protocol.internal.serialize(["CLOSE", client_id, total_count])
            )
            self._notify_count(client_id, self.records_read.get(client_id, 0))

    def _notify_count(self, client_id, count):
        self.sums_output_exchange.send(
            message_protocol.internal.serialize(["COUNT", client_id, count])
        )
        self.global_count[client_id] = self.global_count.get(client_id, 0) + count
        logging.info(f"[DEBUG] notify client_id={client_id} +{count} -> global={self.global_count[client_id]} total={self.total_count.get(client_id)}")
        self._try_finish(client_id)

    def _try_finish(self, client_id):
        if self.global_count.get(client_id) != self.total_count.get(client_id):
            return
        logging.info(f"Flushing client_id: {client_id}")
        client_fruits = self.amount_by_fruit.pop(client_id, {})
        for final_fruit_item in client_fruits.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message for client_id: {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.records_read.pop(client_id, None)
        self.total_count.pop(client_id, None)
        self.global_count.pop(client_id, None)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "DATA":
            self._process_data(*fields[1:])
        elif fields[0] == "EOF":
            self._process_eof(*fields[1:])
        ack()


    def start(self):
        sums_thread = threading.Thread(
            target=self.sums_input_exchange.start_consuming,
            args=(self.process_sums_message,),
            daemon=True,
        )
        sums_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

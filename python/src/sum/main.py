import os
import logging
import threading
import zlib
import signal

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

        self.sum_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        )
        self.sum_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{ID}"]
        )

        self.amount_by_fruit = {}

        self.records_read = {} #lo que leyo este sum de cada cliente
        self.total_count = {} #cantidad total de paquetes del cliente (viene con el EOF)
        self.global_count = {} #la cantidad de paquetes leidos por todos los sum (que este sum se entero)
        self.lock = threading.Lock()
        self.sums_thread = None


    # loop que corre el thread que se encarga de consumir mensajes de otros sums
    def _start_sums_consumer(self):
        try:
            self.sum_input_exchange.start_consuming(self.process_sums_message)
        finally:
            self.sum_input_exchange.close()

    def process_sums_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "CLOSE":
            self._process_close(*fields[1:])
        elif fields[0] == "COUNT":
            self._process_count(*fields[1:])
        ack()

    #mensaje que un sum recibe cuando a otro sum le llego el EOF de un cliente,
    # es un aviso de que el cliente esta terminando (de mandar)
    def _process_close(self, client_id, total_count):
        logging.info(f"Received CLOSE for client_id: {client_id}, total_count: {total_count}")
        with self.lock:
            #se guarda la cantidad total de mensajes que mando el cliente
            self.total_count[client_id] = total_count
            #y le notifica a los demas sum lo que él (este sum) proceso del cliente
            self._notify_count(client_id, self.records_read.get(client_id, 0))

    # msj que recibo de un sum donde me dice cuanto proceso de un cliente
    def _process_count(self, client_id, count):
        with self.lock:
            self.global_count[client_id] = self.global_count.get(client_id, 0) + count
            self._try_finish(client_id)

    def _notify_count(self, client_id, count):
        self.sum_output_exchange.send(
            message_protocol.internal.serialize(["COUNT", client_id, count])
        )
        self.global_count[client_id] = self.global_count.get(client_id, 0) + count
        self._try_finish(client_id)

    #se fija si ya le puede mandar al aggregator, esto solo pasa si entre todos los
    # sums procesaron el total de mensajes del cliente, que venia con el EOF
    def _try_finish(self, client_id):
        if self.global_count.get(client_id) != self.total_count.get(client_id):
            return
        logging.info(f"Flushing client_id: {client_id}")
        client_fruits = self.amount_by_fruit.pop(client_id, {})
        for final_fruit_item in client_fruits.values():
            aggregator_index = zlib.crc32(f"{client_id}{final_fruit_item.fruit}".encode()) % AGGREGATION_AMOUNT
            self.data_output_exchanges[aggregator_index].send(message_protocol.internal.serialize(
                [client_id, final_fruit_item.fruit, final_fruit_item.amount])
            )
        logging.info(f"Broadcasting EOF message for client_id: {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.records_read.pop(client_id, None)
        self.total_count.pop(client_id, None)
        self.global_count.pop(client_id, None)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data for client_id: {client_id}")
        with self.lock:
            client_fruits = self.amount_by_fruit.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            #guardo cuanto lei de ese cliente
            self.records_read[client_id] = self.records_read.get(client_id, 0) + 1

            #si el EOF ya habia llegado, este cliente estaba terminando, por lo que
            # le aviso a los otros sums que lei cosas nuevas
            if client_id in self.total_count:
                self._notify_count(client_id, 1)

    def _process_eof(self, client_id, total_count):
        logging.info(f"Received EOF for client_id: {client_id}, total_count: {total_count}")
        with self.lock:
            self.total_count[client_id] = total_count
            #le aviso a todos los sums que estamos terminando con el cliente
            self.sum_output_exchange.send(
                message_protocol.internal.serialize(["CLOSE", client_id, total_count])
            )
            #les mando a los otros sums cuanto procese del cliente
            self._notify_count(client_id, self.records_read.get(client_id, 0))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "DATA":
            self._process_data(*fields[1:])
        elif fields[0] == "EOF":
            self._process_eof(*fields[1:])
        ack()


    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.sums_thread = threading.Thread(
            target=self._start_sums_consumer
        )
        try:
            self.sums_thread.start()
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.close()

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.input_queue.stop_consuming()
        self.sum_input_exchange.stop_consuming()

    def close(self):
        self.input_queue.stop_consuming()
        self.sums_thread.join()

        self.input_queue.close()
        self.sum_output_exchange.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError
import pika
import random
import string

DIRECT_EXCHANGE = 'direct'
DEFAULT_EXCHANGE = ''

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.handler_connection = RabbitMQChannel(host, prefetch_count=1)
        self.queue_name = self.handler_connection.setup_queue(queue_name)

    def start_consuming(self, on_message_callback):
        self.handler_connection.consume(self.queue_name, on_message_callback)

    def stop_consuming(self):
        self.handler_connection.stop_consuming()

    def send(self, message):
        self.handler_connection.publish(DEFAULT_EXCHANGE, self.queue_name, message, persistent=True)

    def close(self):
        self.handler_connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.handler_connection = RabbitMQChannel(host, prefetch_count=0)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = self.handler_connection.subscribe_to_exchange(exchange_name, routing_keys)

    def start_consuming(self, on_message_callback):
        self.handler_connection.consume(self.queue_name, on_message_callback)

    def stop_consuming(self):
        self.handler_connection.stop_consuming()

    def send(self, message):
        for key in self.routing_keys:
            self.handler_connection.publish(self.exchange_name, key, message, persistent=False)

    def close(self):
        self.handler_connection.close()

class RabbitMQChannel:
    def __init__(self, host, prefetch_count):
        self.connection = None
        self.consumer_tag = None
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=prefetch_count)
        except pika.exceptions.AMQPConnectionError as e:
            if self.connection and self.connection.is_open:
                self.connection.close()
            raise MessageMiddlewareDisconnectedError(f"Conexion failed, host: {host}") from e
        except pika.exceptions.AMQPError as e:
            if self.connection and self.connection.is_open:
                self.connection.close()
            raise MessageMiddlewareMessageError("Error init") from e

    def setup_queue(self, name):
        try:
            result = self.channel.queue_declare(queue=name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Conexion failed") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error declaring the queue") from e
        return result.method.queue

    def subscribe_to_exchange(self, name, routing_keys):
        try:
            self.channel.exchange_declare(exchange=name, exchange_type=DIRECT_EXCHANGE)
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for key in routing_keys:
                self.channel.queue_bind(exchange=name, queue=queue_name, routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Conexion failed") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error while subscribing to a exchange") from e
        return queue_name

    def publish(self, exchange, routing_key, body, persistent=True):
        delivery_mode = pika.DeliveryMode.Persistent if persistent else pika.DeliveryMode.Transient
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=delivery_mode)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Conexion failed") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error while publishing in a channel") from e

    def consume(self, queue, on_message_callback):
        def handle_callback(channel, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self.consumer_tag = self.channel.basic_consume(
                queue=queue,
                on_message_callback=handle_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Conexion failed") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error while consuming in a channel") from e
        finally:
            self.consumer_tag = None

    def stop_consuming(self):
        try:
            if not self.consumer_tag:
                return
            self.channel.stop_consuming(self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Conexion failed, error") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error in stop consuming") from e

    def close(self):
        try:
            if self.connection.is_open:
                if self.consumer_tag:
                    self.channel.stop_consuming(self.consumer_tag)
                    self.consumer_tag = None
                if self.channel.is_open:
                    self.channel.close()
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError("Error closing") from e

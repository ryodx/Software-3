"""Consumidor de FACTURACION (Python).
Cola 'billing.queue' enlazada a 'orders.*' y 'payments.*'.
"""
import json
import os
import time

import pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "admin")
PASS = os.getenv("RABBITMQ_PASS", "admin123")
EXCHANGE = os.getenv("EXCHANGE", "eventos")
QUEUE = "billing.queue"
BINDINGS = ["orders.*", "payments.*"]
SERVICE = "consumer-billing"


def connect():
    creds = pika.PlainCredentials(USER, PASS)
    params = pika.ConnectionParameters(HOST, PORT, "/", creds, heartbeat=60)
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print(f"[{SERVICE}] RabbitMQ no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def on_message(ch, method, properties, body):
    event = json.loads(body)
    data = event["data"]
    if event["type"] == "orders.created":
        print(f"[{SERVICE}] Factura generada para pedido #{data['order_id']} "
              f"por ${data['amount']} (origen: {event['source']})", flush=True)
    elif event["type"] == "payments.completed":
        print(f"[{SERVICE}] Pago #{data['payment_id']} conciliado con pedido "
              f"#{data['order_id']} via {data['method']}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    for key in BINDINGS:
        ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=key)
    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print(f"[{SERVICE}] Esperando eventos {BINDINGS} en '{QUEUE}'...", flush=True)
    ch.start_consuming()


if __name__ == "__main__":
    main()

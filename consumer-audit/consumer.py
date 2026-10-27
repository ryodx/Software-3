"""Consumidor de AUDITORIA (Python).
Cola 'audit.queue' enlazada a '#' (recibe TODOS los eventos).
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
QUEUE = "audit.queue"
BINDINGS = ["#"]
SERVICE = "consumer-audit"
counter = 0


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
    global counter
    counter += 1
    event = json.loads(body)
    print(f"[{SERVICE}] #{counter} AUDITADO {method.routing_key} "
          f"id={event['event_id'][:8]} de {event['source']} @ {event['timestamp']}", flush=True)
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

"""Productor de PEDIDOS (Python).
Publica eventos 'orders.created' en el exchange topic 'eventos'.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "admin")
PASS = os.getenv("RABBITMQ_PASS", "admin123")
EXCHANGE = os.getenv("EXCHANGE", "eventos")
ROUTING_KEY = "orders.created"
INTERVAL = float(os.getenv("INTERVAL", "4"))
SERVICE = "producer-orders"

PRODUCTS = ["Laptop", "Mouse", "Teclado", "Monitor", "Audifonos"]


def connect():
    creds = pika.PlainCredentials(USER, PASS)
    params = pika.ConnectionParameters(HOST, PORT, "/", creds, heartbeat=60)
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print(f"[{SERVICE}] RabbitMQ no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def main():
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    print(f"[{SERVICE}] Conectado. Publicando en '{EXCHANGE}' con key '{ROUTING_KEY}'", flush=True)
    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "type": ROUTING_KEY,
            "source": SERVICE,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "order_id": random.randint(1000, 9999),
                "product": random.choice(PRODUCTS),
                "quantity": random.randint(1, 5),
                "amount": round(random.uniform(20, 1500), 2),
            },
        }
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(event),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(f"[{SERVICE}] -> {ROUTING_KEY}: pedido #{event['data']['order_id']}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

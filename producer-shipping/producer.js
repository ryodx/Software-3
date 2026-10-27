// Productor de ENVIOS (Node.js).
// Publica eventos 'shipping.dispatched' en el exchange topic 'eventos'.
const amqp = require("amqplib");
const crypto = require("crypto");

const HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const PORT = process.env.RABBITMQ_PORT || "5672";
const USER = process.env.RABBITMQ_USER || "admin";
const PASS = process.env.RABBITMQ_PASS || "admin123";
const EXCHANGE = process.env.EXCHANGE || "eventos";
const ROUTING_KEY = "shipping.dispatched";
const INTERVAL = parseFloat(process.env.INTERVAL || "7") * 1000;
const SERVICE = "producer-shipping";
const CARRIERS = ["Servientrega", "Coordinadora", "Interrapidisimo", "DHL"];
const CITIES = ["Bucaramanga", "Bogota", "Medellin", "Cali", "Barranquilla"];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rnd = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const pick = (arr) => arr[rnd(0, arr.length - 1)];

async function connect() {
  const url = `amqp://${USER}:${PASS}@${HOST}:${PORT}`;
  while (true) {
    try {
      return await amqp.connect(url);
    } catch (e) {
      console.log(`[${SERVICE}] RabbitMQ no disponible, reintentando en 3s...`);
      await sleep(3000);
    }
  }
}

(async () => {
  const conn = await connect();
  const ch = await conn.createChannel();
  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  console.log(`[${SERVICE}] Conectado. Publicando en '${EXCHANGE}' con key '${ROUTING_KEY}'`);

  while (true) {
    const event = {
      event_id: crypto.randomUUID(),
      type: ROUTING_KEY,
      source: SERVICE,
      timestamp: new Date().toISOString(),
      data: {
        tracking: `TRK${rnd(100000, 999999)}`,
        order_id: rnd(1000, 9999),
        carrier: pick(CARRIERS),
        destination: pick(CITIES),
      },
    };
    ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(event)), {
      contentType: "application/json",
      persistent: true,
    });
    console.log(`[${SERVICE}] -> ${ROUTING_KEY}: guia ${event.data.tracking}`);
    await sleep(INTERVAL);
  }
})();

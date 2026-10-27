// Productor de PAGOS (Node.js).
// Publica eventos 'payments.completed' en el exchange topic 'eventos'.
const amqp = require("amqplib");
const crypto = require("crypto");

const HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const PORT = process.env.RABBITMQ_PORT || "5672";
const USER = process.env.RABBITMQ_USER || "admin";
const PASS = process.env.RABBITMQ_PASS || "admin123";
const EXCHANGE = process.env.EXCHANGE || "eventos";
const ROUTING_KEY = "payments.completed";
const INTERVAL = parseFloat(process.env.INTERVAL || "5") * 1000;
const SERVICE = "producer-payments";
const METHODS = ["tarjeta", "pse", "nequi", "efectivo"];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rnd = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

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
        payment_id: rnd(10000, 99999),
        order_id: rnd(1000, 9999),
        method: METHODS[rnd(0, METHODS.length - 1)],
        amount: +(Math.random() * 1480 + 20).toFixed(2),
      },
    };
    ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(event)), {
      contentType: "application/json",
      persistent: true,
    });
    console.log(`[${SERVICE}] -> ${ROUTING_KEY}: pago #${event.data.payment_id}`);
    await sleep(INTERVAL);
  }
})();

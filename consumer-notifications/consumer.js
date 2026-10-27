// Consumidor de NOTIFICACIONES (Node.js).
// Cola 'notifications.queue' enlazada a 'payments.*' y 'shipping.*'.
const amqp = require("amqplib");

const HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const PORT = process.env.RABBITMQ_PORT || "5672";
const USER = process.env.RABBITMQ_USER || "admin";
const PASS = process.env.RABBITMQ_PASS || "admin123";
const EXCHANGE = process.env.EXCHANGE || "eventos";
const QUEUE = "notifications.queue";
const BINDINGS = ["payments.*", "shipping.*"];
const SERVICE = "consumer-notifications";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
  await ch.assertQueue(QUEUE, { durable: true });
  for (const key of BINDINGS) await ch.bindQueue(QUEUE, EXCHANGE, key);
  ch.prefetch(10);
  console.log(`[${SERVICE}] Esperando eventos ${JSON.stringify(BINDINGS)} en '${QUEUE}'...`);

  ch.consume(QUEUE, (msg) => {
    if (!msg) return;
    const event = JSON.parse(msg.content.toString());
    const d = event.data;
    if (event.type === "payments.completed") {
      console.log(`[${SERVICE}] Email enviado: pago #${d.payment_id} recibido ($${d.amount})`);
    } else if (event.type === "shipping.dispatched") {
      console.log(`[${SERVICE}] SMS enviado: guia ${d.tracking} (${d.carrier}) hacia ${d.destination}`);
    }
    ch.ack(msg);
  });
})();

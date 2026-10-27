// Consumidor de STOCK (Go).
// Cola 'stock.queue' enlazada a 'orders.created' e 'inventory.*'.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	service = "consumer-stock"
	queue   = "stock.queue"
)

var bindings = []string{"orders.created", "inventory.*"}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func connect(url string) *amqp.Connection {
	for {
		c, err := amqp.Dial(url)
		if err == nil {
			return c
		}
		log.Printf("[%s] RabbitMQ no disponible, reintentando en 3s...", service)
		time.Sleep(3 * time.Second)
	}
}

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/",
		env("RABBITMQ_USER", "admin"), env("RABBITMQ_PASS", "admin123"),
		env("RABBITMQ_HOST", "rabbitmq"), env("RABBITMQ_PORT", "5672"))
	exchange := env("EXCHANGE", "eventos")

	conn := connect(url)
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	for _, key := range bindings {
		if err := ch.QueueBind(queue, key, exchange, false, nil); err != nil {
			log.Fatal(err)
		}
	}
	_ = ch.Qos(10, 0, false)

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%s] Esperando eventos %v en '%s'...", service, bindings, queue)

	for m := range msgs {
		var event struct {
			Type   string         `json:"type"`
			Source string         `json:"source"`
			Data   map[string]any `json:"data"`
		}
		if err := json.Unmarshal(m.Body, &event); err != nil {
			log.Printf("[%s] mensaje invalido: %v", service, err)
			_ = m.Nack(false, false)
			continue
		}
		switch event.Type {
		case "orders.created":
			log.Printf("[%s] Reservando %v x %v para pedido #%v", service,
				event.Data["quantity"], event.Data["product"], event.Data["order_id"])
		case "inventory.updated":
			log.Printf("[%s] Stock actualizado: %v (%v) = %v unidades", service,
				event.Data["sku"], event.Data["product"], event.Data["stock_new"])
		}
		_ = m.Ack(false)
	}
}

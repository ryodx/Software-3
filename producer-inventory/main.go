// Productor de INVENTARIO (Go).
// Publica eventos 'inventory.updated' en el exchange topic 'eventos'.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	service    = "producer-inventory"
	routingKey = "inventory.updated"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
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
	interval, _ := strconv.ParseFloat(env("INTERVAL", "6"), 64)

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
	log.Printf("[%s] Conectado. Publicando en '%s' con key '%s'", service, exchange, routingKey)

	items := []string{"Laptop", "Mouse", "Teclado", "Monitor", "Audifonos"}
	for {
		event := map[string]any{
			"event_id":  uuid(),
			"type":      routingKey,
			"source":    service,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"data": map[string]any{
				"sku":       fmt.Sprintf("SKU-%d", mrand.Intn(900)+100),
				"product":   items[mrand.Intn(len(items))],
				"stock_new": mrand.Intn(200),
			},
		}
		body, _ := json.Marshal(event)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		cancel()
		if err != nil {
			log.Fatalf("[%s] error publicando: %v", service, err)
		}
		d := event["data"].(map[string]any)
		log.Printf("[%s] -> %s: %v stock=%v", service, routingKey, d["sku"], d["stock_new"])
		time.Sleep(time.Duration(interval * float64(time.Second)))
	}
}

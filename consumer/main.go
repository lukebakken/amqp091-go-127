package main

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrLog = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
	Log    = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
	wg     sync.WaitGroup
)

func handler(d amqp.Delivery) {
	Log.Printf(
		"got %dB delivery: [%v] %q",
		len(d.Body),
		d.DeliveryTag,
		d.Body,
	)
}

func SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		Log.Printf("Ctrl+C pressed in Terminal")
		wg.Done()
		/*
		* TODO: shutdown consumers, channels and connection  gracefully
		* if err := rmq.Shutdown(); err != nil {
		* 	ErrLog.Fatalf("error during shutdown: %s", err)
		* }
		 */
	}()
}

func main() {
	Log.Println("Sleeping 5 seconds to allow RabbitMQ to start...")
	time.Sleep(5 * time.Second)
	Log.Println("Connecting to RabbitMQ...")

	SetupCloseHandler()

	cfg := new(tls.Config)

	// see at the top
	cfg.RootCAs = x509.NewCertPool()

	if ca, err := os.ReadFile("certs/ca_certificate.pem"); err == nil {
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}

	// Move the client cert and key to a location specific to your application
	// and load them here.

	if cert, err := tls.LoadX509KeyPair("certs/client_rabbitmq_certificate.pem", "certs/client_rabbitmq_key.pem"); err == nil {
		cfg.Certificates = append(cfg.Certificates, cert)
	}

	// see a note about Common Name (CN) at the top
	conn, err := amqp.DialTLS("amqps://rabbitmq/", cfg)
	if err != nil {
		ErrLog.Fatalf("connection.open: %s", err)
	}
	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		ErrLog.Fatalf("channel.open: %s", err)
	}
	defer c.Close()

	err = c.Qos(1, 0, false)
	if err != nil {
		ErrLog.Fatalf("basic.qos: %v", err)
	}

	const queue = "amqp091-go-127"

	_, err = c.QueueDeclare(queue, true, true, false, false, nil)
	if err != nil {
		ErrLog.Fatalf("queue.declare: %v", err)
	}

	delivery_chan, err := c.Consume(queue, "amqp091-go-127-consumer", false, false, false, false, nil)
	if err != nil {
		ErrLog.Fatalf("basic.consume: %v", err)
	}

	go func() {
		for msg := range delivery_chan {
			Log.Println("  got message")
			if e := msg.Ack(false); e != nil {
				Log.Printf("ack error: %+v", e)
			}
		}
	}()

	Log.Println("Consumer is running...")

	wg.Add(1)
	wg.Wait()
	Log.Println("EXITING")
}

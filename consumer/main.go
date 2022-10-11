package main

import (
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

	rmq := &RabbitMQ{}
	rmq.Connect("rabbitmq", "guest", "guest", "/")

	SetupCloseHandler()

	/*
	* Note: queue name MUST be the same as the routing key used for publish
	* as a direct exchange is the only option at this time
	 */
	go rmq.StartConsumer("amqp091-go-119", "amqp091-go-119", handler, 1)
	Log.Println("Consumer is running...")

	wg.Add(1)
	wg.Wait()
	Log.Println("EXITING")
}

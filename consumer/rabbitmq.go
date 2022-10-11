package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/getsentry/sentry-go"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type RabbitMQ struct {
	conn                  *amqp.Connection
	queues                map[string]amqp.Queue
	connString            string
	rabbitConnCloseError  chan *amqp.Error
	rabbitChannCloseError chan *amqp.Error
	recoveryConsumer      []RecoveryConsumer
	ch                    *amqp.Channel
	// exchange_name string
}

type RecoveryConsumer struct {
	queueName   string
	routingKey  string
	handler     func(d amqp.Delivery)
	concurrency int8
}

type (
	Delivery = amqp.Delivery
)

func (r *RabbitMQ) IfExist(queueName string) bool {
	for _, item := range r.recoveryConsumer {
		if item.queueName == queueName {
			return false
		}
	}
	return true
}

func (r *RabbitMQ) RecoverConsumers() {
	for _, i := range r.recoveryConsumer {
		go r.StartConsumer(i.queueName, i.routingKey, i.handler, int(i.concurrency))
		log.Infof("Consumer for %v successfully recovered", i.queueName)
	}
}

func (r *RabbitMQ) Reconnector() {
	for { //nolint //nolint
		select {
		case err := <-r.rabbitConnCloseError:
			log.Errorf("[RabbitMQ] Connection Closed : {'Reason': '%v', 'Code': '%v', 'Recoverable': '%v', 'Server_Side': '%v'", err.Reason, err.Code, err.Recover, err.Server)
			log.Debug("[RabbitMQ] Reconnecting after connection closed")
			sentry.CaptureException(fmt.Errorf("[RabbitMQ] Connection Closed : {'Reason': '%v', 'Code': '%v', 'Recoverable': '%v', 'Server_Side': '%v'", err.Reason, err.Code, err.Recover, err.Server))
			r.connection()
			r.RecoverConsumers()
		case err := <-r.rabbitChannCloseError:
			log.Errorf("[RabbitMQ] Channel Closed : {'Reason': '%v', 'Code': '%v', 'Recoverable': '%v', 'Server_Side': '%v'", err.Reason, err.Code, err.Recover, err.Server)
			log.Debug("[RabbitMQ] Reconnecting after channel closed")
			sentry.CaptureException(fmt.Errorf("[RabbitMQ] Channel Closed : {'Reason': '%v', 'Code': '%v', 'Recoverable': '%v', 'Server_Side': '%v'", err.Reason, err.Code, err.Recover, err.Server))
			r.ch.Close()
			r.RecoverConsumers()
		}
	}
}

func (r *RabbitMQ) Connect(host string, user string, pass string, virthost string) {
	r.connString = "amqp://" + user + ":" + pass + "@" + host + "/"
	if virthost != "/" || len(virthost) > 0 {
		r.connString += virthost
	}
	r.connection()
	go r.Reconnector()
}

func (r *RabbitMQ) connection() {
	if r.conn != nil {
		if !r.conn.IsClosed() {
			return
		} else {
			log.Info("Reconnecting to RabbitMQ...")
		}
	}

	var err error
	r.conn, err = amqp.Dial(r.connString)
	if err != nil {
		sentry.CaptureException(err)
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
	}
	r.conn.Config.Heartbeat = 5 * time.Second
	r.queues = make(map[string]amqp.Queue)

	r.rabbitConnCloseError = make(chan *amqp.Error)
	r.conn.NotifyClose(r.rabbitConnCloseError)
	log.Debug("[RabbitMQ] Successfully connected to RabbitMQ")
	log.Infof("Number of Active Thread/Goroutine %v", runtime.NumGoroutine())
}

func (r *RabbitMQ) CreateChannel() *amqp.Channel {
	ch, err := r.conn.Channel()
	if err != nil {
		log.Error(err)
		return nil
	}
	return ch
}

func (r *RabbitMQ) QueueAttach(ch *amqp.Channel, name string) {
	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare a queue", err)
	}
	r.queues[name] = q
	// r.ch.ExchangeDeclare()
}

func (r *RabbitMQ) TempQueueAttach(ch *amqp.Channel, name string) {
	_, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		log.Fatalf("%s: %s", "Failed to declare a temporary queue", err)
		sentry.CaptureException(fmt.Errorf("%s: %s", "Failed consume message", err))
	}
}

func (r *RabbitMQ) Publish(ch *amqp.Channel, queue string, body []byte) {
	span := sentry.StartSpan(context.TODO(), "publish message")
	defer span.Finish()
	err := ch.Publish(
		"",                   // exchange
		r.queues[queue].Name, // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			Headers:         map[string]interface{}{},
			ContentType:     "application/json",
			ContentEncoding: "",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			CorrelationId:   "",
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Now().UTC(),
			Type:            "",
			UserId:          "",
			AppId:           "",
			Body:            body,
		})
	if err != nil {
		sentry.CaptureException(err)
		log.Fatalf("%s: %s", "Failed to publish a message", err)
	}

	log.Debugf("Send message: %s", string(body))
}

func (r *RabbitMQ) StartConsumer(queueName string, routingKey string, handler func(d amqp.Delivery), concurrency int) {
	// prefetch 4x as many messages as we can handle at once
	var err error
	ok := r.IfExist(queueName)
	if ok {
		r.recoveryConsumer = append(r.recoveryConsumer, RecoveryConsumer{
			queueName:   queueName,
			routingKey:  routingKey,
			handler:     handler,
			concurrency: int8(concurrency),
		})
	}

	r.ch, err = r.conn.Channel()
	if err != nil {
		log.Error(err)
	}

	r.ch.NotifyClose(r.rabbitChannCloseError)

	prefetchCount := concurrency * 1
	err = r.ch.Qos(prefetchCount, 0, false)
	if err != nil {
		sentry.CaptureException(err)
		log.Errorf("%s: %s", "Failed QOS", err)
	}
	r.QueueAttach(r.ch, queueName)

	msgs, err := r.ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		sentry.CaptureException(err)
		log.Fatalf("%s: %s", "Failed consume message", err)
		sentry.CaptureException(fmt.Errorf("%s: %s", "Failed consume message", err))
		os.Exit(1)
	}

	go func() {
		for msg := range msgs {
			handler(msg)
		}
		log.Error("[RabbitMQ] Rabbit consumer closed")
	}()
}

func (r *RabbitMQ) WaitMessage(ch *amqp.Channel, queueName string, timeout time.Duration) []byte {
	st := time.Now()
	for time.Since(st).Seconds() < 1 {
		msg, ok, err := ch.Get(queueName, true)
		if err != nil {
			log.Errorf("Can't consume queue. Error: %s", err.Error())
			sentry.CaptureException(err)
			return nil
		}
		if ok {
			return msg.Body
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

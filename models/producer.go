package models

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/1046102779/message_middleware/conf"

	"github.com/Shopify/sarama"
)

var (
	producer            *Producer
	MessageInputChannel chan *sarama.ProducerMessage
)

type Producer struct {
	Client sarama.AsyncProducer
}

func GetProducerInstance() *Producer {
	mux.Lock()
	defer mux.Unlock()
	if producer == nil {
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
		config.Producer.RequiredAcks = sarama.NoResponse
		config.Producer.Compression = 2
		config.Producer.Timeout = 1000 * time.Millisecond
		client, err := sarama.NewClient(conf.KafkaClusterServers, config)
		if err != nil {
			panic(err)
		}
		producer = new(Producer)
		producer.Client, err = sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			panic(err)
		}
	}
	return producer
}

func init() {
	if producer = GetProducerInstance(); producer == nil {
		panic("kafka Producer initital failed.")
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	MessageInputChannel = make(chan *sarama.ProducerMessage, 10000)
	var enqueued, errors int
	go func() {
		for {
			select {
			case message := <-MessageInputChannel:
				producer.Client.Input() <- message
			case <-producer.Client.Successes():
				enqueued++
			case err := <-producer.Client.Errors():
				log.Println("send failed. ", err)
				errors++
			case <-signals:
				break
			}
		}
		log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
		defer func() {
			if err := producer.Client.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
	}()
}

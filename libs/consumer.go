package libs

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/1046102779/message_middleware/conf"
	pb "github.com/1046102779/message_middleware/igrpc"
	"github.com/Shopify/sarama"
)

var (
	consumer *Consumer
	mux      sync.RWMutex
	wg       sync.WaitGroup
)

type Consumer struct {
	Client   sarama.Consumer
	TopicMap map[string]interface{} // 消息topic所对应的[]byte解析结构体
}

func init() {
	if consumer = GetConsumerInstance(); consumer == nil {
		panic("kafka Consumer client is nil")
	}
	fmt.Println("kafka consumer init...")
	// 消费者订阅所有的topic
	for index := 0; conf.Topics != nil && index < len(conf.Topics); index++ {
		if strings.TrimSpace(conf.Topics[index]) != "" {
			switch conf.Topics[index] {
			case "template":
				consumer.TopicMap[conf.Topics[index]] = new(pb.SaleOrderTemplateMessage)
			case "sms":
				consumer.TopicMap[conf.Topics[index]] = new(pb.SmsMessage)
			case "email":
				consumer.TopicMap[conf.Topics[index]] = new(pb.EmailMessage)
			}
			go consumer.StartConsumerMQ(conf.Topics[index])
		}
	}
}

func GetConsumerInstance() *Consumer {
	var (
		err error
	)
	mux.Lock()
	defer mux.Unlock()
	if consumer == nil {
		consumer = new(Consumer)
		consumer.TopicMap = make(map[string]interface{})
		consumer.Client, err = sarama.NewConsumer(conf.KafkaClusterServers, nil)
		if err != nil {
			panic(err)
		}
	}
	return consumer
}

func (t *Consumer) StartConsumerMQ(topic string) {
	partitions, err := t.Client.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for partition := range partitions {
		partitionConsumer, err := t.Client.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		defer partitionConsumer.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range partitionConsumer.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				if err = t.SendMessage(topic, msg.Value); err != nil {
					log.Println("send message failed. msg= ", err.Error())
				}
			}
		}(partitionConsumer)
	}
	wg.Wait()
}

func (t *Consumer) SendMessage(topic string, msg []byte) (err error) {
	if err = json.Unmarshal(msg, t.TopicMap[topic]); err != nil {
		log.Println("json unmarshal failed. msg=", err.Error())
		return
	}
	switch topic {
	case "template":
		conf.OfficialAccountClient.Call(fmt.Sprintf("%s.%s", "official_accounts", "SendMessage"), t.TopicMap[topic], t.TopicMap[topic])
	case "sms":
		conf.SmsClient.Call(fmt.Sprintf("%s.%s", "sms", "SendSingleSms"), t.TopicMap[topic], t.TopicMap[topic])
	case "email":
		//conf.EmailClient.Call(fmt.Sprintf("%s.%s", "email", "SendEmail"), t.TopicMap[topic], t.TopicMap[topic])
	}
	return
}

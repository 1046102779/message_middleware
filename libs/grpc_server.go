package libs

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"

	pb "github.com/1046102779/message_middleware/igrpc"
)

type ProducerConsumerServer struct{}

func (t *ProducerConsumerServer) PublishSaleOrderMessage(in *pb.SaleOrderTemplateMessage, out *pb.SaleOrderTemplateMessage) (err error) {
	log.Printf("[%v] enter PublishSaleOrderMessage", in.SaleOrderId)
	defer log.Printf("[%v] left PublishSaleOrderMessage", in.SaleOrderId)
	var bts []byte
	defer func() { err = nil }()
	if bts, err = json.Marshal(*in); err != nil {
		log.Println(err.Error())
		return
	}
	msg := &sarama.ProducerMessage{
		Topic:     "template",
		Partition: int32(-1),
		Key:       sarama.StringEncoder("Random"),
	}
	msg.Value = sarama.ByteEncoder(bts)
	MessageInputChannel <- msg
	return
}

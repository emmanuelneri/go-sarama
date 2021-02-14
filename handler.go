package go_sarama

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
)

type ConsumerHandler struct {
	ctx           context.Context
	ctxCancel     context.CancelFunc
	ready         chan bool
	consumedChan  chan sarama.ConsumerMessage
	processedChan chan sarama.ConsumerMessage
}

func newConsumerHandler() *ConsumerHandler {
	return &ConsumerHandler{
		consumedChan:  make(chan sarama.ConsumerMessage),
		processedChan: make(chan sarama.ConsumerMessage),
		ready:         make(chan bool),
	}
}

func (ch *ConsumerHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("ConsumerHandler setup id %d - memberID %s \n", s.GenerationID(), s.MemberID())
	ctx, cancelFunc := context.WithCancel(context.Background())
	ch.ctx = ctx
	ch.ctxCancel = cancelFunc
	close(ch.ready)
	return nil
}

func (ch *ConsumerHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("ConsumerHandler setup id %d - memberID %s \n", s.GenerationID(), s.MemberID())
	ch.ctxCancel()
	return nil
}

func (ch *ConsumerHandler) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("ConsumerHandler consumeClaim topic %s,partition %d, offset %d \n", claim.Topic(), claim.Partition(), claim.InitialOffset())
	ch.startAck(s)
	for m := range claim.Messages() {
		ch.consumedChan <- *m
	}

	return nil
}

func (ch *ConsumerHandler) Ack(m sarama.ConsumerMessage) {
	ch.processedChan <- m
}

func (ch *ConsumerHandler) startAck(s sarama.ConsumerGroupSession) {
	log.Printf("starting ack listener setup id %d - memberID %s \n", s.GenerationID(), s.MemberID())

	go func() {
		for {
			if ch.ctx.Err() != nil {
				return
			}

			m := <-ch.processedChan
			s.MarkMessage(&m, "")
		}
	}()
}

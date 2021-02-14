package go_sarama

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
}

func NewConsumerGroup(brokersAddress []string, groupID string) Consumer {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Return.Errors = true
	config.ClientID = "kafka-client"
	sarama.Logger = log.New(os.Stdout, "[Sarama] - ", log.LstdFlags)

	consumerGroup, err := sarama.NewConsumerGroup(brokersAddress, groupID, config)
	if err != nil {
		panic(err)
	}

	return Consumer{consumerGroup: consumerGroup}
}

func (c *Consumer) Start(ctx context.Context, topics []string) {
	handler := newConsumerHandler()
	go func() {
		for {
			err := c.consumerGroup.Consume(ctx, topics, handler)
			if err != nil {
				log.Fatalf("consume group error %v \n", err)
			}

			if ctx.Err() != nil {
				log.Printf("context cancelled %v \n", err)
				return
			}

			handler.ready = make(chan bool)
		}
	}()
	<-handler.ready

	for {
		m := <-handler.consumedChan
		log.Printf("message consumed %v \n", m)
		handler.processedChan <- m
	}
}

func (c *Consumer) Stop() {
	if err := c.consumerGroup.Close(); err != nil {
		log.Fatalf("fail to close consumerGroup %v \n", err)
	}
}

package go_sarama

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"go-sarama/test"
	"log"
	"strconv"
	"testing"
)

const groupID = "go-kafka-client"

func TestStart(t *testing.T) {
	it := test.NewIntegrationTest()
	it.Start(t, "simple-consume")
	defer it.Stop()

	t.Run("should commit offset when consume message from topic", func(t *testing.T) {
		personTopic := "person"
		topicsHandler := map[string]func(m *sarama.ConsumerMessage){
			personTopic: func(m *sarama.ConsumerMessage) {
				log.Printf("message consumed %v \n", m)
			},
		}

		testEnv := newConsumerTestEnv()
		testEnv.start(topicsHandler)

		assert.Nil(t, it.ProduceMessage(personTopic, []byte("1"), []byte("Person 1")))
		assert.Nil(t, it.ProduceMessage(personTopic, []byte("2"), []byte("Person 2")))
		assert.Nil(t, it.ProduceMessage(personTopic, []byte("3"), []byte("Person 3")))
		it.AssertLastOffset(t, personTopic, 3)
		it.AssertLastGroupOffset(t, groupID, personTopic, 0, 3)

		testEnv.stop()
	})

	t.Run("should commit offset when consume message from multiple topics", func(t *testing.T) {
		topicsHandler := map[string]func(m *sarama.ConsumerMessage){}

		for i := 0; i < 5; i++ {
			topicName := "topic-" + strconv.Itoa(i)
			topicsHandler[topicName] = func(m *sarama.ConsumerMessage) {
				log.Printf("%s consumed: %v \n", topicName, m)
			}

			for j := 0; j < i+1; j++ {
				assert.Nil(t, it.ProduceMessage(topicName, []byte(strconv.Itoa(j)), []byte("Message "+strconv.Itoa(j))))
			}
		}

		testEnv := newConsumerTestEnv()
		testEnv.start(topicsHandler)

		for i := 0; i < 5; i++ {
			topicName := "topic-" + strconv.Itoa(i)
			quantityExpected := int64(i + 1)
			it.AssertLastOffset(t, topicName, quantityExpected)
			it.AssertLastGroupOffset(t, groupID, topicName, 0, quantityExpected)
		}

		testEnv.stop()
	})

	// TODO process sequence
	// TODO parallel process between topics
}

type ConsumerTestEnv struct {
	ctx           context.Context
	cancelCtxFunc context.CancelFunc
	consumer      Consumer
}

func newConsumerTestEnv() ConsumerTestEnv {
	consumer := NewConsumerGroup(test.BrokersAddress, groupID)
	ctx, cancel := context.WithCancel(context.Background())

	return ConsumerTestEnv{
		ctx:           ctx,
		cancelCtxFunc: cancel,
		consumer:      consumer,
	}
}

func (t *ConsumerTestEnv) start(topicsHandler map[string]func(m *sarama.ConsumerMessage)) {
	go t.consumer.Start(t.ctx, topicsHandler)
}

func (t *ConsumerTestEnv) stop() {
	t.cancelCtxFunc()
	t.consumer.Stop()
}

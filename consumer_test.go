package go_sarama

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go-sarama/test"
	"testing"
)

func TestStart(t *testing.T) {
	it := test.NewIntegrationTest()
	it.Start(t, "simple-consume")
	defer it.Stop()

	t.Run("should commit offset when consume message from topic", func(t *testing.T) {
		personTopic := "person"
		topics := []string{personTopic}

		groupID := "go-kafka-client"
		consumer := NewConsumerGroup(test.BrokersAddress, groupID)

		ctx, cancel := context.WithCancel(context.Background())
		go consumer.Start(ctx, topics)

		assert.Nil(t, it.ProduceMessage(personTopic, []byte("1"), []byte("Person 1")))
		assert.Nil(t, it.ProduceMessage(personTopic, []byte("2"), []byte("Person 2")))
		assert.Nil(t, it.ProduceMessage(personTopic, []byte("3"), []byte("Person 3")))
		it.AssertLastOffset(t, personTopic, 3)
		it.AssertLastGroupOffset(t, groupID, personTopic, 0, 3)

		cancel()
		consumer.Stop()
	})
}

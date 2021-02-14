package test

import (
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"strings"
	"testing"
	"time"
)

const (
	kafkaConnectionWaitDuration          = 1 * time.Second
	kafkaConnectionWaitIterations        = 60
	ConsumerGroupCommittedWaitDuration   = 1 * time.Second
	ConsumerGroupCommittedWaitIterations = 10
)

var (
	BrokersAddress      = []string{"localhost:9092"}
	composeFilePaths    = []string{"testdata/docker-compose.yml"}
	composeStartCommand = []string{"up", "-d"}
)

type IntegrationTest struct {
	compose  *testcontainers.LocalDockerCompose
	Producer sarama.SyncProducer
}

func NewIntegrationTest() IntegrationTest {
	return IntegrationTest{}
}

func (it *IntegrationTest) Start(t *testing.T, name string) {
	identifier := strings.ToLower(name + uuid.New().String())
	it.compose = testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	execError := it.compose.WithCommand(composeStartCommand).Invoke()
	if err := execError.Error; err != nil {
		t.Fatal(err)
	}

	WaitForCondition("kafka", kafkaConnectionWaitDuration, kafkaConnectionWaitIterations, func() bool {
		return isKafkaConnected(BrokersAddress)
	})

	syncProducer, err := startProducer(BrokersAddress)
	if err != nil {
		t.Fatal(err)
	}

	it.Producer = syncProducer
}

func (it *IntegrationTest) ProduceMessage(topic string, key []byte, value []byte) error {
	p, o, err := it.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	})

	if err != nil {
		return err
	}

	log.Printf("message produced. partition: %d - offset: %d \n", p, o)
	return nil
}

func (it *IntegrationTest) AssertLastOffset(t *testing.T, topic string, expectedOffset int64) {
	client, err := startKafkaClient(BrokersAddress)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	offset, err := client.GetOffset(topic, 0, sarama.OffsetNewest)
	assert.Nil(t, err)
	assert.Equal(t, expectedOffset, offset)
}

func (it *IntegrationTest) AssertLastGroupOffset(t *testing.T, groupID string, topic string, partition int32, expectedOffset int64) {
	admin, err := startKafkaAdmin(BrokersAddress)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := admin.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	WaitForCondition("offset committed", ConsumerGroupCommittedWaitDuration, ConsumerGroupCommittedWaitIterations, func() bool {
		offset, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: {partition}})
		if err != nil || offset == nil {
			return false
		}

		topicPartitions := offset.Blocks[topic]
		return topicPartitions[partition] != nil && topicPartitions[partition].Offset == expectedOffset
	})

	offset, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: {partition}})
	assert.Nil(t, err)
	assert.NotNil(t, offset)
	topicPartitions := offset.Blocks[topic]
	assert.NotNil(t, topicPartitions)
	assert.NotNil(t, topicPartitions[partition])
	assert.Equal(t, expectedOffset, topicPartitions[partition].Offset)
}

func (it *IntegrationTest) Stop() {
	if err := it.Producer.Close(); err != nil {
		log.Fatalf("fail to close Producer %v \n", err)
	}

	it.compose.Down()
}

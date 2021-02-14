package test

import (
	"github.com/Shopify/sarama"
	"log"
)

func isKafkaConnected(brokersAddress []string) bool {
	client, err := startKafkaClient(brokersAddress)
	if err != nil {
		return false
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	brokers, err := client.Controller()
	if err != nil {
		return false
	}

	defer func() {
		if err := brokers.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	connected, err := brokers.Connected()
	if err != nil {
		return false
	}

	return connected
}

func startKafkaAdmin(brokersAddress []string) (sarama.ClusterAdmin, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	return sarama.NewClusterAdmin(brokersAddress, config)
}

func startKafkaClient(brokersAddress []string) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	return sarama.NewClient(brokersAddress, config)
}

func startProducer(brokersAddress []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return sarama.NewSyncProducer(brokersAddress, config)
}

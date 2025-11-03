package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

func main() {
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions"

	log.Println("🚀 Transaction Service (Producer with Schema Registry) starting...")

	// Create Schema Registry client
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatalf("❌ Failed to create schema registry client: %v", err)
	}

	// ดึง schema จาก Schema Registry
	subject := fmt.Sprintf("%s-value", topic)
	latestSchema, err := srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		log.Fatalf("❌ Failed to get schema: %v", err)
	}

	// Parse schema ด้วย goavro
	codec, err := goavro.NewCodec(latestSchema.Schema)
	if err != nil {
		log.Fatalf("❌ Failed to parse schema: %v", err)
	}

	log.Printf("✅ Got schema ID: %d", latestSchema.ID)

	// --- Kafka producer ---
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	log.Println("✅ Connected to Kafka and Schema Registry")

	// --- ตัวอย่างข้อมูล ---
	type Transaction struct {
		ID         string
		Type       string
		TerminalID int64
		ReceivedAt string
	}

	transactions := []Transaction{
		{ID: "1001", Type: "CONTROL", TerminalID: 1, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
		{ID: "1002", Type: "CONTROL", TerminalID: 2, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
		{ID: "1003", Type: "CONTROL", TerminalID: 3, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
	}

	// --- ส่ง message ---
	for i, txn := range transactions {
		// สร้าง map โดยใช้ชื่อ field ตรงกับ Schema Registry (ID, Type, TerminalID, ReceivedAt)
		record := map[string]interface{}{
			"ID":         txn.ID,
			"Type":       txn.Type,
			"TerminalID": txn.TerminalID,
			"ReceivedAt": txn.ReceivedAt,
		}

		// Serialize ด้วย goavro
		avroBytes, err := codec.BinaryFromNative(nil, record)
		if err != nil {
			log.Printf("❌ Avro serialization failed for txn %d: %v", i, err)
			continue
		}

		// สร้าง Confluent Schema Registry format: [magic byte][schema ID][avro data]
		var schemaBuf bytes.Buffer
		schemaBuf.WriteByte(0) // magic byte
		binary.Write(&schemaBuf, binary.BigEndian, int32(latestSchema.ID))
		schemaBuf.Write(avroBytes)

		value := schemaBuf.Bytes()

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(txn.ID),
			Value: value,
		}

		if err := producer.Produce(msg, nil); err != nil {
			log.Printf("❌ Failed to send txn %d: %v", i, err)
			continue
		}

		log.Printf("✅ Sent Txn[%d]: ID=%s Type=%s Terminal=%d",
			i+1, txn.ID, txn.Type, txn.TerminalID)

		time.Sleep(time.Second)
	}

	producer.Flush(15 * 1000)
	log.Println("✅ All transactions sent successfully!")
}

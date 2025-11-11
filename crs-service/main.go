package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// SchemaMetadata เก็บข้อมูล schema จาก Schema Registry (wrapper สำหรับ srclient.Schema)
type SchemaMetadata struct {
	Subject string
	Version int
	ID      int
	Schema  string
}

// convertSchemaToMetadata แปลง srclient.Schema เป็น SchemaMetadata
func convertSchemaToMetadata(schema *srclient.Schema, subject string) *SchemaMetadata {
	if schema == nil {
		return nil
	}
	return &SchemaMetadata{
		Subject: subject,
		Version: schema.Version(),
		ID:      schema.ID(),
		Schema:  schema.Schema(),
	}
}

// Transaction represents the transaction data structure
type Transaction struct {
	ID         string
	Type       string
	TerminalID int64
	ReceivedAt string
}

// Config holds application configuration
type Config struct {
	BootstrapServers  string
	SchemaRegistryURL string
	Topic             string
	TopicDLQ          string
	GroupID           string
}

// Consumer interface
type Consumer interface {
	Consume(ctx context.Context)
	Process(msg []byte) error
}

type consumerImp struct {
	cfg          *Config
	kafka        kafka.Kafka
	srClient     *srclient.SchemaRegistryClient
	latestSchema *SchemaMetadata
	codec        *goavro.Codec
	subject      string
}

func NewConsumer(
	cfg *Config,
	kafkaClient kafka.Kafka,
	srClient *srclient.SchemaRegistryClient,
) (Consumer, error) {
	// Verify Schema Registry connection by fetching latest schema
	subject := cfg.Topic
	latestSchemaSR, err := srClient.GetLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to verify Schema Registry connection: %w", err)
	}
	latestSchema := convertSchemaToMetadata(latestSchemaSR, subject)
	log.Printf("✅ Schema Registry connection verified (subject: %s, schema ID: %d)", subject, latestSchema.ID)

	// ใช้ schema.Codec() โดยตรงจาก srclient แทน goavro.NewCodec()
	codec := latestSchemaSR.Codec()
	if codec == nil {
		return nil, fmt.Errorf("failed to get codec from schema")
	}
	log.Println("✅ Avro codec created successfully")

	return &consumerImp{
		cfg:          cfg,
		kafka:        kafkaClient,
		srClient:     srClient,
		latestSchema: latestSchema,
		codec:        codec,
		subject:      subject,
	}, nil
}

func (c *consumerImp) Consume(ctx context.Context) {
	log.Printf("consumer topic %s starting...", c.cfg.Topic)

	err := c.kafka.Consume(ctx, c.cfg.Topic, c.cfg.GroupID, func(msg *sarama.ConsumerMessage) error {
		log.Printf("receive key: %s", string(msg.Key))

		c.kafka.RetryHandler(msg,
			func(m *sarama.ConsumerMessage) error {
				if err := c.Process(m.Value); err != nil {
					log.Printf("❌ process transaction error: %v", err)
					return err
				}

				return nil
			},
			func(m *sarama.ConsumerMessage, errMsg string) {
				_, err := c.kafka.SendToDLQ(kafka.SendToDLQParam{
					TopicDlq:     c.cfg.TopicDLQ,
					Msg:          m,
					ErrorMessage: errMsg,
				})
				if err != nil {
					log.Printf("❌ send to dlq error: %v", err)
				} else {
					log.Printf("⚠️  message sent to DLQ: %s (error: %s)", string(m.Key), errMsg)
				}
			},
		)

		log.Printf("done key: %s", string(msg.Key))
		return nil
	})

	if err != nil {
		log.Printf("❌ consume error: %v", err)
	}
}

func (c *consumerImp) deserialize(msgValue []byte) (Transaction, error) {
	// Extract Schema Registry header: [magic byte][schema ID][avro data]
	if len(msgValue) < 5 {
		return Transaction{}, errors.New("message too short")
	}

	magicByte := msgValue[0]
	schemaID := int32(msgValue[1])<<24 | int32(msgValue[2])<<16 | int32(msgValue[3])<<8 | int32(msgValue[4])
	avroData := msgValue[5:]

	if magicByte != 0 {
		return Transaction{}, fmt.Errorf("invalid magic byte: 0x%02x", magicByte)
	}

	currentCodec := c.codec
	// Verify schema ID matches
	if int(schemaID) != c.latestSchema.ID {
		log.Printf("⚠️  Schema ID mismatch: expected %d, got %d", c.latestSchema.ID, schemaID)
		// Try to fetch the correct schema using srclient
		schemaSR, err := c.srClient.GetSchema(int(schemaID))
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to fetch schema ID %d: %w", schemaID, err)
		}
		// ใช้ schema.Codec() โดยตรงจาก srclient
		currentCodec = schemaSR.Codec()
		if currentCodec == nil {
			return Transaction{}, fmt.Errorf("failed to get codec from schema ID %d", schemaID)
		}
		c.latestSchema.ID = int(schemaID)
		c.latestSchema.Schema = schemaSR.Schema()
		c.codec = currentCodec
	}

	// Deserialize Avro binary data
	native, _, err := currentCodec.NativeFromBinary(avroData)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to deserialize Avro data: %w", err)
	}

	// Convert to map
	txnMapTyped, ok := native.(map[string]interface{})
	if !ok {
		return Transaction{}, fmt.Errorf("deserialized data is not a map: %T", native)
	}

	// 🔍 Debug: Log all fields received (including extra fields if any)
	if len(txnMapTyped) > 0 {
		log.Printf("🔍 Raw deserialized fields: %v", txnMapTyped)
	}

	// ⚠️ ตรวจสอบ field ที่ไม่ได้ register ใน schema ก่อน deserialize
	if amount, exists := txnMapTyped["amount"]; exists {
		log.Printf("⚠️  WARNING: Found 'amount' field in message: %v (type: %T)", amount, amount)
		log.Printf("   ⚠️  This field was NOT in the registered schema!")
		log.Printf("   ⚠️  This means the producer sent extra data that was silently dropped!")
	}

	// Map Avro fields to Transaction struct
	txn := Transaction{
		ID:         getString(txnMapTyped, "id"),
		Type:       getString(txnMapTyped, "type"),
		TerminalID: getInt64(txnMapTyped, "terminal_id"),
		ReceivedAt: getString(txnMapTyped, "received_at"),
	}

	return txn, nil
}

func (c *consumerImp) Process(msgValue []byte) error {
	txn, err := c.deserialize(msgValue)
	if err != nil {
		log.Printf("❌ deserialize message error: %v", err)
		return err
	}

	log.Printf("🔧 Processing transaction ID: %s", txn.ID)

	// Test DLQ: Force failure for specific transaction IDs
	if txn.ID == "FAIL_TEST" || txn.ID == "DLQ_TEST" {
		return fmt.Errorf("💥 simulated processing failure for transaction ID: %s (for DLQ testing)", txn.ID)
	}

	// TODO: Add your business logic here
	// For example: save to database, call other services, etc.

	log.Printf("✅ Transaction %s processed successfully", txn.ID)
	return nil
}

func main() {
	// Configuration
	cfg := &Config{
		BootstrapServers:  "localhost:9094",
		SchemaRegistryURL: "http://localhost:8083",
		Topic:             "transactions-value",
		TopicDLQ:          "transactions-dlq",
		GroupID:           "crs-service-group",
	}

	log.Println("🚀 CRS Service (Consumer with Schema Registry) starting...")

	// Create Schema Registry client using srclient
	srClient := srclient.CreateSchemaRegistryClient(cfg.SchemaRegistryURL)
	// Enable codec creation เพื่อให้สามารถใช้ schema.Codec() ได้
	srClient.CodecCreationEnabled(true)

	// Create Kafka consumer using common library
	kafkaClient, cleanup := kafka.NewKafka(kafka.KafkaConfig{
		Brokers:     cfg.BootstrapServers,
		EnforceTls:  false,
		AuthEnabled: false,
		Retry:       3,
		ClientID:    cfg.GroupID,
	})
	defer cleanup()

	// Create consumer
	consumer, err := NewConsumer(cfg, kafkaClient, srClient)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}

	log.Printf("✅ Subscribed to topic: %s", cfg.Topic)
	log.Printf("✅ DLQ topic: %s", cfg.TopicDLQ)
	log.Println("⏳ Waiting for transactions...")

	// Create context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("⚠️  Received signal: %v, shutting down gracefully...", sig)
		cancel()
	}()

	// Start consuming
	consumer.Consume(ctx)

	log.Println("✅ Consumer stopped gracefully")
}

// getString extracts string value from map, returns empty string if not found or wrong type
func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// getInt64 extracts int64 value from map, handles both int32 and int64, returns 0 if not found or wrong type
func getInt64(m map[string]interface{}, key string) int64 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case int64:
			return v
		case int32:
			return int64(v)
		case int:
			return int64(v)
		}
	}
	return 0
}

package main

import (
	"context"
	"encoding/json"
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

// Transaction represents the transaction data structure
type Transaction struct {
	ID         string  `json:"id"`
	Type       string  `json:"type"`
	TerminalID int64   `json:"terminal_id"`
	Mam        string  `json:"mam"`
	ReceivedAt string  `json:"received_at"`
	Amount     float64 `json:"amount"`
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
	cfg      *Config
	kafka    kafka.Kafka
	srClient *srclient.SchemaRegistryClient
	// Unified cache: schemaID -> codec
	schemaCache map[int]*goavro.Codec // schemaID -> codec
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
	latestSchemaID := latestSchemaSR.ID()
	log.Printf("✅ Schema Registry connection verified (subject: %s, schema ID: %d)", subject, latestSchemaID)

	// ใช้ schema.Codec() โดยตรงจาก srclient แทน goavro.NewCodec()
	codec := latestSchemaSR.Codec()
	if codec == nil {
		return nil, fmt.Errorf("failed to get codec from schema")
	}
	log.Println("✅ Avro codec created successfully")

	// Initialize cache with latest schema codec
	schemaCache := make(map[int]*goavro.Codec)
	schemaCache[latestSchemaID] = codec

	return &consumerImp{
		cfg:         cfg,
		kafka:       kafkaClient,
		srClient:    srClient,
		schemaCache: schemaCache,
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

	schemaIDInt := int(schemaID)

	// Get codec from unified cache (works for both latest and older schemas)
	currentCodec, exists := c.schemaCache[schemaIDInt]
	if !exists {
		// Cache miss - fetch from Schema Registry
		log.Printf("⚠️  Schema ID %d not in cache, fetching from Schema Registry...", schemaIDInt)
		schemaSR, err := c.srClient.GetSchema(schemaIDInt)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to fetch schema ID %d: %w", schemaIDInt, err)
		}

		// Get codec from schema
		currentCodec = schemaSR.Codec()
		if currentCodec == nil {
			return Transaction{}, fmt.Errorf("failed to get codec from schema ID %d", schemaIDInt)
		}

		// Cache the codec for future use
		c.schemaCache[schemaIDInt] = currentCodec
		log.Printf("✅ Cached codec for schema ID %d", schemaIDInt)
	}

	// Deserialize Avro binary data
	native, _, err := currentCodec.NativeFromBinary(avroData)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to deserialize Avro data: %w", err)
	}

	// Verify it's a map (Avro records always deserialize to map[string]interface{})
	if _, ok := native.(map[string]interface{}); !ok {
		return Transaction{}, fmt.Errorf("deserialized data is not a map: %T", native)
	}

	// 🔍 Debug: Log all fields received (including extra fields if any)
	// if txnMap, ok := native.(map[string]interface{}); ok && len(txnMap) > 0 {
	// 	log.Printf("🔍 Raw deserialized fields: %v", txnMap)
	// }

	// Convert native (map[string]interface{}) to Transaction struct using JSON marshal/unmarshal
	// This is cleaner and handles type conversions automatically
	jsonBytes, err := json.Marshal(native)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal native to JSON: %w", err)
	}

	var txn Transaction
	if err := json.Unmarshal(jsonBytes, &txn); err != nil {
		return Transaction{}, fmt.Errorf("failed to unmarshal JSON to Transaction: %w", err)
	}

	fmt.Println("txn", txn)

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

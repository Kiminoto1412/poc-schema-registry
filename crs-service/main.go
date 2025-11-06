package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

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
	Process(txn Transaction) error
}

type consumerImp struct {
	cfg          *Config
	kafka        kafka.Kafka
	srClient     schemaregistry.Client
	latestSchema schemaregistry.SchemaMetadata
	codec        *goavro.Codec
	subject      string
	// DLQ Avro support
	dlqSchema  schemaregistry.SchemaMetadata
	dlqCodec   *goavro.Codec
	dlqSubject string
}

func NewConsumer(
	cfg *Config,
	kafkaClient kafka.Kafka,
	srClient schemaregistry.Client,
) (Consumer, error) {
	// Verify Schema Registry connection by fetching latest schema
	subject := fmt.Sprintf("%s-value", cfg.Topic)
	latestSchema, err := srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to verify Schema Registry connection: %w", err)
	}
	fmt.Println(71, "latestSchema", latestSchema)
	log.Printf("✅ Schema Registry connection verified (subject: %s, schema ID: %d)", subject, latestSchema.ID)

	// Parse schema ด้วย goavro
	codec, err := goavro.NewCodec(latestSchema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}
	log.Println("✅ Avro codec created successfully")
	fmt.Println(80, "codec", codec)
	// Load DLQ schema (get or register)
	dlqSubject := fmt.Sprintf("%s-value", cfg.TopicDLQ)
	dlqSchema, dlqCodec, err := getOrRegisterDLQSchema(srClient, dlqSubject, cfg.SchemaRegistryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to setup DLQ schema: %w", err)
	}
	log.Printf("✅ DLQ schema ready (subject: %s, schema ID: %d)", dlqSubject, dlqSchema.ID)

	return &consumerImp{
		cfg:          cfg,
		kafka:        kafkaClient,
		srClient:     srClient,
		latestSchema: latestSchema,
		codec:        codec,
		subject:      subject,
		dlqSchema:    dlqSchema,
		dlqCodec:     dlqCodec,
		dlqSubject:   dlqSubject,
	}, nil
}

func (c *consumerImp) Consume(ctx context.Context) {
	log.Printf("consumer topic %s starting...", c.cfg.Topic)

	err := c.kafka.Consume(ctx, c.cfg.Topic, c.cfg.GroupID, func(msg *sarama.ConsumerMessage) error {
		log.Printf("receive key: %s", string(msg.Key))

		c.kafka.RetryHandler(msg,
			func(m *sarama.ConsumerMessage) error {
				// Deserialize Avro message
				txn, err := c.deserializeMessage(m)
				if err != nil {
					log.Printf("❌ deserialize message error: %v", err)
					return err
				}

				// Process transaction
				if err := c.Process(txn); err != nil {
					log.Printf("❌ process transaction error: %v", err)
					return err
				}

				return nil
			},
			func(m *sarama.ConsumerMessage, errMsg string) {
				// Send to DLQ using Avro format
				retryCount := 3 // Default retry count from config
				if err := c.sendToDLQAvro(m, errMsg, retryCount); err != nil {
					log.Printf("❌ send to dlq error: %v", err)
					// Fallback to JSON format if Avro fails
					_, fallbackErr := c.kafka.SendToDLQ(kafka.SendToDLQParam{
						TopicDlq:     c.cfg.TopicDLQ,
						Msg:          m,
						ErrorMessage: errMsg,
					})
					if fallbackErr != nil {
						log.Printf("❌ fallback DLQ also failed: %v", fallbackErr)
					}
				} else {
					log.Printf("⚠️  message sent to DLQ (Avro): %s (error: %s)", string(m.Key), errMsg)
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

func (c *consumerImp) deserializeMessage(msg *sarama.ConsumerMessage) (Transaction, error) {
	// Extract Schema Registry header: [magic byte][schema ID][avro data]
	if len(msg.Value) < 5 {
		return Transaction{}, errors.New("message too short")
	}

	magicByte := msg.Value[0]
	schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
	avroData := msg.Value[5:]

	if magicByte != 0 {
		return Transaction{}, fmt.Errorf("invalid magic byte: 0x%02x", magicByte)
	}

	currentCodec := c.codec
	// Verify schema ID matches
	if int(schemaID) != c.latestSchema.ID {
		log.Printf("⚠️  Schema ID mismatch: expected %d, got %d", c.latestSchema.ID, schemaID)
		// Try to fetch the correct schema
		schemaMeta, err := c.srClient.GetBySubjectAndID(c.subject, int(schemaID))
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to fetch schema ID %d: %w", schemaID, err)
		}
		fmt.Println(177, "schemaMeta", schemaMeta)
		currentCodec, err = goavro.NewCodec(schemaMeta.Schema)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to parse schema ID %d: %w", schemaID, err)
		}
		c.latestSchema.ID = int(schemaID)
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

	// Field names must match the schema in Schema Registry (snake_case)
	var txn Transaction
	if id, ok := txnMapTyped["id"].(string); ok {
		txn.ID = id
	}
	if t, ok := txnMapTyped["type"].(string); ok {
		txn.Type = t
	}
	if tid, ok := txnMapTyped["terminal_id"].(int64); ok {
		txn.TerminalID = tid
	} else if tid, ok := txnMapTyped["terminal_id"].(int32); ok {
		txn.TerminalID = int64(tid)
	}
	if ra, ok := txnMapTyped["received_at"].(string); ok {
		txn.ReceivedAt = ra
	}

	// ⚠️ Check if amount field exists (it shouldn't!)
	if amount, exists := txnMapTyped["amount"]; exists {
		log.Printf("⚠️  WARNING: amount field found in message: %v", amount)
	} else {
		// Only log for EXTRA_FIELD_TEST to show the issue
		if txn.ID == "EXTRA_FIELD_TEST" {
			log.Printf("❌ amount field MISSING! (was silently dropped by Avro)")
			log.Printf("   Producer sent: amount=100.50")
			log.Printf("   Consumer received: amount field not found")
			log.Printf("   This is SILENT DATA LOSS! 🚨")
		}
	}

	return txn, nil
}

func (c *consumerImp) Process(txn Transaction) error {
	log.Printf("📨 Received Transaction:")
	log.Printf("   ID: %s", txn.ID)
	log.Printf("   Type: %s", txn.Type)
	log.Printf("   Terminal ID: %d", txn.TerminalID)
	log.Printf("   Received At: %s", txn.ReceivedAt)
	log.Println("   ---")

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

// getOrRegisterDLQSchema gets existing DLQ schema or registers a new one
func getOrRegisterDLQSchema(srClient schemaregistry.Client, subject string, schemaRegistryURL string) (schemaregistry.SchemaMetadata, *goavro.Codec, error) {
	// Try to get existing schema
	dlqSchema, err := srClient.GetLatestSchemaMetadata(subject)
	if err == nil {
		// Schema exists, parse it
		codec, err := goavro.NewCodec(dlqSchema.Schema)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, nil, fmt.Errorf("failed to parse DLQ schema: %w", err)
		}
		return dlqSchema, codec, nil
	}

	// Schema doesn't exist, register it via HTTP API
	// DLQ schema definition
	dlqSchemaJSON := `{
		"type": "record",
		"name": "TransactionDLQ",
		"namespace": "local_db",
		"doc": "Dead Letter Queue schema for failed transaction messages",
		"fields": [
			{"name": "originalKey", "type": "string", "doc": "Original message key from the failed transaction"},
			{"name": "originalValue", "type": "bytes", "doc": "Original message value (Avro binary format) from the failed transaction"},
			{"name": "error", "type": "string", "doc": "Error message describing why the message failed"},
			{"name": "originalSchemaId", "type": ["null", "int"], "default": null, "doc": "Schema ID of the original message (if available)"},
			{"name": "failedAt", "type": "string", "doc": "Timestamp when the message failed (ISO 8601 format)"},
			{"name": "retryCount", "type": ["null", "int"], "default": null, "doc": "Number of retry attempts before sending to DLQ"}
		]
	}`

	// Register schema via HTTP API
	schemaID, err := registerSchemaViaHTTP(schemaRegistryURL, subject, dlqSchemaJSON)
	if err != nil {
		return schemaregistry.SchemaMetadata{}, nil, fmt.Errorf("failed to register DLQ schema: %w", err)
	}

	log.Printf("✅ DLQ schema registered (ID: %d)", schemaID)

	// Get the registered schema metadata (use GetLatestSchemaMetadata since we just registered it)
	// Wait a bit for Schema Registry to propagate the schema
	time.Sleep(100 * time.Millisecond)
	dlqSchema, err = srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		return schemaregistry.SchemaMetadata{}, nil, fmt.Errorf("failed to get registered DLQ schema: %w", err)
	}

	// Parse schema
	codec, err := goavro.NewCodec(dlqSchemaJSON)
	if err != nil {
		return schemaregistry.SchemaMetadata{}, nil, fmt.Errorf("failed to parse DLQ schema: %w", err)
	}

	return dlqSchema, codec, nil
}

// registerSchemaViaHTTP registers a schema via Schema Registry HTTP API
func registerSchemaViaHTTP(schemaRegistryURL, subject, schemaJSON string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", schemaRegistryURL, subject)

	// Schema Registry expects the schema as a JSON-escaped string
	requestBody := map[string]string{
		"schema": schemaJSON,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.ID, nil
}

// sendToDLQAvro sends a failed message to DLQ using Avro format
func (c *consumerImp) sendToDLQAvro(msg *sarama.ConsumerMessage, errorMsg string, retryCount int) error {
	// Extract original schema ID if available
	var originalSchemaID interface{} // For Avro union ["null", "int"]
	if len(msg.Value) >= 5 {
		schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
		id := int(schemaID)
		// Avro union ["null", "int"] requires map format: {"int": value}
		originalSchemaID = map[string]interface{}{"int": id}
	} else {
		// null value
		originalSchemaID = nil
	}

	// Prepare retryCount as Avro union ["null", "int"]
	var retryCountUnion interface{}
	if retryCount > 0 {
		retryCountUnion = map[string]interface{}{"int": retryCount}
	} else {
		retryCountUnion = nil
	}

	// Prepare DLQ record
	dlqRecord := map[string]interface{}{
		"originalKey":      string(msg.Key),
		"originalValue":    msg.Value, // Keep original binary data
		"error":            errorMsg,
		"originalSchemaId": originalSchemaID,
		"failedAt":         time.Now().Format(time.RFC3339),
		"retryCount":       retryCountUnion,
	}

	// Serialize DLQ record to Avro binary
	avroBytes, err := c.dlqCodec.BinaryFromNative(nil, dlqRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize DLQ record: %w", err)
	}

	// Create Confluent Schema Registry format: [magic byte][schema ID][avro data]
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0) // magic byte
	binary.Write(&schemaBuf, binary.BigEndian, int32(c.dlqSchema.ID))
	schemaBuf.Write(avroBytes)

	valueBytes := schemaBuf.Bytes()

	// Send to DLQ topic using common library
	_, err = c.kafka.SendMessage(kafka.SendMessageParam{
		Topic:   c.cfg.TopicDLQ,
		Key:     string(msg.Key),
		Message: string(valueBytes),
	})

	return err
}

func main() {
	// Configuration
	cfg := &Config{
		BootstrapServers:  "localhost:9094",
		SchemaRegistryURL: "http://localhost:8083",
		Topic:             "transactions",
		TopicDLQ:          "transactions-dlq",
		GroupID:           "crs-service-group",
	}

	log.Println("🚀 CRS Service (Consumer with Schema Registry) starting...")

	// Create Schema Registry client
	srConfig := schemaregistry.NewConfig(cfg.SchemaRegistryURL)
	srClient, err := schemaregistry.NewClient(srConfig)
	if err != nil {
		log.Fatalf("❌ Failed to create schema registry client: %v", err)
	}

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

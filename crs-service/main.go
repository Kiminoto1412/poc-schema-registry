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
	subject      string
	// DLQ JSON Schema support
	dlqSchema  schemaregistry.SchemaMetadata
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

	// ตรวจสอบว่าเป็น JSON Schema หรือไม่
	if latestSchema.SchemaType != "JSON" {
		return nil, fmt.Errorf("schema is not JSON Schema, got: %s. Please register JSON Schema first", latestSchema.SchemaType)
	}

	log.Printf("✅ Schema Registry connection verified (subject: %s, schema ID: %d, type: %s)", subject, latestSchema.ID, latestSchema.SchemaType)
	log.Println("✅ JSON Schema loaded successfully")

	// Load DLQ schema (get or register)
	dlqSubject := fmt.Sprintf("%s-value", cfg.TopicDLQ)
	dlqSchema, err := getOrRegisterDLQSchema(srClient, dlqSubject, cfg.SchemaRegistryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to setup DLQ schema: %w", err)
	}
	log.Printf("✅ DLQ schema ready (subject: %s, schema ID: %d)", dlqSubject, dlqSchema.ID)

	return &consumerImp{
		cfg:          cfg,
		kafka:        kafkaClient,
		srClient:     srClient,
		latestSchema: latestSchema,
		subject:      subject,
		dlqSchema:    dlqSchema,
		dlqSubject:   dlqSubject,
	}, nil
}

func (c *consumerImp) Consume(ctx context.Context) {
	log.Printf("consumer topic %s starting...", c.cfg.Topic)

	err := c.kafka.Consume(ctx, c.cfg.Topic, c.cfg.GroupID, func(msg *sarama.ConsumerMessage) error {
		log.Printf("receive key: %s", string(msg.Key))

		c.kafka.RetryHandler(msg,
			func(m *sarama.ConsumerMessage) error {
				// Deserialize JSON Schema message
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
				// Send to DLQ using JSON Schema format
				retryCount := 3 // Default retry count from config
				if err := c.sendToDLQJSON(m, errMsg, retryCount); err != nil {
					log.Printf("❌ send to dlq error: %v", err)
					// Fallback to plain JSON format if JSON Schema fails
					_, fallbackErr := c.kafka.SendToDLQ(kafka.SendToDLQParam{
						TopicDlq:     c.cfg.TopicDLQ,
						Msg:          m,
						ErrorMessage: errMsg,
					})
					if fallbackErr != nil {
						log.Printf("❌ fallback DLQ also failed: %v", fallbackErr)
					}
				} else {
					log.Printf("⚠️  message sent to DLQ (JSON Schema): %s (error: %s)", string(m.Key), errMsg)
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
	// Extract Schema Registry header: [magic byte][schema ID][json data]
	if len(msg.Value) < 5 {
		return Transaction{}, errors.New("message too short")
	}

	magicByte := msg.Value[0]
	schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
	jsonData := msg.Value[5:]

	if magicByte != 0 {
		return Transaction{}, fmt.Errorf("invalid magic byte: 0x%02x", magicByte)
	}

	// Verify schema ID matches (optional: fetch if different)
	if int(schemaID) != c.latestSchema.ID {
		log.Printf("⚠️  Schema ID mismatch: expected %d, got %d", c.latestSchema.ID, schemaID)
		// Try to fetch the correct schema
		schemaMeta, err := c.srClient.GetSchemaMetadata(c.subject, int(schemaID))
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to fetch schema ID %d: %w", schemaID, err)
		}
		// Verify it's still JSON Schema
		if schemaMeta.SchemaType != "JSON" {
			return Transaction{}, fmt.Errorf("schema ID %d is not JSON Schema, got: %s", schemaID, schemaMeta.SchemaType)
		}
		c.latestSchema = schemaMeta
	}

	// Deserialize JSON
	var txn Transaction
	if err := json.Unmarshal(jsonData, &txn); err != nil {
		return Transaction{}, fmt.Errorf("failed to deserialize JSON: %w", err)
	}

	// 🔍 Debug: Log all fields received
	log.Printf("🔍 Raw deserialized JSON: %s", string(jsonData))

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
func getOrRegisterDLQSchema(srClient schemaregistry.Client, subject string, schemaRegistryURL string) (schemaregistry.SchemaMetadata, error) {
	// Try to get existing schema
	dlqSchema, err := srClient.GetLatestSchemaMetadata(subject)
	if err == nil {
		// Schema exists, verify it's JSON Schema
		// Note: SchemaType might be empty string for old Avro schemas
		if dlqSchema.SchemaType != "" && dlqSchema.SchemaType != "JSON" {
			return schemaregistry.SchemaMetadata{}, fmt.Errorf("existing DLQ schema is not JSON Schema, got: %s. Please delete the old schema first", dlqSchema.SchemaType)
		}
		// If SchemaType is empty or JSON, check if it's actually JSON Schema by examining the schema content
		// For now, assume empty SchemaType means Avro (old format), so we'll delete and re-register
		if dlqSchema.SchemaType == "" {
			log.Printf("⚠️  Found schema with empty type (likely Avro), deleting and re-registering as JSON Schema...")
			// Delete the old schema subject
			deleteURL := fmt.Sprintf("%s/subjects/%s", schemaRegistryURL, subject)
			req, _ := http.NewRequest("DELETE", deleteURL, nil)
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
			// Wait a bit for deletion to propagate
			time.Sleep(200 * time.Millisecond)
		} else {
			// SchemaType is JSON, use it
			return dlqSchema, nil
		}
	}

	// Schema doesn't exist or was deleted, register it via HTTP API
	// DLQ JSON Schema definition
	dlqSchemaJSON := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"title": "TransactionDLQ",
		"description": "Dead Letter Queue schema for failed transaction messages",
		"properties": {
			"originalKey": {
				"type": "string",
				"description": "Original message key from the failed transaction"
			},
			"originalValue": {
				"type": "string",
				"description": "Original message value (base64 encoded JSON Schema format) from the failed transaction"
			},
			"error": {
				"type": "string",
				"description": "Error message describing why the message failed"
			},
			"originalSchemaId": {
				"type": ["integer", "null"],
				"description": "Schema ID of the original message (if available)"
			},
			"failedAt": {
				"type": "string",
				"format": "date-time",
				"description": "Timestamp when the message failed (ISO 8601 format)"
			},
			"retryCount": {
				"type": ["integer", "null"],
				"description": "Number of retry attempts before sending to DLQ"
			}
		},
		"required": ["originalKey", "originalValue", "error", "failedAt"]
	}`

	// Register schema via HTTP API with JSON Schema type
	schemaID, err := registerJSONSchemaViaHTTP(schemaRegistryURL, subject, dlqSchemaJSON)
	if err != nil {
		return schemaregistry.SchemaMetadata{}, fmt.Errorf("failed to register DLQ schema: %w", err)
	}

	log.Printf("✅ DLQ schema registered (ID: %d)", schemaID)

	// Get the registered schema metadata (use GetLatestSchemaMetadata since we just registered it)
	// Wait a bit for Schema Registry to propagate the schema
	time.Sleep(100 * time.Millisecond)
	dlqSchema, err = srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		return schemaregistry.SchemaMetadata{}, fmt.Errorf("failed to get registered DLQ schema: %w", err)
	}

	return dlqSchema, nil
}

// registerJSONSchemaViaHTTP registers a JSON Schema via Schema Registry HTTP API
func registerJSONSchemaViaHTTP(schemaRegistryURL, subject, schemaJSON string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", schemaRegistryURL, subject)

	// Schema Registry expects the schema as a JSON-escaped string with schemaType
	requestBody := map[string]interface{}{
		"schema":     schemaJSON,
		"schemaType": "JSON",
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

// sendToDLQJSON sends a failed message to DLQ using JSON Schema format
func (c *consumerImp) sendToDLQJSON(msg *sarama.ConsumerMessage, errorMsg string, retryCount int) error {
	// Extract original schema ID if available
	var originalSchemaID *int
	if len(msg.Value) >= 5 {
		schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
		id := int(schemaID)
		originalSchemaID = &id
	}

	// Prepare DLQ record
	dlqRecord := map[string]interface{}{
		"originalKey":   string(msg.Key),
		"originalValue": string(msg.Value), // Keep original data as string (base64 could be used but string is simpler)
		"error":         errorMsg,
		"failedAt":      time.Now().Format(time.RFC3339),
	}

	// Add optional fields if they have values
	if originalSchemaID != nil {
		dlqRecord["originalSchemaId"] = *originalSchemaID
	}
	if retryCount > 0 {
		dlqRecord["retryCount"] = retryCount
	}

	// Serialize DLQ record to JSON
	jsonBytes, err := json.Marshal(dlqRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize DLQ record: %w", err)
	}

	// Create Confluent Schema Registry format: [magic byte][schema ID][json data]
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0) // magic byte
	binary.Write(&schemaBuf, binary.BigEndian, int32(c.dlqSchema.ID))
	schemaBuf.Write(jsonBytes)

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

	log.Println("🚀 CRS Service (Consumer with JSON Schema) starting...")

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

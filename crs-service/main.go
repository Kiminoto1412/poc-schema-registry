package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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

func main() {
	// Configuration
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions"
	groupID := "crs-service-group"

	log.Println("🚀 CRS Service (Consumer with Schema Registry) starting...")

	// Create Schema Registry client
	srConfig := schemaregistry.NewConfig(schemaRegistryURL)
	srClient, err := schemaregistry.NewClient(srConfig)
	if err != nil {
		log.Fatalf("❌ Failed to create schema registry client: %v", err)
	}

	// Verify Schema Registry connection by fetching latest schema
	subject := fmt.Sprintf("%s-value", topic)
	latestSchema, err := srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		log.Fatalf("❌ Failed to verify Schema Registry connection: %v", err)
	}
	log.Printf("✅ Schema Registry connection verified (subject: %s, schema ID: %d)", subject, latestSchema.ID)

	// Parse schema ด้วย goavro (same approach as transaction-service)
	codec, err := goavro.NewCodec(latestSchema.Schema)
	if err != nil {
		log.Fatalf("❌ Failed to parse schema: %v", err)
	}
	log.Println("✅ Avro codec created successfully")

	// Create Kafka consumer using common library
	kafkaClient, cleanup := kafka.NewKafka(kafka.KafkaConfig{
		Brokers:     bootstrapServers,
		EnforceTls:  false,
		AuthEnabled: false,
		Retry:       3,
		ClientID:    groupID,
	})
	defer cleanup()

	log.Printf("✅ Subscribed to topic: %s", topic)
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

	// Consume messages using common library
	err = kafkaClient.Consume(ctx, topic, groupID, func(msg *sarama.ConsumerMessage) error {
		// Extract Schema Registry header: [magic byte][schema ID][avro data]
		if len(msg.Value) < 5 {
			log.Printf("❌ Message too short: %d bytes", len(msg.Value))
			return nil // Skip this message
		}

		magicByte := msg.Value[0]
		schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
		avroData := msg.Value[5:]

		if magicByte != 0 {
			log.Printf("❌ Invalid magic byte: 0x%02x", magicByte)
			return nil // Skip this message
		}

		currentCodec := codec
		// Verify schema ID matches
		if int(schemaID) != latestSchema.ID {
			log.Printf("⚠️  Schema ID mismatch: expected %d, got %d", latestSchema.ID, schemaID)
			// Try to fetch the correct schema
			schemaMeta, err := srClient.GetSchemaMetadata(subject, int(schemaID))
			if err != nil {
				log.Printf("❌ Failed to fetch schema ID %d: %v", schemaID, err)
				return nil // Skip this message
			}
			currentCodec, err = goavro.NewCodec(schemaMeta.Schema)
			if err != nil {
				log.Printf("❌ Failed to parse schema ID %d: %v", schemaID, err)
				return nil // Skip this message
			}
			latestSchema.ID = int(schemaID)
		}

		// Deserialize Avro binary data
		native, _, err := currentCodec.NativeFromBinary(avroData)
		if err != nil {
			log.Printf("❌ Failed to deserialize Avro data: %v", err)
			log.Printf("   Message key: %s", string(msg.Key))
			log.Printf("   Message size: %d bytes", len(msg.Value))
			return nil // Skip this message
		}

		// Convert to map
		txnMapTyped, ok := native.(map[string]interface{})
		if !ok {
			log.Printf("❌ Deserialized data is not a map: %T", native)
			return nil // Skip this message
		}

		// Debug: Log available fields
		log.Printf("🔍 Deserialized fields: %v", getKeys(txnMapTyped))

		// Field names must match the schema in Schema Registry (uppercase)
		var txn Transaction
		if id, ok := txnMapTyped["ID"].(string); ok {
			txn.ID = id
		}
		if t, ok := txnMapTyped["Type"].(string); ok {
			txn.Type = t
		}
		if tid, ok := txnMapTyped["TerminalID"].(int64); ok {
			txn.TerminalID = tid
		} else if tid, ok := txnMapTyped["TerminalID"].(int32); ok {
			txn.TerminalID = int64(tid)
		}
		if ra, ok := txnMapTyped["ReceivedAt"].(string); ok {
			txn.ReceivedAt = ra
		}

		// Process transaction
		log.Printf("📨 Received Transaction:")
		log.Printf("   ID: %s", txn.ID)
		log.Printf("   Type: %s", txn.Type)
		log.Printf("   Terminal ID: %d", txn.TerminalID)
		log.Printf("   Received At: %s", txn.ReceivedAt)
		log.Println("   ---")

		// Simulate processing
		processTransaction(txn)

		// Return nil to mark message as processed
		// Common library will handle offset commit automatically
		return nil
	})

	if err != nil {
		log.Fatalf("❌ Consumer error: %v", err)
	}

	log.Println("✅ Consumer stopped gracefully")
}

func processTransaction(txn Transaction) {
	log.Printf("🔧 Processing transaction ID: %s", txn.ID)

	// TODO: Add your business logic here
	// For example: save to database, call other services, etc.

	log.Printf("✅ Transaction %s processed successfully", txn.ID)
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

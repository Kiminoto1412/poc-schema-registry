package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"reflect"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/wirelessr/avroschema"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

type Transaction struct {
	ID         string  `json:"id"`
	Type       string  `json:"type"`
	TerminalID int64   `json:"terminal_id"`
	Mam        string  `json:"mam"`
	// ReceivedAt string  `json:"received_at"`
	// Amount     float64 `json:"amount"`
}

func ensureSchemaMatchesStruct(srClient *srclient.SchemaRegistryClient, subject string, structType reflect.Type) (*srclient.Schema, *goavro.Codec, error) {
	schemaJSON, err := avroschema.Reflect(&Transaction{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to reflect schema: %w", err)
	}

	existingSchema, err := srClient.GetLatestSchema(subject)
	if err != nil {
		log.Printf("📝 Registering new schema for subject: %s", subject)
		return registerSchema(srClient, subject, schemaJSON)
	}

	existingSchemaJSON := existingSchema.Schema()
	isCompatible, err := srClient.IsSchemaCompatible(subject, schemaJSON, fmt.Sprintf("%d", existingSchema.Version()), srclient.Avro)
	if err != nil {
		return nil, nil, fmt.Errorf("schema compatibility check failed: %w", err)
	}

	if !isCompatible {
		compatLevel, _ := srClient.GetCompatibilityLevel(subject, true)
		compatMode := "forward"
		if compatLevel != nil {
			compatMode = string(*compatLevel)
		}
		return nil, nil, fmt.Errorf(
			"schema is not compatible (mode: %s)\n"+
				"  old schema: %s\n"+
				"  new schema: %s\n"+
				"  tip: %s compatibility requires new schema to have all fields from old schema",
			compatMode, existingSchemaJSON, schemaJSON, compatMode)
	}

	log.Printf("✅ Schema is compatible, registering new version...")
	return registerSchema(srClient, subject, schemaJSON)
}

func registerSchema(srClient *srclient.SchemaRegistryClient, subject, schemaJSON string) (*srclient.Schema, *goavro.Codec, error) {
	schema, err := srClient.CreateSchema(subject, schemaJSON, srclient.Avro)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to register schema: %w", err)
	}
	log.Printf("✅ Registered schema (ID: %d, Version: %d)", schema.ID(), schema.Version())

	codec := schema.Codec()
	if codec == nil {
		return nil, nil, fmt.Errorf("failed to get codec from schema")
	}
	return schema, codec, nil
}

func createConfluentFormat(schemaID int, avroBytes []byte) []byte {
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0)
	binary.Write(&schemaBuf, binary.BigEndian, int32(schemaID))
	schemaBuf.Write(avroBytes)
	return schemaBuf.Bytes()
}

func main() {
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions-value"

	log.Println("🚀 Transaction Service starting...")

	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)
	// Cache Codec Schema
	srClient.CodecCreationEnabled(true)

	compatibilityLevel, err := srClient.ChangeSubjectCompatibilityLevel(topic, srclient.Forward)
	if err != nil {
		log.Printf("⚠️  Failed to set compatibility level: %v", err)
		currentLevel, err := srClient.GetCompatibilityLevel(topic, true)
		if err == nil {
			log.Printf("📋 Current compatibility level: %s", *currentLevel)
		}
	} else {
		log.Printf("✅ Set compatibility level to: %s", *compatibilityLevel)
	}

	schema, codec, err := ensureSchemaMatchesStruct(srClient, topic, reflect.TypeOf(Transaction{}))
	if err != nil {
		log.Fatalf("❌ Failed to ensure schema matches struct: %v", err)
	}

	log.Printf("✅ Using schema ID: %d", schema.ID())

	kafkaClient, cleanup := kafka.NewKafka(kafka.KafkaConfig{
		Brokers:     bootstrapServers,
		EnforceTls:  false,
		AuthEnabled: false,
		Retry:       3,
		ClientID:    "transaction-service-producer",
	})
	defer cleanup()

	log.Println("✅ Connected to Kafka and Schema Registry")

	txn := Transaction{
		ID:         "TXN_0000001",
		Type:       "SALE",
		TerminalID: 2,
		Mam:        "MAM_0000001",
	}

	txnMap := map[string]interface{}{
		"id":          txn.ID,
		"type":        txn.Type,
		"terminal_id": txn.TerminalID,
		"mam":         txn.Mam,
		// "received_at": txn.ReceivedAt,
		// "amount":      txn.Amount,
	}

	avroBytes, err := codec.BinaryFromNative(nil, txnMap)
	if err != nil {
		log.Fatalf("❌ Serialization failed: %v", err)
	}

	valueBytes := createConfluentFormat(schema.ID(), avroBytes)

	result, err := kafkaClient.SendMessage(kafka.SendMessageParam{
		Topic:   topic,
		Key:     txn.ID,
		Message: string(valueBytes),
	})
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	log.Printf("✅ Message sent successfully | ID=%s Type=%s (Partition: %d, Offset: %d)",
		txn.ID, txn.Type, result.Partition, result.Offset)
}

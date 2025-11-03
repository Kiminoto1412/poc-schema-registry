package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// toSnakeCase converts PascalCase to snake_case
// Example: TerminalID → terminal_id
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		if r >= 'A' && r <= 'Z' {
			result.WriteRune(r + 32) // Convert to lowercase
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// validateSchema validates a record against the Avro schema before serialization
// Note: Producer-side validation failures should fail fast and return error
// DLQ is only for consumer-side processing failures (handled by CRS service)
func validateSchema(codec *goavro.Codec, record map[string]interface{}) error {
	// Try to serialize to a test buffer to validate the structure
	// This will catch type mismatches and missing required fields
	_, err := codec.BinaryFromNative(nil, record)
	if err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	return nil
}

// structToAvroMap converts a struct to map[string]interface{} for Avro serialization
// using struct tags to map Go field names to Avro field names
// Example: ID string `avro:"id"` → map["id"] = struct.ID
func structToAvroMap(v interface{}) (map[string]interface{}, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %T", v)
	}

	rt := rv.Type()
	result := make(map[string]interface{})

	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		fieldValue := rv.Field(i)

		// Get Avro field name from struct tag, fallback to field name
		avroName := field.Tag.Get("avro")
		if avroName == "" {
			// Convert Go field name (PascalCase) to Avro name (snake_case)
			avroName = toSnakeCase(field.Name)
		}

		// Skip unexported fields
		if !fieldValue.CanInterface() {
			continue
		}

		// Handle zero values - only include if not zero or if required
		if fieldValue.IsZero() {
			// Check if field has "omitempty" tag
			if strings.Contains(field.Tag.Get("avro"), "omitempty") {
				continue
			}
		}

		result[avroName] = fieldValue.Interface()
	}

	return result, nil
}

// createConfluentFormat creates Confluent Schema Registry wire format:
// [magic byte (1 byte)][schema ID (4 bytes)][avro data (variable)]
func createConfluentFormat(schemaID int, avroBytes []byte) []byte {
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0) // magic byte
	binary.Write(&schemaBuf, binary.BigEndian, int32(schemaID))
	schemaBuf.Write(avroBytes)
	return schemaBuf.Bytes()
}

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

	// --- Kafka producer using common library ---
	kafkaClient, cleanup := kafka.NewKafka(kafka.KafkaConfig{
		Brokers:     bootstrapServers,
		EnforceTls:  false,
		AuthEnabled: false,
		Retry:       3,
		ClientID:    "transaction-service-producer",
	})
	defer cleanup()

	log.Println("✅ Connected to Kafka and Schema Registry")

	// --- ตัวอย่างข้อมูล ---
	type Transaction struct {
		ID         string `avro:"id"`          // Map Go field "ID" to Avro field "id"
		Type       string `avro:"type"`        // Map Go field "Type" to Avro field "type"
		TerminalID int64  `avro:"terminal_id"` // Map Go field "TerminalID" to Avro field "terminal_id"
		ReceivedAt string `avro:"received_at"` // Map Go field "ReceivedAt" to Avro field "received_at"
	}

	transactions := []Transaction{
		{ID: "1001", Type: "CONTROL", TerminalID: 1, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
		{ID: "1002", Type: "CONTROL", TerminalID: 2, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
		{ID: "1003", Type: "CONTROL", TerminalID: 3, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},
		{ID: "FAIL_TEST", Type: "CONTROL", TerminalID: 999, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")}, // 💥 This will fail at consumer processing (not validation)
		{ID: "DLQ_TEST", Type: "CONTROL", TerminalID: 888, ReceivedAt: time.Now().Format("2006-01-02 15:04:05")},  // 💥 This will also fail at consumer processing (not validation)
	}

	// --- ส่ง message ---
	for i, txn := range transactions {
		// แปลง struct เป็น map โดยใช้ struct tags อัตโนมัติ		
		record, err := structToAvroMap(txn)
		if err != nil {
			log.Printf("❌ Failed to convert struct to map for txn %d: %v", i, err)
			continue
		}

		// 🔍 Validate schema ก่อนส่ง message
		// Producer-side: Fail fast and return error (ไม่ส่งไป DLQ เพราะเป็นต้นทาง)
		// DLQ จะใช้สำหรับ consumer-side failures เท่านั้น
		if err := validateSchema(codec, record); err != nil {
			log.Printf("❌ Schema validation failed for txn %d (ID: %s): %v", i, txn.ID, err)
			log.Printf("   Record structure: %+v", record)
			log.Printf("   Error: Message structure does not match Schema Registry schema")
			log.Printf("   ⚠️  Producer should fix the data source - this is not a consumer failure")
			// ไม่ส่งไป DLQ เพราะเป็น producer (ต้นทาง) - ควรแก้ที่ source code
			continue
		}

		// Serialize ด้วย goavro (validation ผ่านแล้ว)
		avroBytes, err := codec.BinaryFromNative(nil, record)
		if err != nil {
			log.Printf("❌ Avro serialization failed for txn %d: %v", i, err)
			continue
		}

		// สร้าง Confluent Schema Registry format: [magic byte][schema ID][avro data]
		valueBytes := createConfluentFormat(latestSchema.ID, avroBytes)

		// Convert binary data to string for common library
		// sarama.StringEncoder will convert string back to []byte correctly
		valueString := string(valueBytes)
		fmt.Println("valueBytes", valueBytes)
		fmt.Println("valueString", valueString)

		// Send message using common library
		result, err := kafkaClient.SendMessage(kafka.SendMessageParam{
			Topic:   topic,
			Key:     txn.ID,
			Message: valueString,
		})

		if err != nil {
			log.Printf("❌ Failed to send txn %d: %v", i, err)
			continue
		}

		log.Printf("✅ Sent Txn[%d]: ID=%s Type=%s Terminal=%d (Partition: %d, Offset: %d)",
			i+1, txn.ID, txn.Type, txn.TerminalID, result.Partition, result.Offset)

		time.Sleep(time.Second)
	}

	log.Println("✅ All valid transactions sent successfully!")

	// --- ทดสอบ schema validation ด้วยข้อมูลที่ไม่ถูกต้อง ---
	// Note: Producer-side validation failures should NOT go to DLQ
	// DLQ is only for consumer-side processing failures
	log.Println("\n🧪 Testing schema validation with invalid data (for demonstration)...")
	log.Println("   Note: Invalid messages will NOT be sent to DLQ (producer is the source)")

	// Test case 1: Missing required field
	log.Println("\n📋 Test 1: Missing required field 'received_at'")
	invalidRecord1 := map[string]interface{}{
		"id":          "INVALID_001",
		"type":        "CONTROL",
		"terminal_id": int64(1),
		// Missing "received_at" (Avro field name)
	}
	if err := validateSchema(codec, invalidRecord1); err != nil {
		log.Printf("❌ Validation correctly caught missing field: %v", err)
		log.Printf("   ✅ Correct behavior: Producer should fix data source, NOT send to DLQ")
	} else {
		log.Printf("⚠️  Validation should have failed but didn't!")
	}

	// Test case 2: Wrong field type
	log.Println("\n📋 Test 2: Wrong field type (terminal_id should be int64, got string)")
	invalidRecord2 := map[string]interface{}{
		"id":          "INVALID_002",
		"type":        "CONTROL",
		"terminal_id": "should_be_int64", // Wrong type!
		"received_at": time.Now().Format("2006-01-02 15:04:05"),
	}
	if err := validateSchema(codec, invalidRecord2); err != nil {
		log.Printf("❌ Validation correctly caught type mismatch: %v", err)
		log.Printf("   ✅ Correct behavior: Producer should fix data source, NOT send to DLQ")
	} else {
		log.Printf("⚠️  Validation should have failed but didn't!")
	}

	log.Println("\n📝 Summary:")
	log.Println("   - Producer-side validation failures: Fail fast, return error")
	log.Println("   - Consumer-side processing failures: Retry, then send to DLQ")
	log.Println("   - This separation ensures proper error handling and responsibility")
}

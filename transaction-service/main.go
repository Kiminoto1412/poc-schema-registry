package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/xeipuuv/gojsonschema"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// validateJSONSchema validates a JSON object against the JSON Schema
// Note: Producer-side validation failures should fail fast and return error
// DLQ is only for consumer-side processing failures (handled by CRS service)
func validateJSONSchema(schemaStr string, jsonData []byte) error {
	schemaLoader := gojsonschema.NewStringLoader(schemaStr)
	documentLoader := gojsonschema.NewBytesLoader(jsonData)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("schema validation error: %w", err)
	}

	if !result.Valid() {
		var errors []string
		for _, desc := range result.Errors() {
			errors = append(errors, desc.String())
		}
		return fmt.Errorf("schema validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

// createConfluentFormat creates Confluent Schema Registry wire format:
// [magic byte (1 byte)][schema ID (4 bytes)][json data (variable)]
func createConfluentFormat(schemaID int, jsonBytes []byte) []byte {
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0) // magic byte
	binary.Write(&schemaBuf, binary.BigEndian, int32(schemaID))
	schemaBuf.Write(jsonBytes) // JSON string bytes
	return schemaBuf.Bytes()
}

func main() {
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions"

	log.Println("🚀 Transaction Service (Producer with JSON Schema) starting...")

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

	// ตรวจสอบว่าเป็น JSON Schema หรือไม่
	if latestSchema.SchemaType != "JSON" {
		log.Fatalf("❌ Schema is not JSON Schema, got: %s. Please register JSON Schema first.", latestSchema.SchemaType)
	}

	log.Printf("✅ Got JSON Schema ID: %d", latestSchema.ID)

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
		ID         string `json:"id"`          // JSON field name
		Type       string `json:"type"`        // JSON field name
		TerminalID int64  `json:"terminal_id"` // JSON field name
		ReceivedAt string `json:"received_at"` // JSON field name
	}

	// Transaction types สำหรับสร้างข้อมูลที่หลากหลาย
	transactionTypes := []string{"CONTROL", "SALE", "RETURN", "VOID", "AUTHORIZE", "CAPTURE", "REFUND"}
	numTransactions := 1_000_000 // 1 ล้าน records

	log.Printf("📊 Generating %d transactions...", numTransactions)

	// --- Resource tracking สำหรับ summary ---
	startTime := time.Now()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	startMemAlloc := m.Alloc
	startMemSys := m.Sys

	// Statistics counters
	var successCount int64
	var failureCount int64
	var validationFailCount int64
	var serializationFailCount int64

	// --- ส่ง message ---
	for i := 0; i < numTransactions; i++ {
		// สร้าง transaction แบบ dynamic
		var txn Transaction

		// สร้าง ID ที่ unique (TXN_0000001, TXN_0000002, ...)
		txn.ID = fmt.Sprintf("TXN_%07d", i+1)

		// ใช้ transaction type แบบวนรอบจาก array
		txn.Type = transactionTypes[i%len(transactionTypes)]

		// Terminal ID แบบสุ่มระหว่าง 1-1000
		txn.TerminalID = int64((i % 1000) + 1)

		// Timestamp ที่เพิ่มขึ้นเล็กน้อยสำหรับแต่ละ record (RFC3339 format for JSON Schema date-time)
		txn.ReceivedAt = time.Now().Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339)

		// เพิ่ม special test cases ที่ตำแหน่งที่กำหนด
		if i == 999998 {
			txn.ID = "FAIL_TEST"
			txn.TerminalID = 999
		} else if i == 999999 {
			txn.ID = "DLQ_TEST"
			txn.TerminalID = 888
		}

		// Serialize เป็น JSON
		jsonBytes, err := json.Marshal(txn)
		if err != nil {
			serializationFailCount++
			log.Printf("❌ JSON marshaling failed for txn %d: %v", i+1, err)
			continue
		}

		// 🔍 Validate schema ก่อนส่ง message
		// Producer-side: Fail fast and return error (ไม่ส่งไป DLQ เพราะเป็นต้นทาง)
		// DLQ จะใช้สำหรับ consumer-side failures เท่านั้น
		if err := validateJSONSchema(latestSchema.Schema, jsonBytes); err != nil {
			validationFailCount++
			log.Printf("❌ Schema validation failed for txn %d (ID: %s): %v", i+1, txn.ID, err)
			log.Printf("   JSON data: %s", string(jsonBytes))
			log.Printf("   ⚠️  Producer should fix the data source - this is not a consumer failure")
			// ไม่ส่งไป DLQ เพราะเป็น producer (ต้นทาง) - ควรแก้ที่ source code
			continue
		}

		// สร้าง Confluent Schema Registry format: [magic byte][schema ID][json data]
		valueBytes := createConfluentFormat(latestSchema.ID, jsonBytes)

		// Convert binary data to string for common library
		// sarama.StringEncoder will convert string back to []byte correctly
		valueString := string(valueBytes)
		// Debug prints removed for performance (1M records)

		// Send message using common library
		result, err := kafkaClient.SendMessage(kafka.SendMessageParam{
			Topic:   topic,
			Key:     txn.ID,
			Message: valueString,
		})

		if err != nil {
			failureCount++
			log.Printf("❌ Failed to send txn %d: %v", i+1, err)
			continue
		}

		successCount++

		// Log progress every 10,000 records เพื่อไม่ให้ log เยอะเกินไป
		if (i+1)%10000 == 0 || i < 5 || i >= numTransactions-2 {
			log.Printf("✅ Progress: Sent %d/%d transactions | Last: ID=%s Type=%s Terminal=%d (Partition: %d, Offset: %d)",
				i+1, numTransactions, txn.ID, txn.Type, txn.TerminalID, result.Partition, result.Offset)
		}

		// Sleep removed for performance - sending 1M records
	}

	// --- Resource Summary ---
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	runtime.ReadMemStats(&m)
	endMemAlloc := m.Alloc
	endMemSys := m.Sys

	memAllocUsed := endMemAlloc - startMemAlloc
	memSysUsed := endMemSys - startMemSys

	throughput := float64(successCount) / duration.Seconds()

	log.Println("\n" + strings.Repeat("=", 80))
	log.Println("📊 RESOURCE USAGE SUMMARY")
	log.Println(strings.Repeat("=", 80))
	log.Printf("⏱️  Execution Time:")
	log.Printf("   • Total Duration:     %v", duration)
	log.Printf("   • Duration (seconds): %.2f seconds", duration.Seconds())
	log.Printf("   • Duration (minutes): %.2f minutes", duration.Minutes())
	log.Println()
	log.Printf("📈 Transaction Statistics:")
	log.Printf("   • Total Attempted:    %d", numTransactions)
	log.Printf("   • Successfully Sent:  %d", successCount)
	log.Printf("   • Failed to Send:     %d", failureCount)
	log.Printf("   • Validation Failed: %d", validationFailCount)
	log.Printf("   • Serialization Failed: %d", serializationFailCount)
	log.Printf("   • Success Rate:       %.2f%%", float64(successCount)/float64(numTransactions)*100)
	log.Println()
	log.Printf("⚡ Performance Metrics:")
	log.Printf("   • Throughput:         %.2f transactions/second", throughput)
	log.Printf("   • Avg Time per Record: %.4f ms", float64(duration.Milliseconds())/float64(numTransactions))
	log.Println()
	log.Printf("💾 Memory Usage:")
	log.Printf("   • Memory Allocated:   %d bytes (%.2f MB)", memAllocUsed, float64(memAllocUsed)/1024/1024)
	log.Printf("   • System Memory:      %d bytes (%.2f MB)", memSysUsed, float64(memSysUsed)/1024/1024)
	log.Printf("   • Current Heap Alloc: %d bytes (%.2f MB)", m.Alloc, float64(m.Alloc)/1024/1024)
	log.Printf("   • Current Sys Memory: %d bytes (%.2f MB)", m.Sys, float64(m.Sys)/1024/1024)
	log.Printf("   • GC Cycles:          %d", m.NumGC)
	log.Println(strings.Repeat("=", 80))

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
		"terminal_id": 1,
		// Missing "received_at"
	}
	invalidJSON1, _ := json.Marshal(invalidRecord1)
	if err := validateJSONSchema(latestSchema.Schema, invalidJSON1); err != nil {
		log.Printf("❌ Validation correctly caught missing field: %v", err)
		log.Printf("   ✅ Correct behavior: Producer should fix data source, NOT send to DLQ")
	} else {
		log.Printf("⚠️  Validation should have failed but didn't!")
	}

	// Test case 2: Wrong field type
	log.Println("\n📋 Test 2: Wrong field type (terminal_id should be integer, got string)")
	invalidRecord2 := map[string]interface{}{
		"id":          "INVALID_002",
		"type":        "CONTROL",
		"terminal_id": "should_be_integer", // Wrong type!
		"received_at": time.Now().Format(time.RFC3339),
	}
	invalidJSON2, _ := json.Marshal(invalidRecord2)
	if err := validateJSONSchema(latestSchema.Schema, invalidJSON2); err != nil {
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

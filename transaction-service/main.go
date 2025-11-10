package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// generateAvroSchema สร้าง Avro schema JSON จาก struct type โดยใช้ reflection
func generateAvroSchema(structType reflect.Type) string {
	fields := make([]map[string]interface{}, 0)

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		avroTag := field.Tag.Get("avro")
		if avroTag == "" || avroTag == "-" {
			continue
		}

		fieldDef := map[string]interface{}{"name": avroTag}
		var avroType interface{}

		switch field.Type.Kind() {
		case reflect.String:
			avroType = "string"
		case reflect.Int64:
			avroType = "long"
		case reflect.Int32:
			avroType = "int"
		case reflect.Float64:
			avroType = []interface{}{"null", "double"}
			fieldDef["default"] = nil
		case reflect.Float32:
			avroType = []interface{}{"null", "float"}
			fieldDef["default"] = nil
		case reflect.Bool:
			avroType = "boolean"
		default:
			avroType = "string"
		}
		fieldDef["type"] = avroType
		fields = append(fields, fieldDef)
	}

	recordName := structType.Name()
	if recordName == "" {
		recordName = "Record"
	}

	schemaJSON, _ := json.Marshal(map[string]interface{}{
		"type":   "record",
		"name":   recordName,
		"fields": fields,
	})
	return string(schemaJSON)
}

// ensureSchemaMatchesStruct ตรวจสอบและ register schema ถ้าไม่ตรงกับ struct
func ensureSchemaMatchesStruct(srClient *srclient.SchemaRegistryClient, subject string, structType reflect.Type) (*srclient.Schema, *goavro.Codec, error) {
	structSchemaJSON := generateAvroSchema(structType)

	// ดึง schema ที่มีอยู่
	existingSchema, err := srClient.GetLatestSchema(subject)
	if err != nil {
		log.Printf("⚠️  Schema not found, registering new schema...")
		return registerSchema(srClient, subject, structSchemaJSON)
	}

	// ตรวจสอบ compatibility
	isCompatible, err := srClient.IsSchemaCompatible(subject, structSchemaJSON, fmt.Sprintf("%d", existingSchema.Version()), srclient.Avro)
	if err != nil || !isCompatible {
		if err != nil {
			log.Printf("⚠️  Schema compatibility check failed: %v", err)
		} else {
			log.Printf("⚠️  Schema not compatible, auto-registering new version...")
		}
		return registerSchema(srClient, subject, structSchemaJSON)
	}

	log.Printf("✅ Schema is compatible (ID: %d)", existingSchema.ID())
	codec := existingSchema.Codec()
	if codec == nil {
		return nil, nil, fmt.Errorf("failed to get codec from schema")
	}
	return existingSchema, codec, nil
}

// registerSchema register schema และ return schema + codec
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

// createConfluentFormat สร้าง Confluent Schema Registry wire format:
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
	topic := "transactions-value"

	log.Println("🚀 Transaction Service (Producer with Schema Registry) starting...")

	// สร้าง Schema Registry client โดยใช้ srclient
	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)
	// Enable codec creation เพื่อให้สามารถใช้ schema.Codec() ได้
	srClient.CodecCreationEnabled(true)

	// --- ตัวอย่างข้อมูล ---
	type Transaction struct {
		ID         string  `avro:"id"`          // Map Go field "ID" to Avro field "id"
		Type       string  `avro:"type"`        // Map Go field "Type" to Avro field "type"
		TerminalID int64   `avro:"terminal_id"` // Map Go field "TerminalID" to Avro field "terminal_id"
		ReceivedAt string  `avro:"received_at"` // Map Go field "ReceivedAt" to Avro field "received_at"
		Amount     float64 `avro:"amount"`      // Field ที่มีใน schema (union type)
		Amount2    float64 `avro:"amount2"`     // Field ที่มีใน schema (union type)
		Amount3    float64 `avro:"amount3"`     // Field ที่มีใน schema (union type)
	}

	// ตรวจสอบและ register schema ถ้าไม่ตรงกับ struct โดยใช้ reflection
	schema, codec, err := ensureSchemaMatchesStruct(srClient, topic, reflect.TypeOf(Transaction{}))
	if err != nil {
		log.Fatalf("❌ Failed to ensure schema matches struct: %v", err)
	}

	log.Printf("✅ Using schema ID: %d", schema.ID())

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

	// Transaction types สำหรับสร้างข้อมูลที่หลากหลาย
	transactionTypes := []string{"CONTROL", "SALE", "RETURN", "VOID", "AUTHORIZE", "CAPTURE", "REFUND"}
	// numTransactions := 1_000_000 // 1 ล้าน records
	numTransactions := 5 // 5 records

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

		// Timestamp ที่เพิ่มขึ้นเล็กน้อยสำหรับแต่ละ record
		txn.ReceivedAt = time.Now().Add(time.Duration(i) * time.Millisecond).Format("2006-01-02 15:04:05")

		// ⚠️ เพิ่ม field ใหม่ที่ยังไม่ได้ register ใน Schema Registry
		txn.Amount = float64(i+1) * 100.50

		// เพิ่ม special test cases ที่ตำแหน่งที่กำหนด
		if i == 999998 {
			txn.ID = "FAIL_TEST"
			txn.TerminalID = 999
		} else if i == 999999 {
			txn.ID = "DLQ_TEST"
			txn.TerminalID = 888
		} else if i == 0 {
			// Test case สำหรับแสดงปัญหา field ที่ไม่ได้ register
			txn.ID = "EXTRA_FIELD_TEST"
			txn.Amount = 100.50
			log.Printf("⚠️  Test: Sending transaction with extra field 'amount' that is NOT in schema")
		}

		// Serialize ด้วย goavro
		// แปลง struct เป็น map สำหรับ goavro
		txnMap := map[string]interface{}{
			"id":          txn.ID,
			"type":        txn.Type,
			"terminal_id": txn.TerminalID,
			"received_at": txn.ReceivedAt,
		}

		// สำหรับ union type ["null", "double"] ใน Avro ต้องส่งเป็น map
		// ถ้าเป็น null: nil
		// ถ้าเป็น double: map[string]interface{}{"double": value}
		if txn.Amount != 0 {
			txnMap["amount"] = map[string]interface{}{"double": txn.Amount}
		} else {
			txnMap["amount"] = nil
		}

		// Debug: log สำหรับ test case
		if i == 0 {
			log.Printf("🔍 Debug: Sending data with extra field: %+v", txnMap)
		}

		avroBytes, err := codec.BinaryFromNative(nil, txnMap)
		if err != nil {
			serializationFailCount++
			log.Printf("❌ Avro serialization failed for txn %d: %v", i+1, err)
			continue
		}

		// สร้าง Confluent Schema Registry format: [magic byte][schema ID][avro data]
		valueBytes := createConfluentFormat(schema.ID(), avroBytes)

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

	log.Println("\n📝 Summary:")
	log.Println("   - Producer-side validation failures: Fail fast, return error")
	log.Println("   - Consumer-side processing failures: Retry, then send to DLQ")
	log.Println("   - This separation ensures proper error handling and responsibility")
}

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
	"github.com/wirelessr/avroschema"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// --- ตัวอย่างข้อมูล ---
type Transaction struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	TerminalID int64  `json:"terminal_id"`
	// ReceivedAt string   `json:"received_at"`
	// Amount *float64 `json:"amount"`
	// Status string   `json:"status"`
	// Mam        string   `json:"mam,default=ACTIVE"`
	// CreatedAt  string   `json:"created_at,default=eiei"`
}

// addDefaultValues เพิ่ม default values ให้กับ schema JSON สำหรับ backward compatibility
func addDefaultValues(schemaJSON string) (string, error) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return "", fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return "", fmt.Errorf("fields is not an array")
	}

	// Field names ที่ต้องการ default values
	defaultValues := map[string]interface{}{
		"created_at": "",
		"status":     "ACTIVE",
		"mam":        "ACTIVE",
	}

	// เพิ่ม default values ให้กับ fields
	for i, field := range fields {
		fieldMap, ok := field.(map[string]interface{})
		if !ok {
			continue
		}

		fieldName, ok := fieldMap["name"].(string)
		if !ok {
			continue
		}

		if defaultValue, exists := defaultValues[fieldName]; exists {
			fieldMap["default"] = defaultValue
			fields[i] = fieldMap
		}
	}

	schema["fields"] = fields

	// แปลงกลับเป็น JSON string
	modifiedJSON, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to marshal modified schema: %w", err)
	}

	return string(modifiedJSON), nil
}

// normalizeSchemaJSON ทำให้ schema JSON เป็นรูปแบบเดียวกันสำหรับการเปรียบเทียบ
func normalizeSchemaJSON(schemaJSON string) (string, error) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return "", err
	}

	// Sort fields array by field name เพื่อให้เปรียบเทียบได้แม่นยำ
	if fields, ok := schema["fields"].([]interface{}); ok {
		// Convert to slice of maps for sorting
		fieldMaps := make([]map[string]interface{}, len(fields))
		for i, field := range fields {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				fieldMaps[i] = fieldMap
			}
		}

		// Sort by field name
		for i := 0; i < len(fieldMaps)-1; i++ {
			for j := i + 1; j < len(fieldMaps); j++ {
				nameI, _ := fieldMaps[i]["name"].(string)
				nameJ, _ := fieldMaps[j]["name"].(string)
				if nameI > nameJ {
					fieldMaps[i], fieldMaps[j] = fieldMaps[j], fieldMaps[i]
				}
			}
		}

		// Convert back to []interface{}
		sortedFields := make([]interface{}, len(fieldMaps))
		for i, fieldMap := range fieldMaps {
			sortedFields[i] = fieldMap
		}
		schema["fields"] = sortedFields
	}

	normalized, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

// ensureSchemaMatchesStruct ตรวจสอบและ register schema ถ้าไม่ตรงกับ struct
func ensureSchemaMatchesStruct(srClient *srclient.SchemaRegistryClient, subject string, structType reflect.Type) (*srclient.Schema, *goavro.Codec, error) {
	// แปลง struct -> Avro schema JSON
	schemaJSON, err := avroschema.Reflect(&Transaction{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to reflect schema: %w", err)
	}

	// เพิ่ม default values สำหรับ backward compatibility
	schemaJSON, err = addDefaultValues(schemaJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to add default values: %w", err)
	}

	fmt.Println("schemaJSON:", schemaJSON)

	// ดึง schema ที่มีอยู่
	existingSchema, err := srClient.GetLatestSchema(subject)
	if err != nil {
		log.Printf("⚠️  Schema not found, registering new schema...")
		return registerSchema(srClient, subject, schemaJSON)
	}

	existingSchemaJSON := existingSchema.Schema()
	log.Printf("📋 Existing schema (ID: %d, Version: %d): %s", existingSchema.ID(), existingSchema.Version(), existingSchemaJSON)

	// เปรียบเทียบ schema JSON โดยตรง (normalize ก่อนเปรียบเทียบ)
	normalizedNew, err := normalizeSchemaJSON(schemaJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to normalize new schema: %w", err)
	}

	normalizedExisting, err := normalizeSchemaJSON(existingSchemaJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to normalize existing schema: %w", err)
	}

	// ถ้า schema ไม่ตรงกัน (แม้จะ compatible) ให้ register schema ใหม่
	if normalizedNew != normalizedExisting {
		log.Printf("⚠️  Schema structure changed (struct has new/modified fields), registering new version...")
		log.Printf("   Old schema: %s", normalizedExisting)
		log.Printf("   New schema: %s", normalizedNew)

		// ตรวจสอบ compatibility ก่อน register
		log.Printf("🔍 Checking compatibility:")
		log.Printf("   Subject: %s", subject)
		log.Printf("   Version: %d", existingSchema.Version())
		log.Printf("   Schema to check: %s", schemaJSON)

		// Debug: แสดง payload ที่จะส่งไป (format ที่ library จะสร้าง)
		payloadJSON := fmt.Sprintf(`{"schema":%q,"schemaType":"AVRO"}`, schemaJSON)
		log.Printf("   📤 Payload that will be sent: %s", payloadJSON)

		isCompatible, err := srClient.IsSchemaCompatible(subject, schemaJSON, fmt.Sprintf("%d", existingSchema.Version()), srclient.Avro)
		if err != nil {
			return nil, nil, fmt.Errorf("❌ Schema compatibility check failed: %v", err)
		}
		if !isCompatible {
			return nil, nil, fmt.Errorf("❌ Schema is NOT backward compatible! Cannot register new version. Old schema has fields that new schema is missing")
		}

		log.Printf("   ✅ Compatibility check result: %v", isCompatible)

		log.Printf("✅ Schema is backward compatible, registering new version...")
		return registerSchema(srClient, subject, schemaJSON)
	}

	log.Printf("✅ Schema matches struct exactly (ID: %d, Version: %d)", existingSchema.ID(), existingSchema.Version())
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

	// ตั้งค่า compatibility level เป็น BACKWARD สำหรับ subject
	compatibilityLevel, err := srClient.ChangeSubjectCompatibilityLevel(topic, srclient.Forward)
	if err != nil {
		log.Printf("⚠️  Failed to set compatibility level (may already be set): %v", err)
		// ตรวจสอบ compatibility level ปัจจุบัน
		currentLevel, err := srClient.GetCompatibilityLevel(topic, true)
		if err != nil {
			log.Printf("⚠️  Failed to get compatibility level: %v", err)
		} else {
			log.Printf("📋 Current compatibility level: %s", *currentLevel)
		}
	} else {
		log.Printf("✅ Set compatibility level to: %s", *compatibilityLevel)
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
		// txn.ReceivedAt = time.Now().Add(time.Duration(i) * time.Millisecond).Format("2006-01-02 15:04:05")

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
			log.Printf("⚠️  Test: Sending transaction with extra field 'amount' that is NOT in schema")
		}

		// Serialize ด้วย goavro
		// แปลง struct เป็น map สำหรับ goavro
		txnMap := map[string]interface{}{
			"id":          txn.ID,
			"type":        txn.Type,
			"terminal_id": txn.TerminalID,
			// "received_at": txn.ReceivedAt,
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

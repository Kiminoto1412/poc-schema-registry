package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

// SchemaRegistryClient สำหรับเรียก Schema Registry REST API
type SchemaRegistryClient struct {
	baseURL string
	client  *http.Client
}

// SchemaMetadata เก็บข้อมูล schema จาก Schema Registry
type SchemaMetadata struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// NewSchemaRegistryClient สร้าง Schema Registry client ใหม่
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// GetLatestSchema ดึง latest schema จาก Schema Registry
func (sr *SchemaRegistryClient) GetLatestSchema(subject string) (*SchemaMetadata, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", sr.baseURL, subject)

	resp, err := sr.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var metadata SchemaMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &metadata, nil
}

// CheckCompatibility ตรวจสอบว่า schema ใหม่ compatible กับ latest version หรือไม่
// ใช้ Schema Registry API ในการตรวจสอบ
func (sr *SchemaRegistryClient) CheckCompatibility(subject string, schemaJSON string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", sr.baseURL, subject)

	requestBody := map[string]string{
		"schema": schemaJSON,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := sr.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// ถ้า status code ไม่ใช่ 200 หรือ 404 (subject ไม่มี) แสดงว่าไม่ compatible
	if resp.StatusCode == http.StatusNotFound {
		// Subject ยังไม่มี → compatible (สามารถ register ได้)
		return true, nil
	}

	if resp.StatusCode != http.StatusOK {
		// Schema Registry return error → ไม่ compatible
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("schema registry compatibility check failed (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.IsCompatible, nil
}

// RegisterSchema register schema ใหม่ไปยัง Schema Registry
func (sr *SchemaRegistryClient) RegisterSchema(subject string, schemaJSON string) (*SchemaMetadata, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", sr.baseURL, subject)

	requestBody := map[string]string{
		"schema": schemaJSON,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := sr.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// ดึง schema ที่ register แล้ว
	time.Sleep(100 * time.Millisecond) // รอให้ Schema Registry propagate
	return sr.GetLatestSchema(subject)
}

// generateAvroSchemaFromStruct สร้าง Avro schema JSON จาก struct fields
func generateAvroSchemaFromStruct(structFields map[string]string) string {
	fields := make([]map[string]interface{}, 0, len(structFields))

	for avroName, goType := range structFields {
		field := map[string]interface{}{
			"name": avroName,
		}

		// แปลง Go type เป็น Avro type
		var avroType interface{}
		switch goType {
		case "string":
			avroType = "string"
		case "int64":
			avroType = "long"
		case "int32":
			avroType = "int"
		case "float64":
			// สำหรับ float64 ให้เป็น union type ["null", "double"] เพื่อรองรับ optional
			avroType = []interface{}{"null", "double"}
			field["default"] = nil
		case "float32":
			avroType = []interface{}{"null", "float"}
			field["default"] = nil
		case "bool":
			avroType = "boolean"
		default:
			avroType = "string" // default
		}
		field["type"] = avroType

		fields = append(fields, field)
	}

	schema := map[string]interface{}{
		"type":   "record",
		"name":   "Transaction",
		"fields": fields,
	}

	schemaJSON, _ := json.Marshal(schema)
	return string(schemaJSON)
}

// ensureSchemaMatchesStruct ตรวจสอบและ register schema ถ้าไม่ตรงกับ struct
// ใช้ Schema Registry compatibility check API แทนการ validate เอง
func ensureSchemaMatchesStruct(srClient *SchemaRegistryClient, subject string, structFields map[string]string) (*SchemaMetadata, *goavro.Codec, error) {
	// สร้าง schema จาก struct
	structSchemaJSON := generateAvroSchemaFromStruct(structFields)

	// ดึง schema ที่มีอยู่
	existingSchema, err := srClient.GetLatestSchema(subject)
	if err != nil {
		// ถ้ายังไม่มี schema ให้ register ใหม่
		log.Printf("⚠️  Schema not found, registering new schema...")
		newSchema, err := srClient.RegisterSchema(subject, structSchemaJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to register schema: %w", err)
		}
		log.Printf("✅ Registered new schema (ID: %d)", newSchema.ID)

		codec, err := goavro.NewCodec(newSchema.Schema)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
		}
		return newSchema, codec, nil
	}

	// Log schemas เพื่อ debug
	log.Printf("🔍 Checking compatibility...")
	log.Printf("   Existing schema from Registry: %s", existingSchema.Schema)
	log.Printf("   Struct schema to check: %s", structSchemaJSON)

	// ตรวจสอบว่า field ใน struct ตรงกับ Registry หรือไม่
	// แปลง Registry schema เป็น map เพื่อตรวจสอบ fields
	var registrySchema map[string]interface{}
	if err := json.Unmarshal([]byte(existingSchema.Schema), &registrySchema); err != nil {
		return nil, nil, fmt.Errorf("failed to parse registry schema: %w", err)
	}

	registryFields, ok := registrySchema["fields"].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("invalid registry schema format")
	}

	// สร้าง map ของ field names ใน Registry
	registryFieldMap := make(map[string]bool)
	for _, field := range registryFields {
		if fieldMap, ok := field.(map[string]interface{}); ok {
			if name, ok := fieldMap["name"].(string); ok {
				registryFieldMap[name] = true
			}
		}
	}

	// ตรวจสอบว่า struct fields อยู่ใน Registry หรือไม่
	var missingFields []string
	for structFieldName := range structFields {
		if !registryFieldMap[structFieldName] {
			missingFields = append(missingFields, structFieldName)
		}
	}

	// ถ้ามี field ใน struct ที่ไม่มีใน Registry → auto-register
	if len(missingFields) > 0 {
		log.Printf("⚠️  Struct has fields not in Registry: %v", missingFields)
		log.Printf("🔄 Auto-registering new schema...")

		// Register schema ใหม่
		newSchema, err := srClient.RegisterSchema(subject, structSchemaJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to register new schema: %w", err)
		}
		log.Printf("✅ Registered new schema version (ID: %d, Version: %d)", newSchema.ID, newSchema.Version)

		codec, err := goavro.NewCodec(newSchema.Schema)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
		}
		return newSchema, codec, nil
	}

	// ใช้ Schema Registry API ตรวจสอบ compatibility
	isCompatible, err := srClient.CheckCompatibility(subject, structSchemaJSON)
	if err != nil {
		// Schema Registry return error → ไม่ compatible → auto-register
		log.Printf("⚠️  Schema compatibility check failed (not compatible): %v", err)
		log.Printf("🔄 Auto-registering new schema...")

		// Register schema ใหม่
		newSchema, err := srClient.RegisterSchema(subject, structSchemaJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to register new schema: %w", err)
		}
		log.Printf("✅ Registered new schema version (ID: %d, Version: %d)", newSchema.ID, newSchema.Version)

		codec, err := goavro.NewCodec(newSchema.Schema)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
		}
		return newSchema, codec, nil
	}

	if !isCompatible {
		// Schema Registry บอกว่าไม่ compatible → auto-register
		log.Printf("⚠️  Schema not compatible with latest version! (is_compatible = false)")
		log.Printf("🔄 Auto-registering new schema...")

		// Register schema ใหม่
		newSchema, err := srClient.RegisterSchema(subject, structSchemaJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to register new schema: %w", err)
		}
		log.Printf("✅ Registered new schema version (ID: %d, Version: %d)", newSchema.ID, newSchema.Version)

		codec, err := goavro.NewCodec(newSchema.Schema)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
		}
		return newSchema, codec, nil
	}

	// Schema compatible และ fields ตรงกัน → ใช้ schema ที่มีอยู่
	log.Printf("✅ Schema is compatible with latest version and all struct fields exist in Registry (ID: %d)", existingSchema.ID)
	codec, err := goavro.NewCodec(existingSchema.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse schema: %w", err)
	}
	return existingSchema, codec, nil
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

	// สร้าง Schema Registry client
	srClient := NewSchemaRegistryClient(schemaRegistryURL)

	// --- ตัวอย่างข้อมูล ---
	type Transaction struct {
		ID         string  `avro:"id"`          // Map Go field "ID" to Avro field "id"
		Type       string  `avro:"type"`        // Map Go field "Type" to Avro field "type"
		TerminalID int64   `avro:"terminal_id"` // Map Go field "TerminalID" to Avro field "terminal_id"
		ReceivedAt string  `avro:"received_at"` // Map Go field "ReceivedAt" to Avro field "received_at"
		Amount     float64 `avro:"amount"`      // Field ที่มีใน schema (union type)
		Amount2    float64 `avro:"amount2"`     // Field ที่มีใน schema (union type)
	}

	// สร้าง map ของ struct fields สำหรับตรวจสอบ schema
	structFields := map[string]string{
		"id":          "string",
		"type":        "string",
		"terminal_id": "int64",
		"received_at": "string",
		"amount":      "float64", // จะต้องเป็น union type ["null", "double"] ใน schema
		"amount2":     "float64", // จะต้องเป็น union type ["null", "double"] ใน schema
	}

	// ตรวจสอบและ register schema ถ้าไม่ตรงกับ struct
	subject := topic
	latestSchema, codec, err := ensureSchemaMatchesStruct(srClient, subject, structFields)
	if err != nil {
		log.Fatalf("❌ Failed to ensure schema matches struct: %v", err)
	}

	log.Printf("✅ Using schema ID: %d", latestSchema.ID)

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
	numTransactions := 5 // 1 ล้าน records

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
	var structConversionFailCount int64

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
		valueBytes := createConfluentFormat(latestSchema.ID, avroBytes)

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
	log.Printf("   • Struct Conversion Failed: %d", structConversionFailCount)
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

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
	"google.golang.org/protobuf/proto"

	// Import the generated protobuf code
	// Note: You'll need to generate this from transaction.proto using:
	// protoc --go_out=. --go_opt=paths=source_relative shared/schemas/transaction.proto
	transactionpb "local_db/proto"
)

// createConfluentFormat creates Confluent Schema Registry wire format:
// [magic byte (1 byte)][schema ID (4 bytes)][protobuf data (variable)]
func createConfluentFormat(schemaID int, protoBytes []byte) []byte {
	var schemaBuf bytes.Buffer
	schemaBuf.WriteByte(0) // magic byte (same for protobuf)
	binary.Write(&schemaBuf, binary.BigEndian, int32(schemaID))
	schemaBuf.Write(protoBytes)
	return schemaBuf.Bytes()
}

func main() {
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions"

	log.Println("🚀 Transaction Service (Producer with Schema Registry - Protobuf) starting...")

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
	var serializationFailCount int64

	// --- ส่ง message ---
	for i := 0; i < numTransactions; i++ {
		// สร้าง transaction แบบ dynamic
		txn := &transactionpb.Transaction{}

		// สร้าง ID ที่ unique (TXN_0000001, TXN_0000002, ...)
		txn.Id = fmt.Sprintf("TXN_%07d", i+1)

		// ใช้ transaction type แบบวนรอบจาก array
		txn.Type = transactionTypes[i%len(transactionTypes)]

		// Terminal ID แบบสุ่มระหว่าง 1-1000
		txn.TerminalId = int64((i % 1000) + 1)

		// Timestamp ที่เพิ่มขึ้นเล็กน้อยสำหรับแต่ละ record
		txn.ReceivedAt = time.Now().Add(time.Duration(i) * time.Millisecond).Format("2006-01-02 15:04:05")

		// เพิ่ม special test cases ที่ตำแหน่งที่กำหนด
		if i == 999998 {
			txn.Id = "FAIL_TEST"
			txn.TerminalId = 999
		} else if i == 999999 {
			txn.Id = "DLQ_TEST"
			txn.TerminalId = 888
		}

		// Serialize ด้วย protobuf
		protoBytes, err := proto.Marshal(txn)
		if err != nil {
			serializationFailCount++
			log.Printf("❌ Protobuf serialization failed for txn %d: %v", i+1, err)
			continue
		}

		// สร้าง Confluent Schema Registry format: [magic byte][schema ID][protobuf data]
		valueBytes := createConfluentFormat(latestSchema.ID, protoBytes)

		// Convert binary data to string for common library
		// sarama.StringEncoder will convert string back to []byte correctly
		valueString := string(valueBytes)

		// Send message using common library
		result, err := kafkaClient.SendMessage(kafka.SendMessageParam{
			Topic:   topic,
			Key:     txn.Id,
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
				i+1, numTransactions, txn.Id, txn.Type, txn.TerminalId, result.Partition, result.Offset)
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
}

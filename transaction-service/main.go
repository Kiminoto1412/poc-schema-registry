package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"gitlab.bigc-cs.com/pos-transformation/pos-go-common/kafka"
)

func main() {
	bootstrapServers := "localhost:9094"
	schemaRegistryURL := "http://localhost:8083"
	topic := "transactions-value"

	log.Println("🚀 Transaction Service (Producer with Schema Registry) starting...")

	// Create Schema Registry client
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatalf("❌ Failed to create schema registry client: %v", err)
	}

	// สร้าง Confluent Avro Serializer
	avroSerializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("❌ Failed to create Avro serializer: %v", err)
	}

	log.Println("✅ Created Confluent Avro serializer")

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

		// เพิ่ม special test cases ที่ตำแหน่งที่กำหนด
		if i == 999998 {
			txn.ID = "FAIL_TEST"
			txn.TerminalID = 999
		} else if i == 999999 {
			txn.ID = "DLQ_TEST"
			txn.TerminalID = 888
		}

		// Serialize ด้วย Confluent Avro serializer
		// Serializer จะจัดการ Confluent wire format (magic byte + schema ID + avro data) อัตโนมัติ
		valueBytes, err := avroSerializer.Serialize(topic, &txn)
		if err != nil {
			serializationFailCount++
			log.Printf("❌ Avro serialization failed for txn %d: %v", i+1, err)
			continue
		}

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

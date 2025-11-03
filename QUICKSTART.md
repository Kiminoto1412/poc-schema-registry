# ⚡ Quick Start Guide

## 🎯 ทดสอบ Kafka POC ใน 3 ขั้นตอน

### ✅ Step 1: ตรวจสอบ Infrastructure

```bash
# ตรวจสอบว่า services ทำงานอยู่
docker-compose ps

# ควรเห็น:
# - poc-kafka: Up
# - poc-schema-registry: Up  
# - poc-kafka-ui: Up

# ถ้ายังไม่ทำงาน ให้ start:
docker-compose up -d
```

---

### ✅ Step 2: สร้าง Consumer (Terminal 1)

```bash
cd crs-service

# Download dependencies
go mod download

# Run consumer
go run main.go
```

**Expected Output:**
```
🚀 CRS Service (Consumer) starting...
✅ Subscribed to topic: transactions
⏳ Waiting for transactions...
```

**ปล่อย terminal นี้ให้รันไว้** 🟢

---

### ✅ Step 3: ส่ง Transactions (Terminal 2)

เปิด terminal ใหม่:

```bash
cd transaction-service

# Download dependencies
go mod download

# Run producer
go run main.go
```

**Expected Output:**
```
🚀 Transaction Service starting...
✅ Connected to Kafka
✅ Sent transaction 1: ID=1001, Type=CONTROL, Terminal=1
✅ Sent transaction 2: ID=1002, Type=CONTROL, Terminal=2
✅ Sent transaction 3: ID=1003, Type=CONTROL, Terminal=3
✅ All transactions sent successfully!
```

---

### 🎉 ดูผลลัพธ์ใน Terminal 1

ใน Terminal 1 (Consumer) จะเห็น:

```
📨 Received Transaction:
   ID: 1001
   Type: CONTROL
   Terminal ID: 1
   Received At: 2025-11-02 21:30:45
   ---
🔧 Processing transaction ID: 1001
✅ Transaction 1001 processed successfully

📨 Received Transaction:
   ID: 1002
   Type: CONTROL
   Terminal ID: 2
   ...
```

---

## 🌐 เปิด Web UI

### Kafka UI
http://localhost:8084

ดู:
- **Topics** → `transactions` → Messages
- **Consumer Groups** → `crs-service-group`

### Schema Registry
http://localhost:8083/subjects

---

## 🔍 Troubleshooting

### Topic ไม่มี
```bash
# สร้าง topic
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server poc-kafka:29092 \
  --partitions 1 \
  --replication-factor 1
```

### Error: Cannot connect to broker
```bash
# Restart services
docker-compose restart

# Wait 10 seconds
sleep 10

# Try again
```

### Dependencies ไม่ครบ
```bash
cd transaction-service
go mod tidy

cd ../crs-service
go mod tidy
```

---

## 📚 Next Steps

ดูเอกสารเพิ่มเติม:
- **Full README**: `POC_README.md`
- **Architecture**: `POC_README.md` (Architecture section)
- **Schema Examples**: `schema_examples.md`
- **Kafka Guide**: `kafka_topic_guide.md`


# 🚀 Kafka + Schema Registry POC

POC แสดงการใช้งาน Kafka กับ Schema Registry สำหรับ microservices communication

---

## 📦 Architecture

```
┌─────────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│ Transaction Service │────────▶│  Kafka Broker    │────────▶│  CRS Service    │
│   (Producer)        │         │  - Topic:        │         │  (Consumer)     │
│                     │         │    transactions  │         │                 │
│  Sends transactions │         │                  │         │  Processes      │
│  to Kafka           │         │  Port: 9094      │         │  transactions   │
└─────────────────────┘         └──────────────────┘         └─────────────────┘
                                        ▲
                                        │
                                ┌───────┴────────┐
                                │ Schema Registry│
                                │                │
                                │  Port: 8083    │
                                └────────────────┘

                          ┌─────────────────────┐
                          │    Kafka UI         │
                          │                     │
                          │  Port: 8084         │
                          └─────────────────────┘
```

---

## 🗂️ Project Structure

```
poc-schema-registry/
├── docker-compose.yml              # Kafka, Schema Registry, Kafka UI
├── transaction-service/            # Producer service
│   ├── main.go
│   ├── go.mod
│   └── README.md
├── crs-service/                    # Consumer service
│   ├── main.go
│   ├── go.mod
│   └── README.md
├── shared/
│   └── schemas/
│       └── transaction.avsc        # Avro schema (future use)
├── Schema_Registry_API.postman_collection.json
├── schema_examples.md
├── kafka_topic_guide.md
├── topic_transactions_config.md
└── README1.md                      # Original README
```

---

## 🚀 Quick Start

### 1. Start Infrastructure

```bash
# Start Kafka, Schema Registry, Kafka UI
docker-compose up -d

# Verify all services are running
docker-compose ps
```

**Expected output:**
```
NAME                 STATUS          PORTS
poc-kafka            Up              0.0.0.0:9094->9092/tcp, ...
poc-schema-registry  Up              0.0.0.0:8083->8081/tcp
poc-kafka-ui         Up              0.0.0.0:8084->8080/tcp
```

### 2. Create Kafka Topic (Optional - if not created yet)

**Option A: Use Kafka UI**
1. เปิด http://localhost:8084
2. ไปที่ **Topics** → **Create Topic**
3. ตั้งชื่อ: `transactions`
4. Partitions: `1`, Replication Factor: `1`
5. คลิก **Create topic**

**Option B: Use Command Line**
```bash
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server poc-kafka:29092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. Start Consumer (CRS Service)

**Terminal 1:**
```bash
cd crs-service
go mod download
go run main.go
```

**Expected output:**
```
🚀 CRS Service (Consumer) starting...
✅ Subscribed to topic: transactions
⏳ Waiting for transactions...
```

### 4. Start Producer (Transaction Service)

**Terminal 2:**
```bash
cd transaction-service
go mod download
go run main.go
```

**Expected output:**
```
🚀 Transaction Service starting...
✅ Connected to Kafka
✅ Sent transaction 1: ID=1001, Type=CONTROL, Terminal=1
✅ Sent transaction 2: ID=1002, Type=CONTROL, Terminal=2
✅ Sent transaction 3: ID=1003, Type=CONTROL, Terminal=3
✅ All transactions sent successfully!
```

### 5. Watch Consumer Receive Messages

ใน **Terminal 1** (CRS Service) จะเห็น:

```
📨 Received Transaction:
   ID: 1001
   Type: CONTROL
   Terminal ID: 1
   Received At: 2025-11-02 20:30:45
   ---
🔧 Processing transaction ID: 1001
✅ Transaction 1001 processed successfully
```

---

## 🔍 Verify in Web UI

### Kafka UI
เปิด http://localhost:8084

ดูได้ที่:
- **Topics** → `transactions` → Messages
- **Consumer Groups** → `crs-service-group`

### Schema Registry (Optional)
เปิด http://localhost:8083/subjects

ตัวอย่างคำสั่ง:
```bash
# List all subjects
curl http://localhost:8083/subjects

# Get topics
curl http://localhost:8083/subjects/transactions-value
```

---

## 🧪 Testing Commands

### Manual Producer Test

```bash
docker exec -it poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094
```

พิมพ์:
```json
{"id":"999","type":"MANUAL","terminal_id":99,"received_at":"2025-11-02 12:00:00"}
```

### Manual Consumer Test

```bash
docker exec -it poc-kafka kafka-console-consumer \
  --topic transactions \
  --from-beginning \
  --bootstrap-server localhost:9094
```

### List Topics

```bash
docker exec poc-kafka kafka-topics --list \
  --bootstrap-server localhost:9094
```

### Describe Topic

```bash
docker exec poc-kafka kafka-topics --describe \
  --topic transactions \
  --bootstrap-server localhost:9094
```

---

## 📊 Message Flow

```
1. Transaction Service
   └─> JSON: {"id":"1001","type":"CONTROL","terminal_id":1,"received_at":"..."}
       └─> Serialize to JSON bytes
           └─> Kafka Producer
               └─> Send to topic: "transactions"

2. Kafka Broker
   └─> Store message in partition
       └─> Return acknowledgment

3. CRS Service
   └─> Consumer poll
       └─> Receive JSON bytes
           └─> Deserialize to Transaction struct
               └─> Process transaction
                   └─> Log result
```

---

## 🔧 Troubleshooting

### Port Already in Use

```bash
# Check what's using the port
lsof -i :9094
lsof -i :8083
lsof -i :8084

# Stop conflicting containers
docker stop kafka kafka-ui
```

### No Messages Received

1. ตรวจสอบ Topic สร้างแล้ว:
```bash
docker exec poc-kafka kafka-topics --list --bootstrap-server localhost:9094
```

2. ตรวจสอบ Consumer group:
```bash
docker exec poc-kafka kafka-consumer-groups --describe \
  --group crs-service-group \
  --bootstrap-server localhost:9094
```

3. ดู logs:
```bash
docker-compose logs -f
```

### Service Won't Start

```bash
# Check if Kafka is accessible
docker exec poc-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9094

# Restart services
docker-compose restart
```

---

## 📚 Additional Documentation

- **Schema Registry API**: `Schema_Registry_API.postman_collection.json`
- **Examples**: `schema_examples.md`
- **Kafka Topic Guide**: `kafka_topic_guide.md`
- **Transaction Topic Config**: `topic_transactions_config.md`
- **Original README**: `README1.md`

---

## 🎯 Next Steps

### Phase 1: Basic JSON ✅
- [x] Kafka infrastructure
- [x] Transaction Service (Producer)
- [x] CRS Service (Consumer)
- [x] Manual testing

### Phase 2: Schema Registry Integration
- [ ] Register Avro schema
- [ ] Use Avro serialization
- [ ] Schema evolution testing
- [ ] Compatibility checks

### Phase 3: Advanced Features
- [ ] Dead Letter Queue (DLQ)
- [ ] Retry mechanism
- [ ] Metrics & monitoring
- [ ] Multi-partition handling

---

## 🛠️ Technologies Used

- **Kafka**: Apache Kafka 7.6.0 (KRaft mode)
- **Schema Registry**: Confluent 7.6.0
- **Kafka UI**: kafbat/kafka-ui
- **Language**: Go 1.21+
- **Libraries**: confluent-kafka-go v2

---

## 📝 Notes

- **KRaft Mode**: ใช้ Kafka without Zookeeper
- **Single Broker**: สำหรับ development/ POC only
- **JSON Format**: ปัจจุบันใช้ JSON serialization (สามารถ upgrade เป็น Avro ได้)
- **Port Mapping**:
  - Kafka: `9094` (external) → `9092` (internal)
  - Schema Registry: `8083` → `8081`
  - Kafka UI: `8084` → `8080`

---

## 🧾 License

MIT License © 2025


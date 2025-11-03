# 📖 Kafka Topic และ Schema Registry ใช้งานอย่างไร

## 🎯 Topic คืออะไร?

**Topic** = ช่องสื่อสาร (channel) ใน Kafka ที่ producer ส่งข้อความ และ consumer ดึงข้อความ

### เปรียบเทียบกับชีวิตจริง:
```
Topic = ช่องทางสื่อสาร
├─ LINE: ส่งข้อความได้หลายคน หลายคนอ่านได้
├─ Facebook: หลายคนโพสต์ หลายคนเห็น
└─ Topic: หลาย Producer ส่ง, หลาย Consumer ดึง
```

---

## 🔗 ความสัมพันธ์ระหว่าง Topic กับ Schema Registry

### ทั่วไป:
```
┌─────────────────┐         ┌──────────────────┐         ┌──────────────┐
│  Schema Registry │         │   Kafka Topic    │         │  Services    │
│                 │         │                  │         │              │
│  - Avro Schema  │◄───────►│  transactions    │◄───────►│  Producer    │
│  - JSON Schema  │         │  products        │         │  Consumer    │
│  - Protobuf     │         │  orders          │         │              │
└─────────────────┘         └──────────────────┘         └──────────────┘
```

### Workflow:
1. **Register Schema** → Schema Registry เก็บโครงสร้างข้อมูล
2. **Create Topic** → Kafka สร้างช่องทางสื่อสาร
3. **Producer ส่ง** → Serialize ด้วย schema → ส่งเข้า topic
4. **Consumer รับ** → Deserialize ด้วย schema → ใช้งานข้อมูล

---

## 🎬 ใช้งาน Topic ตอนไหน?

### Scenario ที่ต้องสร้าง Topic:

#### 1️⃣ **Event-Driven Architecture (Microservices)**
```
Service A → Topic "user-registered" → Service B, C, D
```
**เมื่อ**: Service ต้องการส่ง event ให้หลาย services
**ทำไม**: แยก coupling, scale ได้

#### 2️⃣ **Data Streaming (Real-time)**
```
Database → Topic "transactions" → Analytics, Reports, API
```
**เมื่อ**: ต้องการ stream ข้อมูลแบบ real-time
**ทำไม**: ประมวลผลทันที

#### 3️⃣ **CDC (Change Data Capture)**
```
MySQL → Topic "transactions" → Elasticsearch, Data Warehouse
```
**เมื่อ**: ต้องการ sync ข้อมูลระหว่างระบบ
**ทำไม**: หลีกเลี่ยง polling

#### 4️⃣ **Log Aggregation**
```
Apps → Topic "app-logs" → Logging System
```
**เมื่อ**: รวม logs จากหลาย services
**ทำไม**: centralized logging

---

## 📊 ตัวอย่าง: Transaction System

### 1. สร้าง Topic
```bash
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9094 \
  --partitions 1 \
  --replication-factor 1
```

**ตั้งชื่อตาม Domain**: `transactions`, `orders`, `payments`

---

### 2. Register Schema ไปที่ Schema Registry
```bash
curl -X POST http://localhost:8083/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":\"string\"}]}"
  }'
```

**Subject name** = `{topic-name}-{key|value}`
- `transactions-value` = schema สำหรับ value ของ message
- `transactions-key` = schema สำหรับ key ของ message (optional)

---

### 3. ส่งข้อมูลเข้า Topic (Producer)

#### แบบ Text (ไม่มี Schema):
```bash
docker exec poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094

# พิมพ์:
1001,CONTROL,1,2025-09-02 07:55:20
```

#### แบบ JSON (ใช้ Schema):
```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json

# Setup Schema Registry
schema_registry_conf = {'url': 'http://localhost:8083'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Get schema
schema_str = """{
  "type": "record",
  "name": "Transaction",
  "namespace": "local_db",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "type", "type": "string"},
    {"name": "terminal_id", "type": "long"},
    {"name": "received_at", "type": "string"}
  ]
}"""

# Serialize
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'value.serializer': avro_serializer
}
producer = Producer(producer_conf)

# Send message
transaction = {
    'id': '1001',
    'type': 'CONTROL',
    'terminal_id': 1,
    'received_at': '2025-09-02T07:55:20'
}
producer.produce('transactions', value=transaction)
producer.flush()
print("✅ Message sent!")
```

---

### 4. รับข้อมูลจาก Topic (Consumer)

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Setup Consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'transaction-processor',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# Deserialize
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8083'})
avro_deserializer = AvroDeserializer(schema_registry_client)

# Subscribe
consumer.subscribe(['transactions'])

# Consume
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Error: {msg.error()}")
        continue
    
    # Deserialize
    transaction = avro_deserializer(msg.value(), None)
    print(f"✅ Received: {transaction}")
    
    # Process
    process_transaction(transaction)
```

---

## 🏗️ สถาปัตยกรรมตัวอย่าง

### Example: E-Commerce System

```
┌─────────────────────────────────────────────────────────────┐
│                    Schema Registry                          │
│  - products-value                                           │
│  - orders-value                                             │
│  - users-value                                              │
└─────────────────────────────────────────────────────────────┘
                         ▲
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
┌─────────────┐  ┌──────────────┐  ┌─────────────┐
│ Topic:      │  │ Topic:       │  │ Topic:      │
│ products    │  │ orders       │  │ payments    │
├─────────────┤  ├──────────────┤  ├─────────────┤
│ Producer:   │  │ Producer:    │  │ Producer:   │
│ - Catalog   │  │ - Checkout   │  │ - Payment   │
│             │  │              │  │             │
│ Consumer:   │  │ Consumer:    │  │ Consumer:   │
│ - Search    │  │ - Inventory  │  │ - Banking   │
│ - Analytics │  │ - Shipping   │  │ - Email     │
└─────────────┘  └──────────────┘  └─────────────┘
```

---

## ⚙️ คำสั่ง Kafka ที่ใช้บ่อย

### สร้าง Topic
```bash
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9094 \
  --partitions 3 \
  --replication-factor 1
```

### ดู Topic ทั้งหมด
```bash
docker exec poc-kafka kafka-topics --list \
  --bootstrap-server localhost:9094
```

### ดูรายละเอียด Topic
```bash
docker exec poc-kafka kafka-topics --describe \
  --topic transactions \
  --bootstrap-server localhost:9094
```

### ลบ Topic
```bash
docker exec poc-kafka kafka-topics --delete \
  --topic transactions \
  --bootstrap-server localhost:9094
```

### ส่งข้อความ (Producer)
```bash
docker exec poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094
```

### รับข้อความ (Consumer)
```bash
docker exec poc-kafka kafka-console-consumer \
  --topic transactions \
  --from-beginning \
  --bootstrap-server localhost:9094
```

---

## 🎓 Best Practices

### 1. **ตั้งชื่อ Topic ให้ชัดเจน**
✅ `transactions`  
✅ `user-events`  
✅ `order-updates`  
❌ `topic1`  
❌ `test123`

### 2. **ใช้ Schema Registry ทุกครั้ง**
✅ ลดปัญหา version mismatch  
✅ Validate ข้อมูลอัตโนมัติ  
✅ Document โครงสร้างข้อมูล

### 3. **ตั้ง Partitions ตาม scale**
- **1 partition** = sequential, ง่าย debug
- **3+ partitions** = parallel processing
- **สูตร**: `partitions = consumers × 2`

### 4. **ตั้ง Consumer Group**
```python
'group.id': 'payment-processor'  # แต่ละ service ควรมี group ตัวเอง
```

---

## 📝 สรุป

| สิ่ง | หน้าที่ | ตอนไหนใช้ |
|------|---------|-----------|
| **Topic** | ช่องทางสื่อสาร | เมื่อ services ต้องแลกเปลี่ยนข้อมูล |
| **Schema** | โครงสร้างข้อมูล | Register ก่อนส่งข้อมูล |
| **Producer** | ส่งข้อมูล | เมื่อเกิด event/update |
| **Consumer** | รับข้อมูล | เมื่อต้องการ process ข้อมูล |
| **Schema Registry** | จัดการ schema | ตลอดเวลา (register/validate/version) |

---

## 🚀 Quick Start

```bash
# 1. สร้าง Topic
docker exec poc-kafka kafka-topics --create \
  --topic transactions --bootstrap-server localhost:9094 \
  --partitions 1 --replication-factor 1

# 2. Register Schema
curl -X POST http://localhost:8083/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":\"string\"}]}"}'

# 3. ดูใน Kafka UI
# เปิด: http://localhost:8084

# 4. Test Producer/Consumer
docker exec poc-kafka kafka-console-producer --topic transactions --bootstrap-server localhost:9094
docker exec poc-kafka kafka-console-consumer --topic transactions --from-beginning --bootstrap-server localhost:9094
```


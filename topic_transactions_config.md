# ⚙️ การตั้งค่า Topic: transactions

## 📋 ค่าแนะนำสำหรับ Development/Testing

### สำหรับการใช้งานใน Kafka UI (http://localhost:8084)

---

## ✅ ฟิลด์ที่ต้องกรอก (Required Fields)

### 1. **Topic Name** ⭐
```
transactions
```
- ใช้ชื่อเดียวกับที่ใช้ใน Schema Registry: `transactions-value`

---

### 2. **Number of Partitions** ⭐
```
1
```
**เหตุผล:**
- Development/Testing: 1 partition พอเพียง
- Single broker setup: ไม่ต้อง scale หลาย partitions
- การ debug ง่ายกว่า (ข้อมูล sequential)

**ถ้า Production:**
- เริ่มที่ `3` หรือ `5` partitions
- สูตร: `partitions = expected_consumers × 2`

---

### 3. **Replication Factor** ⭐
```
1
```
**เหตุผล:**
- Single broker environment (KRaft mode)
- Development: ไม่จำเป็นต้องมี replicas หลายตัว
- ถ้า replication factor > 1 ใน single broker จะเกิด error

**ถ้า Production (Multi-broker):**
- ใช้ `3` สำหรับ high availability

---

## 🔧 ฟิลด์แนะนำ (Recommended)

### 4. **Min In Sync Replicas**
```
1
```
**เหตุผล:**
- ต้อง ≤ Replication Factor
- ถ้า Replication Factor = 1 → Min In Sync = 1
- หมายถึง: ต้องมี replicas อย่างน้อย 1 ตัวที่ sync ก่อนจะยอมรับ message

---

### 5. **Cleanup Policy**
```
Delete (default)
```
**เหตุผล:**
- Transaction logs ไม่จำเป็นต้องเก็บถาวร
- Kafka จะลบข้อมูลเก่าตาม retention policy
- เหมาะสำหรับ event streaming

**ถ้าต้องการเก็บ latest state:**
- เปลี่ยนเป็น `Compact` (ถ้าต้องการ key-value retention)

---

### 6. **Time to retain data (in ms)**
```
7 days (604800000 ms)
```
**หรือเลือกปุ่ม:** `7 days`

**เหตุผล:**
- Default 7 วัน เหมาะสำหรับ transaction logs
- ถ้าต้องการเก็บนานขึ้น → เลือก `4 weeks`
- ถ้าต้องการเก็บสั้นลง → เลือก `1 day` หรือ `2 days`

**คำนวณ ms:**
- 7 days = 7 × 24 × 60 × 60 × 1000 = **604800000 ms**
- 1 day = **86400000 ms**
- 2 days = **172800000 ms**

---

## 🔍 ฟิลด์ที่ไม่จำเป็น (Optional)

### 7. **Max partition size in GB**
```
Not Set (default)
```
**เหตุผล:**
- ใช้ retention time แทน (ง่ายกว่า)
- ปล่อยให้ Kafka จัดการเอง

**ถ้าต้องการจำกัด:**
- เช่น `10 GB` = ถ้า partition ถึง 10GB จะลบข้อมูลเก่าทันที

---

### 8. **Maximum message size in bytes**
```
Not Set (default)
```
**หรือถ้าต้องการกำหนด:**
```
1048576 (1 MB)
```

**เหตุผล:**
- Transaction data ไม่ใหญ่มาก
- Default broker config มักเป็น 1 MB
- ถ้าไม่แน่ใจ ปล่อย Not Set ได้

**คำนวณขนาด:**
- Transaction record: ~100-500 bytes
- 1 MB = **1048576 bytes** (เพียงพอแล้ว)

---

### 9. **Custom parameters**
```
ไม่ต้องเพิ่ม
```
**ถ้าต้องการ advanced config:**
- `compression.type`: `gzip` หรือ `snappy` (ลดขนาด message)
- `min.insync.replicas`: ถ้ายังไม่ได้ตั้งด้านบน

---

## 📊 สรุปค่าแนะนำ

| ฟิลด์ | ค่าที่แนะนำ | เหตุผล |
|-------|------------|--------|
| **Topic Name** | `transactions` | ⭐ Required |
| **Number of Partitions** | `1` | Development, single broker |
| **Replication Factor** | `1` | Single broker environment |
| **Min In Sync Replicas** | `1` | ต้อง ≤ Replication Factor |
| **Cleanup Policy** | `Delete` | เหมาะกับ transaction logs |
| **Time to retain data** | `7 days` | Default, เหมาะกับ transactions |
| **Max partition size** | `Not Set` | ใช้ retention time แทน |
| **Maximum message size** | `Not Set` | Default 1MB เพียงพอ |

---

## 🚀 ขั้นตอนการสร้าง Topic

### วิธีที่ 1: ใช้ Kafka UI (แนะนำ)
1. เปิด http://localhost:8084
2. ไปที่ **Topics** → **Create Topic**
3. กรอกตามตารางด้านบน
4. คลิก **Create topic**

### วิธีที่ 2: ใช้ Command Line
```bash
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9094 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
```

---

## 🔄 หลังจากสร้าง Topic แล้ว

### 1. ตรวจสอบ Topic
```bash
docker exec poc-kafka kafka-topics --describe \
  --topic transactions \
  --bootstrap-server localhost:9094
```

### 2. Register Schema (ถ้ายังไม่ได้ทำ)
```bash
curl -X POST http://localhost:8083/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":\"string\"}]}"
  }'
```

### 3. Test Producer/Consumer
```bash
# Producer
docker exec poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094

# Consumer (terminal อีก tab)
docker exec poc-kafka kafka-console-consumer \
  --topic transactions \
  --from-beginning \
  --bootstrap-server localhost:9094
```

---

## 🎯 สำหรับ Production Environment

ถ้าไป Production ควรปรับเป็น:

| ฟิลด์ | Production Value | เหตุผล |
|-------|-----------------|--------|
| Partitions | `3` หรือ `5` | Parallel processing |
| Replication Factor | `3` | High availability |
| Min In Sync Replicas | `2` | Durability |
| Retention | `30 days` หรือมากกว่า | Compliance, audit |
| Compression | `gzip` (custom param) | ลด storage cost |

---

## ✅ Checklist

ก่อนสร้าง Topic ตรวจสอบ:
- [ ] Topic name: `transactions`
- [ ] Partitions: `1` (dev) หรือ `3+` (prod)
- [ ] Replication Factor: `1` (dev) หรือ `3` (prod)
- [ ] Min In Sync Replicas: `1` (≤ replication factor)
- [ ] Cleanup Policy: `Delete`
- [ ] Retention: `7 days` (หรือตาม business requirement)

หลังจากสร้าง Topic:
- [ ] ตรวจสอบใน Kafka UI ว่าสร้างสำเร็จ
- [ ] Register Schema ใน Schema Registry
- [ ] Test Producer/Consumer


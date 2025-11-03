# Avro vs JSON Comparison Demo

## 🤔 คำถาม: Avro กระทัดรัดยังไง? ดูเหมือน JSON นะ?

---

## 📝 สิ่งที่คุณเห็น (JSON Format)

ตอนที่เราส่ง schema ไป registry มันดูเหมือน JSON:

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "local_db",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "type", "type": "string"},
    {"name": "terminal_id", "type": "long"},
    {"name": "received_at", "type": {"type": "string", "logicalType": "timestamp-millis"}}
  ]
}
```

**นี่คือแค่ "Schema Definition" เท่านั้น!** (เหมือน blueprint)

---

## 🎯 ความแตกต่างจริงๆ อยู่ตรงไหน?

### JSON Format (Plain Text)
```
ข้อมูล + Schema ต้องไปด้วยกันทุกครั้ง
```

**Message ที่ส่งเข้าก Kafka:**

```json
{"id":"1001","type":"CONTROL","terminal_id":1,"received_at":"2025-09-02T07:55:20"}
```

**Size: ~70 bytes** (รวมกับ field names ซ้ำไปมา)

---

### Avro Binary Format (Compact)
```
Schema เก็บที่ Registry แล้ว
Message ส่งแค่ "values" เท่านั้น
```

**Message ที่ส่งเข้า Kafka:**

```
[binary data: bytes ที่ compact มาก]
00000000  00 00 00 00 01   14 31 30 30 31   0e 43 4f 4e 54 52 4f 4c  
00000010  02  01  14 32 30 32 35 2d 30 39 2d 30 32 54 30 37 3a 35 35 3a 32 30
```

**Size: ~35-40 bytes** (ขนาดเล็กลงเกือบ 50%!)

---

## 🔬 มันอัดแน่นยังไง?

Avro ใช้เทคนิคหลายอย่าง:

### 1️⃣ เก็บแค่ Values (ไม่มี Field Names)
```json
// JSON ต้องเก็บ field names ทุกครั้ง
{"id":"1001","type":"CONTROL","terminal_id":1,"received_at":"..."}
  ^^^^^^       ^^^^^^             ^^^^^^^^^^^^     ^^^^^^^^^^^^
  field names (กินที่เยอะมาก!)
```

```
// Avro เก็บแค่ values เรียงตามลำดับ
id: "1001"
type: "CONTROL"  
terminal_id: 1
received_at: "..."
```

### 2️⃣ Type Encoding ที่ประหยัด
```
long = 1  จะถูก encode เป็นแค่ 1 byte (not "1" = 2 bytes)
string = UTF-8 encoding ที่ optimized
```

### 3️⃣ Schema Compression
```
Schema เก็บไว้ที่ Registry
แต่ละ message แค่ใส่ Schema ID (4 bytes) เท่านั้น
```

---

## 📊 เปรียบเทียบจริงๆ

### Scenario: ส่งข้อมูล 10,000 transactions

| Format | Size per Message | Total for 10k | Schema Overhead |
|--------|------------------|---------------|-----------------|
| **JSON** | ~70 bytes | ~700 KB | เก็บทุกครั้ง |
| **Avro** | ~35 bytes | ~350 KB | ครั้งแรก 4 bytes |
| **Saved** | **50%** | **350 KB** | มากกว่า |

---

## 🎬 Actual Example

### JSON Message:
```json
{
  "id": "1001",
  "type": "CONTROL",
  "terminal_id": 1,
  "received_at": "2025-09-02T07:55:20"
}
```
**Size: 95 bytes** (with formatting)  
**Size: 70 bytes** (minified)

### Avro Binary Equivalent:
```
[Magic Byte: 0x00] [Schema ID: 0x00000001] 
[Value: id="1001"] 
[Value: type="CONTROL"]
[Value: terminal_id=1]
[Value: received_at="2025-09-02T07:55:20"]
```
**Size: ~40 bytes**

**ประหยัดได้ ~43%!** 🎉

---

## 🛠️ ดูไบนารีจริงๆ

ลองรัน Python script นี้:

```python
import json
import avro.schema
import avro.io
import io

# JSON version
json_data = {"id": "1001", "type": "CONTROL", "terminal_id": 1, "received_at": "2025-09-02T07:55:20"}
json_bytes = json.dumps(json_data).encode('utf-8')
print(f"JSON size: {len(json_bytes)} bytes")

# Avro version
schema_json = {
    "type": "record",
    "name": "Transaction",
    "namespace": "local_db",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "terminal_id", "type": "long"},
        {"name": "received_at", "type": "string"}
    ]
}
schema = avro.schema.parse(json.dumps(schema_json))

# Serialize
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
datum_writer = avro.io.DatumWriter(schema)
datum_writer.write(json_data, encoder)
avro_bytes = bytes_writer.getvalue()

print(f"Avro binary size: {len(avro_bytes)} bytes")
print(f"Bytes saved: {len(json_bytes) - len(avro_bytes)} bytes")
print(f"Avro data: {avro_bytes.hex()}")
```

---

## 🌟 ข้อดีอื่นๆ ของ Binary Format

### 1. **Faster Parsing**
```
JSON: ต้อง parse string → convert types → validate
Avro: อ่าน binary → ตรงไปตรงมา (เร็วกว่า 2-3x)
```

### 2. **Type Safety**
```
JSON: "123" = string หรือ number? ต้อง guess
Avro: schema บอกชัดเจน → type-checked
```

### 3. **Schema Evolution**
```
JSON: ถ้าเปลี่ยน schema → breaking change
Avro: Schema Registry ตรวจ compatibility อัตโนมัติ
```

---

## 📚 สรุป

| | JSON | Avro Binary |
|---|------|-------------|
| **รูปที่เห็น** | Text ธรรมดา | ไบนารี (hex bytes) |
| **Schema** | ต้องเก็บทุกครั้ง | เก็บที่ Registry |
| **ขนาด** | ใหญ่ | เล็ก ~50% |
| **ความเร็ว** | ธรรมดา | เร็วกว่า |
| **Type Safety** | ไม่มี | มี |
| **เหมาะกับ** | ทุกคนอ่านได้ | Production systems |

---

## 💡 สรุปง่ายๆ

> **JSON** = ภาษาพูดธรรมดา (อ่านง่ายแต่ยาว)  
> **Avro Binary** = รหัสโทรเลข (กระทัดรัดแต่เร็ว)

ทั้งสองเก็บข้อมูลเดียวกัน แต่ Avro ประหยัด bandwidth และ CPU มากกว่า! 🚀


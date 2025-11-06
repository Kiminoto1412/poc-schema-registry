# Flow การเปลี่ยน Type ใน Transaction Service (Producer)

## 📋 สรุป Flow โดยย่อ

```
Transaction struct 
  ↓ structToAvroMap()
map[string]interface{} 
  ↓ validateSchema()
map[string]interface{} (validated)
  ↓ codec.BinaryFromNative()
[]byte (Avro binary format)
  ↓ createConfluentFormat()
[]byte (Confluent Schema Registry format)
  ↓ string()
string
  ↓ kafkaClient.SendMessage()
Kafka Message (sent to topic)
```

---

## 🔍 Flow แบบละเอียดทีละ Function

### 1️⃣ เริ่มต้น: `Transaction` struct
**Location:** Line 176-197

```go
var txn Transaction  // Type: Transaction struct
```

**โครงสร้าง:**
```go
type Transaction struct {
    ID         string `avro:"id"`          
    Type       string `avro:"type"`        
    TerminalID int64  `avro:"terminal_id"` 
    ReceivedAt string `avro:"received_at"` 
}
```

**ตัวอย่างข้อมูล:**
```go
txn = Transaction{
    ID: "TXN_0000001",
    Type: "SALE",
    TerminalID: 1,
    ReceivedAt: "2024-01-01 12:00:00"
}
```

---

### 2️⃣ `structToAvroMap()` → แปลง struct เป็น map
**Location:** Line 48-91, Called at Line 199

**Input:** `Transaction` struct  
**Output:** `map[string]interface{}`

```go
record, err := structToAvroMap(txn)
// record = map[string]interface{}{
//     "id": "TXN_0000001",
//     "type": "SALE",
//     "terminal_id": int64(1),
//     "received_at": "2024-01-01 12:00:00"
// }
```

**สิ่งที่ทำ:**
- ใช้ reflection เพื่ออ่าน struct fields
- ใช้ struct tag `avro:"..."` เพื่อกำหนดชื่อ field ใน Avro
- ถ้าไม่มี tag จะแปลง PascalCase → snake_case อัตโนมัติ (เช่น `TerminalID` → `terminal_id`)
- แปลง Go struct → `map[string]interface{}` ที่ Avro เข้าใจได้

**Type เปลี่ยนจาก:** `Transaction` struct  
**Type เปลี่ยนเป็น:** `map[string]interface{}`

---

### 3️⃣ `validateSchema()` → Validate schema
**Location:** Line 35-46, Called at Line 209

**Input:** `map[string]interface{}`  
**Output:** `map[string]interface{}` (same type, but validated)

```go
if err := validateSchema(codec, record); err != nil {
    // Handle error
}
```

**สิ่งที่ทำ:**
- ใช้ `codec.BinaryFromNative()` เพื่อทดสอบว่า record ตรงกับ schema หรือไม่
- ถ้าไม่ตรงจะ return error ทันที (fail fast)
- ถ้าผ่าน validation จะดำเนินการต่อ

**Type:** ยังคงเป็น `map[string]interface{}` แต่ผ่าน validation แล้ว

---

### 4️⃣ `codec.BinaryFromNative()` → Serialize เป็น Avro binary
**Location:** Line 220 (goavro library)

**Input:** `map[string]interface{}`  
**Output:** `[]byte` (Avro binary format)

```go
avroBytes, err := codec.BinaryFromNative(nil, record)
// avroBytes = []byte{...} // Avro binary serialized data
```

**สิ่งที่ทำ:**
- Serialize `map[string]interface{}` เป็น Avro binary format (`[]byte`)
- ใช้ schema ที่ parse ไว้แล้ว (`codec`) เพื่อ encode ข้อมูล

**Type เปลี่ยนจาก:** `map[string]interface{}`  
**Type เปลี่ยนเป็น:** `[]byte` (Avro binary)

---

### 5️⃣ `createConfluentFormat()` → เพิ่ม Confluent Schema Registry header
**Location:** Line 93-101, Called at Line 228

**Input:** `int` (schema ID), `[]byte` (Avro binary)  
**Output:** `[]byte` (Confluent Schema Registry format)

```go
valueBytes := createConfluentFormat(latestSchema.ID, avroBytes)
// valueBytes = [0x00][schema_id_4_bytes][avro_bytes...]
```

**รูปแบบ Confluent Format:**
```
[Magic Byte (1 byte: 0x00)]
[Schema ID (4 bytes: big-endian int32)]
[Avro Binary Data (variable length)]
```

**สิ่งที่ทำ:**
- เพิ่ม magic byte (0x00) ที่ด้านหน้า
- เพิ่ม Schema ID (4 bytes, big-endian) หลังจาก magic byte
- ต่อ Avro binary data ไว้ท้าย

**Type:** ยังคงเป็น `[]byte` แต่มี format ที่ Confluent Schema Registry ต้องการ

---

### 6️⃣ `string()` → Convert เป็น string
**Location:** Line 232

**Input:** `[]byte`  
**Output:** `string`

```go
valueString := string(valueBytes)
// valueString = string ที่มี binary data ข้างใน
```

**สิ่งที่ทำ:**
- Convert `[]byte` → `string` เพื่อให้ส่งผ่าน common library ได้
- Common library (`sarama.StringEncoder`) จะแปลงกลับเป็น `[]byte` อีกครั้งเมื่อส่งจริง

**Type เปลี่ยนจาก:** `[]byte`  
**Type เปลี่ยนเป็น:** `string`

---

### 7️⃣ `kafkaClient.SendMessage()` → ส่งไป Kafka
**Location:** Line 236-240

**Input:** `kafka.SendMessageParam` (มี `Message: string`)  
**Output:** `kafka.SendMessageResult` (มี partition, offset)

```go
result, err := kafkaClient.SendMessage(kafka.SendMessageParam{
    Topic:   topic,        // "transactions"
    Key:     txn.ID,       // "TXN_0000001"
    Message: valueString,  // string ที่มี Confluent format
})
```

**สิ่งที่ทำ:**
- Common library จะแปลง `string` → `[]byte` อีกครั้ง
- ส่ง message ไปยัง Kafka topic
- Return partition และ offset ที่ message ถูกส่งไป

**Type:** ส่งไป Kafka เป็น `[]byte` ในรูปแบบ Confluent Schema Registry format

---

## 📊 สรุป Type Transformation Table

| Step | Function/Operation | Input Type | Output Type | Location |
|------|-------------------|------------|-------------|----------|
| 1 | Create struct | - | `Transaction` struct | Line 176 |
| 2 | `structToAvroMap()` | `Transaction` struct | `map[string]interface{}` | Line 199 |
| 3 | `validateSchema()` | `map[string]interface{}` | `map[string]interface{}` (validated) | Line 209 |
| 4 | `codec.BinaryFromNative()` | `map[string]interface{}` | `[]byte` (Avro binary) | Line 220 |
| 5 | `createConfluentFormat()` | `[]byte` (Avro) | `[]byte` (Confluent format) | Line 228 |
| 6 | `string()` | `[]byte` | `string` | Line 232 |
| 7 | `kafkaClient.SendMessage()` | `string` | Sent to Kafka (`[]byte`) | Line 236 |

---

## 🔑 Key Points

1. **Struct → Map:** ใช้ reflection + struct tags เพื่อแปลง Go struct เป็น map ที่ Avro เข้าใจ
2. **Validation:** Validate ก่อน serialize เพื่อ fail fast (producer-side)
3. **Avro Serialization:** Convert map เป็น Avro binary format
4. **Confluent Format:** เพิ่ม magic byte + schema ID เพื่อให้ Schema Registry รู้ schema
5. **String Conversion:** Convert เป็น string เพื่อให้ส่งผ่าน common library ได้
6. **Final Send:** Common library จะแปลงกลับเป็น `[]byte` และส่งไป Kafka

---

## 📝 หมายเหตุ

- **Producer-side validation:** ถ้า validation fail จะไม่ส่งไป DLQ (เพราะเป็นต้นทาง) ควรแก้ที่ source code
- **Consumer-side failures:** จะ retry แล้วส่งไป DLQ (จัดการโดย CRS service)
- **Confluent Format:** Schema ID ช่วยให้ consumer รู้ว่าต้องใช้ schema ไหนในการ deserialize


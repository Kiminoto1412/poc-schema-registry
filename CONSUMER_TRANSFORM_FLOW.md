# Flow การ Transform Data ใน Consumer (CRS Service)

## 📊 แผนภาพ Transform Type (Consumer)

```
┌─────────────────────────────────────────────────────────────┐
│  []byte (Confluent Schema Registry format)                  │
│  จาก Kafka Message                                          │
│  Format: [magic byte][schema ID][avro data]                 │
└─────────────────────────────────────────────────────────────┘
                          ↓
              extractSchemaID()
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  []byte (Avro binary format)                                │
│  Avro serialized data only                                  │
└─────────────────────────────────────────────────────────────┘
                          ↓
              codec.NativeFromBinary()
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  map[string]interface{}                                     │
│  {                                                           │
│    "id": "TXN_0000001",                                     │
│    "type": "SALE",                                          │
│    "terminal_id": int64(1),                                 │
│    "received_at": "2024-01-01 12:00:00"                    │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
              manual mapping
              (snake_case → PascalCase)
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Transaction struct{}                                       │
│  {                                                           │
│    ID: "TXN_0000001",                                       │
│    Type: "SALE",                                            │
│    TerminalID: 1,                                           │
│    ReceivedAt: "2024-01-01 12:00:00"                       │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 📋 สรุป Flow โดยย่อ

```
[]byte (Confluent Schema Registry format) - จาก Kafka
  ↓ extractSchemaID()
[]byte (Avro binary format)
  ↓ codec.NativeFromBinary()
map[string]interface{}
  ↓ manual mapping
Transaction struct{}
```

---

## 🔍 Flow แบบละเอียดทีละ Function

### 1️⃣ เริ่มต้น: รับ Message จาก Kafka
**Location:** Line 104-110 (`Consume()` function)

```go
func(msg *sarama.ConsumerMessage) error {
    // msg.Value = []byte (Confluent Schema Registry format)
    txn, err := c.deserializeMessage(msg)
}
```

**โครงสร้าง Confluent Format:**
```
[Magic Byte (1 byte: 0x00)]
[Schema ID (4 bytes: big-endian int32)]
[Avro Binary Data (variable length)]
```

**ตัวอย่างข้อมูล:**
```go
msg.Value = []byte{
    0x00,                    // Magic byte
    0x00, 0x00, 0x00, 0x01, // Schema ID = 1 (4 bytes, big-endian)
    // ... Avro binary data ...
}
```

**Type:** `[]byte` (Confluent Schema Registry format)

---

### 2️⃣ `deserializeMessage()` → Extract Schema ID และ Avro Data
**Location:** Line 153-232

**Input:** `*sarama.ConsumerMessage` (มี `Value: []byte`)  
**Output:** `Transaction` struct

#### 2.1 Extract Magic Byte และ Schema ID
**Location:** Line 155-161

```go
if len(msg.Value) < 5 {
    return Transaction{}, errors.New("message too short")
}

magicByte := msg.Value[0]
schemaID := int32(msg.Value[1])<<24 | int32(msg.Value[2])<<16 | int32(msg.Value[3])<<8 | int32(msg.Value[4])
avroData := msg.Value[5:]
```

**สิ่งที่ทำ:**
- ตรวจสอบว่า message มีความยาวอย่างน้อย 5 bytes (magic byte + schema ID)
- อ่าน magic byte (byte ที่ 0)
- อ่าน schema ID (bytes ที่ 1-4, big-endian format)
- แยก Avro binary data (bytes ที่ 5 เป็นต้นไป)

**Type เปลี่ยนจาก:** `[]byte` (Confluent format)  
**Type เปลี่ยนเป็น:** `[]byte` (Avro binary format) - เก็บใน `avroData`

---

#### 2.2 Verify Schema ID และ Fetch Schema (ถ้าจำเป็น)
**Location:** Line 167-182

```go
currentCodec := c.codec
if int(schemaID) != c.latestSchema.ID {
    // Fetch schema จาก Schema Registry
    schemaMeta, err := c.srClient.GetSchemaMetadata(c.subject, int(schemaID))
    currentCodec, err = goavro.NewCodec(schemaMeta.Schema)
}
```

**สิ่งที่ทำ:**
- ตรวจสอบว่า schema ID ตรงกับที่ cache ไว้หรือไม่
- ถ้าไม่ตรง จะ fetch schema ใหม่จาก Schema Registry
- สร้าง codec ใหม่จาก schema ที่ fetch มา

**Type:** ยังคงเป็น `[]byte` (Avro binary) แต่มี codec ที่ถูกต้องแล้ว

---

### 3️⃣ `codec.NativeFromBinary()` → Deserialize Avro Binary
**Location:** Line 185-188

**Input:** `[]byte` (Avro binary format)  
**Output:** `map[string]interface{}`

```go
native, _, err := currentCodec.NativeFromBinary(avroData)
// native = map[string]interface{}{
//     "id": "TXN_0000001",
//     "type": "SALE",
//     "terminal_id": int64(1),
//     "received_at": "2024-01-01 12:00:00"
// }
```

**สิ่งที่ทำ:**
- Deserialize Avro binary data (`[]byte`) เป็น native Go type
- ใช้ schema ที่ parse ไว้แล้ว (`codec`) เพื่อ decode ข้อมูล
- ผลลัพธ์เป็น `map[string]interface{}` ที่มี field names เป็น snake_case (ตาม Avro schema)

**Type เปลี่ยนจาก:** `[]byte` (Avro binary)  
**Type เปลี่ยนเป็น:** `map[string]interface{}`

---

### 4️⃣ Type Assertion → ตรวจสอบว่าเป็น map
**Location:** Line 191-194

```go
txnMapTyped, ok := native.(map[string]interface{})
if !ok {
    return Transaction{}, fmt.Errorf("deserialized data is not a map: %T", native)
}
```

**สิ่งที่ทำ:**
- ตรวจสอบว่า deserialized data เป็น `map[string]interface{}` หรือไม่
- ถ้าไม่ใช่จะ return error

**Type:** ยังคงเป็น `map[string]interface{}` แต่ผ่าน type assertion แล้ว

---

### 5️⃣ Manual Mapping → แปลง map เป็น Transaction struct
**Location:** Line 202-216

**Input:** `map[string]interface{}`  
**Output:** `Transaction` struct

```go
var txn Transaction
if id, ok := txnMapTyped["id"].(string); ok {
    txn.ID = id
}
if t, ok := txnMapTyped["type"].(string); ok {
    txn.Type = t
}
if tid, ok := txnMapTyped["terminal_id"].(int64); ok {
    txn.TerminalID = tid
} else if tid, ok := txnMapTyped["terminal_id"].(int32); ok {
    txn.TerminalID = int64(tid)
}
if ra, ok := txnMapTyped["received_at"].(string); ok {
    txn.ReceivedAt = ra
}
```

**สิ่งที่ทำ:**
- Map แต่ละ field จาก `map[string]interface{}` ไปยัง `Transaction` struct
- Field names ใน map เป็น snake_case (ตาม Avro schema): `id`, `type`, `terminal_id`, `received_at`
- Field names ใน struct เป็น PascalCase: `ID`, `Type`, `TerminalID`, `ReceivedAt`
- Handle type conversion (เช่น `int32` → `int64`)

**Type เปลี่ยนจาก:** `map[string]interface{}`  
**Type เปลี่ยนเป็น:** `Transaction` struct

---

### 6️⃣ Return Transaction → ส่งต่อไป Process
**Location:** Line 231

```go
return txn, nil
```

**สิ่งที่ทำ:**
- Return `Transaction` struct ที่แปลงเสร็จแล้ว
- ส่งต่อไปยัง `Process()` function เพื่อประมวลผลต่อ

**Type:** `Transaction` struct

---

## 📊 สรุป Type Transformation Table

| Step | Function/Operation | Input Type | Output Type | Location |
|------|-------------------|------------|-------------|----------|
| 1 | Receive from Kafka | Kafka Message | `[]byte` (Confluent format) | Line 104 |
| 2 | Extract schema ID | `[]byte` (Confluent) | `[]byte` (Avro binary) | Line 155-161 |
| 3 | Verify/Fetch schema | `[]byte` (Avro) | `[]byte` (Avro) + codec | Line 167-182 |
| 4 | `codec.NativeFromBinary()` | `[]byte` (Avro binary) | `map[string]interface{}` | Line 185 |
| 5 | Type assertion | `interface{}` | `map[string]interface{}` | Line 191 |
| 6 | Manual mapping | `map[string]interface{}` | `Transaction` struct | Line 202-216 |
| 7 | Return | `Transaction` struct | `Transaction` struct | Line 231 |

---

## 🔑 Key Points

1. **Confluent Format Parsing:** แยก magic byte, schema ID, และ Avro data ออกจากกัน
2. **Schema ID Verification:** ตรวจสอบ schema ID และ fetch schema ใหม่ถ้าจำเป็น
3. **Avro Deserialization:** Convert Avro binary → native Go map
4. **Manual Mapping:** Map จาก snake_case (Avro) → PascalCase (Go struct)
5. **Type Safety:** ใช้ type assertion และ type checking เพื่อความปลอดภัย

---

## 📝 หมายเหตุ

- **Schema Evolution:** Consumer สามารถรับ schema ที่แตกต่างจากที่ cache ไว้ได้ (fetch schema ใหม่จาก Schema Registry)
- **Field Name Mapping:** Avro ใช้ snake_case แต่ Go struct ใช้ PascalCase ต้อง map เอง
- **Type Conversion:** Avro อาจ return `int32` แต่ Go struct ต้องการ `int64` ต้อง convert เอง
- **Extra Fields:** Fields ที่ไม่รู้จักจะถูก ignore โดย Avro (silent data loss)

---

## 🔄 Comparison: Producer vs Consumer

| Aspect | Producer | Consumer |
|--------|----------|----------|
| **Starting Point** | `Transaction` struct | `[]byte` (Confluent format) |
| **Ending Point** | `[]byte` (Confluent format) | `Transaction` struct |
| **Struct → Map** | `structToAvroMap()` (reflection) | Manual mapping |
| **Map → Avro** | `codec.BinaryFromNative()` | `codec.NativeFromBinary()` |
| **Avro → Confluent** | `createConfluentFormat()` | Extract schema ID |
| **Schema Handling** | Register/get schema once | Verify/fetch schema per message |


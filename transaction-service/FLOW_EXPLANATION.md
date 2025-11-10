# 📋 Flow Explanation: Transaction Service Producer

## 🔄 Execution Flow

### 1️⃣ **Startup Phase** (ครั้งแรกที่รัน)

```
main()
  ↓
srclient.CreateSchemaRegistryClient()
  ↓ [สร้าง client + enable caching]
srClient.CodecCreationEnabled(true)
  ↓
ensureSchemaMatchesStruct()
  ↓
generateAvroSchema()  [ใช้ reflection อ่าน Transaction struct]
  ↓ [สร้าง schema JSON จาก struct]
  {
    "type": "record",
    "name": "Transaction",
    "fields": [
      {"name": "id", "type": "string"},
      {"name": "type", "type": "string"},
      {"name": "terminal_id", "type": "long"},
      {"name": "received_at", "type": "string"},
      {"name": "amount", "type": ["null", "double"], "default": null},
      ...
    ]
  }
  ↓
srClient.GetLatestSchema("transactions-value")
  ↓ [ตรวจสอบ cache ก่อน]
  ├─ Cache MISS → HTTP GET /subjects/transactions-value/versions/latest
  │   ↓ [เก็บใน cache]
  │   Cache: {
  │     "transactions-value": {
  │       schema: {...},
  │       codec: *goavro.Codec,
  │       id: 1,
  │       version: 1
  │     }
  │   }
  │
  └─ Cache HIT → return จาก cache (ไม่เรียก HTTP)
  ↓
srClient.IsSchemaCompatible()
  ↓ [ตรวจสอบ compatibility]
  ├─ Compatible → ใช้ schema ที่มีอยู่
  └─ Not Compatible → registerSchema()
      ↓
      srClient.CreateSchema()
      ↓ [register schema ใหม่]
      ↓ [เก็บใน cache]
      Cache: {
        "transactions-value": {
          schema: {...new schema...},
          codec: *goavro.Codec,
          id: 2,
          version: 2
        }
      }
  ↓
return schema, codec
```

### 2️⃣ **Producing Messages** (ใน loop)

```
for i := 0; i < numTransactions; i++ {
  ↓
  สร้าง Transaction struct
  ↓
  แปลงเป็น map[string]interface{}
  ↓
  codec.BinaryFromNative()  [ใช้ codec ที่ cache ไว้]
  ↓ [serialize เป็น Avro binary]
  avroBytes = [binary data]
  ↓
  createConfluentFormat(schema.ID(), avroBytes)
  ↓ [สร้าง wire format]
  [0x00][0x00 0x00 0x00 0x01][avro binary data]
   ↑        ↑                    ↑
  magic   schema ID (4 bytes)   payload
  ↓
  kafkaClient.SendMessage()
}
```

### 3️⃣ **รอบต่อไป (Restart หรือ Run อีกครั้ง)**

```
main()
  ↓
ensureSchemaMatchesStruct()
  ↓
generateAvroSchema()  [สร้าง schema จาก struct อีกครั้ง]
  ↓
srClient.GetLatestSchema("transactions-value")
  ↓ [ตรวจสอบ cache]
  ├─ Cache MISS (ถ้า restart) → HTTP GET
  │   ↓ [เก็บใน cache]
  │
  └─ Cache HIT (ถ้า run ในโปรแกรมเดียวกัน) → return จาก cache
  ↓
srClient.IsSchemaCompatible()
  ↓
  ├─ Compatible → ใช้ schema ที่มีอยู่ (ไม่ต้อง register ใหม่)
  └─ Not Compatible → registerSchema() [register version ใหม่]
```

---

## 💾 Caching Mechanism

### **srclient Cache Structure**

`srclient` มี internal cache ที่เก็บ:

```go
// Internal cache structure (simplified)
cache := map[string]*Schema{
    "transactions-value": {
        ID:      1,
        Version: 1,
        Schema:  `{"type":"record","name":"Transaction",...}`,
        Codec:   *goavro.Codec,  // ← cache codec ด้วย!
    }
}
```

### **สิ่งที่ถูก Cache:**

1. **Schema Metadata**
   - Schema ID
   - Schema Version
   - Schema JSON string
   - Subject name

2. **Codec** (ถ้า `CodecCreationEnabled(true)`)
   - `*goavro.Codec` object ที่ parse แล้ว
   - ใช้สำหรับ serialize/deserialize โดยไม่ต้อง parse schema ซ้ำ

### **Cache Lookup Flow:**

```go
// เมื่อเรียก GetLatestSchema()
func GetLatestSchema(subject string) {
    // 1. ตรวจสอบ cache ก่อน
    if cached, exists := cache[subject]; exists {
        return cached  // ← Cache HIT! ไม่ต้องเรียก HTTP
    }
    
    // 2. Cache MISS → เรียก HTTP API
    resp := http.Get("/subjects/{subject}/versions/latest")
    
    // 3. เก็บใน cache
    cache[subject] = parseResponse(resp)
    
    // 4. ถ้า CodecCreationEnabled → สร้าง codec และ cache ด้วย
    if codecEnabled {
        codec := goavro.NewCodec(schema)
        cache[subject].Codec = codec
    }
    
    return cache[subject]
}
```

---

## 📊 Cache หน้าตาเป็นยังไง?

### **ตัวอย่าง Cache Content:**

```go
// Cache entry สำหรับ "transactions-value"
{
    Subject: "transactions-value",
    ID:      1,
    Version: 1,
    Schema: `{
        "type": "record",
        "name": "Transaction",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "terminal_id", "type": "long"},
            {"name": "received_at", "type": "string"},
            {"name": "amount", "type": ["null", "double"], "default": null},
            {"name": "amount2", "type": ["null", "double"], "default": null},
            {"name": "amount3", "type": ["null", "double"], "default": null}
        ]
    }`,
    Codec: *goavro.Codec {  // ← Parsed codec object
        schema: {...},
        codec: {...}
    }
}
```

### **Cache Key:**
- Key = Subject name (เช่น `"transactions-value"`)
- Value = Schema object ที่มี schema + codec

---

## 🔁 ถ้า Produce รอบต่อไปทำอะไร?

### **Scenario 1: Run โปรแกรมใหม่ (Restart)**

```
1. main() เริ่มต้น
2. ensureSchemaMatchesStruct() ถูกเรียก
3. generateAvroSchema() สร้าง schema จาก struct
4. srClient.GetLatestSchema()
   ├─ Cache MISS (เพราะ restart) → HTTP GET
   └─ เก็บใน cache
5. ตรวจสอบ compatibility
   ├─ Compatible → ใช้ schema ที่มีอยู่
   └─ Not Compatible → registerSchema() [register version ใหม่]
6. ใช้ codec ที่ได้จาก cache
7. Produce messages (ใช้ codec เดิม)
```

### **Scenario 2: Schema เปลี่ยน (เพิ่ม field ใหม่)**

```
1. เพิ่ม field ใหม่ใน Transaction struct
   type Transaction struct {
       ...
       Amount4 float64 `avro:"amount4"`  // ← field ใหม่
   }

2. ensureSchemaMatchesStruct() ถูกเรียก
3. generateAvroSchema() สร้าง schema ใหม่ (มี amount4)
4. srClient.GetLatestSchema()
   └─ Cache HIT → return schema เก่า (ไม่มี amount4)
5. ตรวจสอบ compatibility
   └─ Not Compatible → registerSchema()
       ↓
       srClient.CreateSchema() [register version 2]
       ↓
       Cache อัปเดต: {
           "transactions-value": {
               id: 2,
               version: 2,
               schema: {...new schema with amount4...},
               codec: *goavro.Codec [new codec]
           }
       }
6. ใช้ codec ใหม่ (รองรับ amount4)
7. Produce messages (ใช้ schema version 2)
```

### **Scenario 3: Schema ไม่เปลี่ยน (Run อีกครั้ง)**

```
1. ensureSchemaMatchesStruct() ถูกเรียก
2. generateAvroSchema() สร้าง schema (เหมือนเดิม)
3. srClient.GetLatestSchema()
   └─ Cache HIT → return schema จาก cache
4. ตรวจสอบ compatibility
   └─ Compatible → ใช้ schema ที่มีอยู่
5. ใช้ codec จาก cache (ไม่ต้อง parse ใหม่)
6. Produce messages (ใช้ schema เดิม)
```

---

## ⚡ Performance Benefits

### **Cache HIT:**
- ไม่ต้องเรียก HTTP API
- ไม่ต้อง parse schema JSON
- ไม่ต้องสร้าง codec ใหม่
- **เร็วมาก!** (~0.001ms)

### **Cache MISS:**
- ต้องเรียก HTTP API (~10-50ms)
- ต้อง parse schema JSON (~1ms)
- ต้องสร้าง codec (~5ms)
- **ช้ากว่า** (~16-56ms)

### **ตัวอย่าง:**

```
Round 1 (Cache MISS):
  GetLatestSchema() → HTTP GET → Parse → Create Codec → Cache
  Time: ~50ms

Round 2 (Cache HIT):
  GetLatestSchema() → Return from Cache
  Time: ~0.001ms
  
Improvement: 50,000x faster! 🚀
```

---

## 📝 Summary

1. **Startup**: สร้าง schema จาก struct → ตรวจสอบ cache → register ถ้าจำเป็น
2. **Cache**: เก็บ schema + codec ใน memory (key = subject name)
3. **Produce**: ใช้ codec จาก cache เพื่อ serialize messages
4. **รอบต่อไป**: 
   - Cache HIT → ใช้ schema เดิม (เร็วมาก)
   - Schema เปลี่ยน → register version ใหม่ → อัปเดต cache
   - Restart → Cache MISS → ดึง schema ใหม่ → cache อีกครั้ง


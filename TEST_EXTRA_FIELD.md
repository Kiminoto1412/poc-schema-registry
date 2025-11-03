# 🧪 การทดสอบ: ส่ง Message พร้อม Field ใหม่โดยไม่ Update Schema

## 📋 สรุป

ทดสอบว่า **ถ้าเพิ่ม field ใหม่ใน message โดยไม่ update schema จะเกิดอะไรขึ้น?**

---

## 🎯 ผลลัพธ์ที่คาดหวัง

### กรณีที่ 1: ใช้ goavro.BinaryFromNative (ปัจจุบัน)
✅ **Avro จะ serialize ได้ แต่จะ IGNORE field ที่เพิ่มมา** (silently drop)

- Serialization จะสำเร็จ
- Field ที่เพิ่มมา (`Amount`) จะถูก **drop** ไป
- Consumer จะไม่ได้รับ field ที่เพิ่มมา
- **ไม่มี error** แต่ข้อมูลหายไป!

### กรณีที่ 2: ใช้ Schema Registry Client ที่ validate
❌ **Schema Registry จะ reject message** (ถ้าใช้ compatibility checking)

---

## 🔍 ผลลัพธ์จริง

### เมื่อรัน Transaction Service:

```bash
cd transaction-service
go run main.go
```

**Expected Output:**
```
🧪 Testing: Sending message with EXTRA FIELD (Amount) that is NOT in schema...
📝 Record with extra field: map[Amount:100.5 ID:EXTRA_FIELD_TEST ReceivedAt:... Type:CONTROL TerminalID:999]
✅ Avro serialization succeeded - but extra field was IGNORED!
   ℹ️  Avro silently drops fields that are not in the schema
✅ Sent message with extra field (but it was ignored): ID=EXTRA_FIELD_TEST (Partition: 0, Offset: X)
   ⚠️  Consumer will NOT receive the 'Amount' field!
```

---

## 📊 ตรวจสอบใน Consumer

เมื่อ Consumer (CRS Service) รับ message `EXTRA_FIELD_TEST`:

**Expected Output:**
```
📨 Received Transaction:
   ID: EXTRA_FIELD_TEST
   Type: CONTROL
   Terminal ID: 999
   Received At: 2025-11-03 15:XX:XX
   ---
```

**สังเกต:** ไม่มี field `Amount` เพราะถูก drop ไปแล้ว!

---

## ⚠️ ปัญหาที่อาจเกิดขึ้น

### 1. **Silent Data Loss** 🔴
- Field ที่เพิ่มมาจะหายไปโดยไม่มีการแจ้งเตือน
- อาจทำให้เกิด bug ที่ตรวจจับได้ยาก

### 2. **Schema Mismatch** 🔴
- Producer คิดว่าส่ง field ไปแล้ว แต่ Consumer ไม่ได้รับ
- อาจทำให้เกิด confusion ในการ debug

### 3. **Type Mismatch** 🔴
- ถ้า field name ตรงกันแต่ type ไม่ตรงกัน → Error!
- เช่น schema ต้องการ `string` แต่ส่ง `int` → จะ error

---

## ✅ วิธีป้องกัน

### 1. **Always Update Schema First**
```bash
# Update schema ก่อนส่ง message
curl -X POST http://localhost:8083/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":[\"null\",\"double\"],\"default\":null}]}"
  }'
```

### 2. **ใช้ Schema Registry Validation**
- ใช้ Schema Registry client ที่ validate schema ก่อนส่ง
- จะ reject message ที่ schema ไม่ match

### 3. **Schema Compatibility Mode**
- ตั้งค่า compatibility mode เป็น `BACKWARD` หรือ `FULL`
- Schema Registry จะ validate schema evolution

---

## 📝 Schema Compatibility Modes

| Mode | Description | อนุญาตอะไร |
|------|-------------|------------|
| **BACKWARD** | New schema can read old data | ✅ เพิ่ม field ใหม่ (optional) |
| **FORWARD** | Old schema can read new data | ✅ ลบ field เก่า |
| **FULL** | Both backward and forward | ✅ เพิ่ม/ลบ field ได้ |
| **NONE** | No validation | ⚠️ อนุญาตทุกอย่าง |

---

## 🧪 ทดสอบ Compatibility Mode

### ดู Compatibility Mode ปัจจุบัน:
```bash
curl -X GET http://localhost:8083/config/transactions-value
```

### ตั้งค่า Compatibility Mode:
```bash
# BACKWARD compatibility (แนะนำ)
curl -X PUT http://localhost:8083/config/transactions-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

---

## 🎯 สรุป

### ❌ **ไม่ควรทำ:**
```go
// ❌ Bad: เพิ่ม field โดยไม่ update schema
record := map[string]interface{}{
    "ID": "1001",
    "Type": "CONTROL",
    "Amount": 100.50, // ❌ Field นี้จะหายไป!
}
```

### ✅ **ควรทำ:**
```go
// ✅ Good: Update schema ก่อน แล้วค่อยเพิ่ม field
// 1. Register schema ใหม่ที่มี field "Amount"
// 2. ดึง schema ใหม่
// 3. ส่ง message พร้อม field ใหม่
```

---

## 📚 เอกสารเพิ่มเติม

- [Avro Schema Evolution](https://avro.apache.org/docs/1.11.1/specification/#schema-resolution)
- [Schema Registry Compatibility](https://docs.confluent.io/platform/current/schema-registry/avro.html#compatibility-types)
- [Schema Evolution Best Practices](https://www.confluent.io/blog/avro-schema-evolution/)

---

## 🚀 ทดสอบเลย!

1. รัน Transaction Service:
   ```bash
   cd transaction-service
   go run main.go
   ```

2. ดู logs จะเห็น:
   - ✅ Avro serialize สำเร็จ
   - ⚠️ แต่ field `Amount` ถูก ignore

3. ตรวจสอบใน Consumer:
   - Consumer จะไม่เห็น field `Amount`

4. ตรวจสอบใน Kafka UI:
   - Message จะมีแค่ 4 fields: ID, Type, TerminalID, ReceivedAt
   - ไม่มี `Amount`!

---

## ⚠️ คำเตือน

**อย่าพึ่งพา Avro silent drop!** 
- ควร update schema ก่อนส่ง field ใหม่
- ใช้ Schema Registry validation
- ตั้งค่า compatibility mode ให้เหมาะสม

**Silent failures เป็นศัตรูตัวร้าย!** 🐛


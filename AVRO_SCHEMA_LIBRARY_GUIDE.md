# Avro Schema Generator Libraries สำหรับ Go

## 1. github.com/wirelessr/avroschema (แนะนำ)

Library ที่สามารถ generate Avro schema จาก Go struct โดยอัตโนมัติ

### การติดตั้ง:
```bash
go get github.com/wirelessr/avroschema
```

### ตัวอย่างการใช้งาน:

```go
package main

import (
    "fmt"
    "github.com/wirelessr/avroschema"
)

type Transaction struct {
    ID         string `avro:"id"`
    Type       string `avro:"type"`
    TerminalID int64  `avro:"terminal_id"`
    ReceivedAt string `avro:"received_at"`
}

func main() {
    // Generate schema จาก struct โดยอัตโนมัติ
    schema, err := avroschema.Reflect(&Transaction{})
    if err != nil {
        panic(err)
    }
    
    fmt.Println(schema.String())
    // Output: Avro schema JSON string
}
```

### ข้อดี:
- ✅ ไม่ต้องเขียนโค้ด generate schema เอง
- ✅ ใช้ reflection อัตโนมัติ
- ✅ รองรับ struct tags (`avro:"field_name"`)

---

## 2. github.com/hamba/avro/v2

Library ที่มีฟีเจอร์ครบครัน แต่ต้อง parse schema JSON เอง

### การติดตั้ง:
```bash
go get github.com/hamba/avro/v2
```

### ตัวอย่างการใช้งาน:

```go
package main

import (
    "github.com/hamba/avro/v2"
)

type Transaction struct {
    ID         string `avro:"id"`
    Type       string `avro:"type"`
    TerminalID int64  `avro:"terminal_id"`
    ReceivedAt string `avro:"received_at"`
}

func main() {
    // Parse schema จาก JSON string
    schema, err := avro.Parse(`{
        "type": "record",
        "name": "Transaction",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "terminal_id", "type": "long"},
            {"name": "received_at", "type": "string"}
        ]
    }`)
    if err != nil {
        panic(err)
    }
    
    // Marshal struct เป็น Avro binary
    txn := Transaction{
        ID:         "123",
        Type:       "SALE",
        TerminalID: 1001,
        ReceivedAt: "2024-01-01T00:00:00Z",
    }
    
    data, err := avro.Marshal(schema, txn)
    if err != nil {
        panic(err)
    }
    
    // ใช้ data...
}
```

### ข้อดี:
- ✅ มี serialization/deserialization ที่ดี
- ✅ รองรับ schema evolution
- ❌ ต้องเขียน schema JSON เอง (ไม่มี auto-generation)

---

## 3. github.com/actgardner/gogen-avro

Code generator ที่ generate Go code จาก Avro schema (reverse direction)

### การติดตั้ง:
```bash
go install github.com/actgardner/gogen-avro/gogen-avro@latest
```

### การใช้งาน:
```bash
# Generate Go code จาก Avro schema file
gogen-avro --package=avro output_directory schema.avsc
```

### ข้อดี:
- ✅ Generate type-safe Go code
- ✅ Fast serialization
- ❌ ต้องมี Avro schema file ก่อน (ไม่ใช่ struct → schema)

---

## สรุปและคำแนะนำ

### สำหรับกรณีของคุณ (ต้องการ struct → schema):

**แนะนำ: `github.com/wirelessr/avroschema`**

เพราะ:
1. ✅ มี `Reflect()` function ที่ทำ struct → schema ได้โดยตรง
2. ✅ ไม่ต้องเขียนโค้ด generate schema เอง
3. ✅ ใช้งานง่าย

### ตัวอย่างการ integrate ใน crs-service:

```go
import "github.com/wirelessr/avroschema"

type Transaction struct {
    ID         string `avro:"id"`
    Type       string `avro:"type"`
    TerminalID int64  `avro:"terminal_id"`
    ReceivedAt string `avro:"received_at"`
}

// Generate schema อัตโนมัติ
func generateSchemaFromStruct() (string, error) {
    schema, err := avroschema.Reflect(&Transaction{})
    if err != nil {
        return "", err
    }
    return schema.String(), nil
}
```

---

## หมายเหตุ

- Library `github.com/wirelessr/avroschema` อาจจะไม่ค่อยมีคนใช้มากนัก
- ถ้าต้องการ library ที่ maintain ดีกว่า อาจจะต้องใช้ `hamba/avro` แต่ต้องเขียน schema JSON เอง
- หรือใช้ function `generateAvroSchema` ที่มีอยู่ใน transaction-service ก็ได้ (แต่ต้องเขียนเอง)


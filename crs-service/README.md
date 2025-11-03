# 🏦 CRS Service (Consumer)

Service ที่รับข้อมูล transaction จาก Kafka topic และ process

## 🚀 Setup

### 1. Install Dependencies

```bash
cd crs-service
go mod download
```

### 2. Build

```bash
go build -o crs-service
```

### 3. Run

```bash
./crs-service
```

หรือใช้:

```bash
go run main.go
```

## 📋 Configuration

แก้ไขค่าที่ `main.go`:

```go
bootstrapServers := "localhost:9094"  // Kafka broker
topic := "transactions"                // Topic name
groupID := "crs-service-group"         // Consumer group ID
```

## 🎯 How It Works

1. **Connect** ไปยัง Kafka broker
2. **Subscribe** ไปยัง topic `transactions`
3. **Listen** รับ messages แบบ real-time
4. **Deserialize** JSON
5. **Process** transaction (business logic)

## 📊 Sample Output

```
🚀 CRS Service (Consumer) starting...
✅ Subscribed to topic: transactions
⏳ Waiting for transactions...
📨 Received Transaction:
   ID: 1001
   Type: CONTROL
   Terminal ID: 1
   Received At: 2025-11-02 20:30:45
   ---
🔧 Processing transaction ID: 1001
✅ Transaction 1001 processed successfully
```

## 🧪 Testing

### 1. Start CRS Service

```bash
cd crs-service
go run main.go
```

### 2. Send Messages (จาก terminal อีก tab)

```bash
cd transaction-service
go run main.go
```

หรือ:

```bash
docker exec poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094
# พิมพ์: {"id":"999","type":"TEST","terminal_id":99,"received_at":"2025-11-02 12:00:00"}
```

## ⚠️ Consumer Group

Consumer group `crs-service-group`:
- **Multiple instances** ของ CRS service จะแชร์ work
- Kafka จะ distribute messages แบบ load balancing
- ถ้าต้องการ consume ทั้งหมด → ใช้ **group.id ต่างกัน**

## 🏗️ Project Structure

```
crs-service/
├── main.go           # Main application code
├── go.mod            # Go dependencies
└── README.md         # This file
```


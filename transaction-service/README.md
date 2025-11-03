# 💳 Transaction Service (Producer)

Service ที่ส่งข้อมูล transaction ไปยัง Kafka topic

## 🚀 Setup

### 1. Install Dependencies

```bash
cd transaction-service
go mod download
```

### 2. Build

```bash
go build -o transaction-service
```

### 3. Run

```bash
./transaction-service
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
```

## 🎯 How It Works

1. **Connect** ไปยัง Kafka broker
2. **Create** transactions (ตัวอย่าง: 3 transactions)
3. **Serialize** เป็น JSON
4. **Send** เข้า Kafka topic
5. **Flush** รอให้ message ส่งครบ

## 📊 Sample Output

```
🚀 Transaction Service starting...
✅ Connected to Kafka
✅ Sent transaction 1: ID=1001, Type=CONTROL, Terminal=1
✅ Sent transaction 2: ID=1002, Type=CONTROL, Terminal=2
✅ Sent transaction 3: ID=1003, Type=CONTROL, Terminal=3
✅ All transactions sent successfully!
```

## 🔍 Verify

ตรวจสอบ message ใน Kafka:

```bash
# เปิด terminal ใหม่
docker exec poc-kafka kafka-console-consumer \
  --topic transactions \
  --from-beginning \
  --bootstrap-server localhost:9094
```

หรือเปิด Kafka UI: http://localhost:8084

## 🏗️ Project Structure

```
transaction-service/
├── main.go           # Main application code
├── go.mod            # Go dependencies
└── README.md         # This file
```


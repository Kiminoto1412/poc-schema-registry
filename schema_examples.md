# Schema Registry API Examples

## Service Endpoints

- **Schema Registry**: http://localhost:8083
- **Kafka**: localhost:9094 (external), poc-kafka:29092 (internal)
- **Kafka UI**: http://localhost:8084

## 1. Register Transaction Schema

### JSON Schema (Avro)

```bash
curl -X POST http://localhost:8083/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":{\"type\":\"string\",\"logicalType\":\"timestamp-millis\"}}]}"
  }'
```

**Response:**
```json
{
  "id": 1,
  "subject": "transactions-value",
  "version": 1,
  "schema": "..."
}
```

---

## 2. Get Latest Schema

```bash
curl -X GET http://localhost:8083/subjects/transactions-value/versions/latest
```

**Response:**
```json
{
  "subject": "transactions-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":{\"type\":\"string\",\"logicalType\":\"timestamp-millis\"}}]}"
}
```

---

## 3. List All Subjects

```bash
curl -X GET http://localhost:8083/subjects
```

**Response:**
```json
["transactions-value"]
```

---

## 4. Get All Versions

```bash
curl -X GET http://localhost:8083/subjects/transactions-value/versions
```

**Response:**
```json
[1]
```

---

## 5. Check Schema Compatibility

```bash
curl -X POST http://localhost:8083/compatibility/subjects/transactions-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"local_db\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"terminal_id\",\"type\":\"long\"},{\"name\":\"received_at\",\"type\":\"string\"}]}"
  }'
```

**Response:**
```json
{
  "is_compatible": true
}
```

---

## 6. Get Schema by ID

```bash
curl -X GET http://localhost:8083/schemas/ids/1
```

---

## 7. Create Topic and Test

### Create Topic
```bash
docker exec poc-kafka kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9094 \
  --partitions 1 \
  --replication-factor 1
```

### List Topics
```bash
docker exec poc-kafka kafka-topics --list \
  --bootstrap-server localhost:9094
```

### Produce Message (with schema)
```bash
# You'll need a producer that uses the registered schema
# Or use kafka-console-producer for simple text
docker exec poc-kafka kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9094
```

### Consume Messages
```bash
docker exec poc-kafka kafka-console-consumer \
  --topic transactions \
  --from-beginning \
  --bootstrap-server localhost:9094
```

---

## Compatibility Levels

Valid compatibility levels:
- `NONE`: No compatibility checking (default)
- `BACKWARD`: New schema can read old data
- `BACKWARD_TRANSITIVE`: Backward + transitive
- `FORWARD`: Old schema can read new data
- `FORWARD_TRANSITIVE`: Forward + transitive
- `FULL`: Both backward and forward
- `FULL_TRANSITIVE`: Full + transitive

### Update Compatibility Level
```bash
curl -X PUT http://localhost:8083/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibilityLevel": "BACKWARD"}'
```

---

## Transaction Table Schema Mapping

### MySQL Table:
```sql
CREATE TABLE `transactions` (
  `id` varchar(255) NOT NULL,
  `type` varchar(40) NOT NULL,
  `terminal_id` bigint NOT NULL,
  `received_at` datetime NOT NULL
);
```

### Avro Schema:
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

---

## Using with Python (confluent-kafka)

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import Producer

# Schema Registry config
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

# Create serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer config
producer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

# Create producer and send
producer = Producer(producer_conf)
transaction_data = {
    'id': '1001',
    'type': 'CONTROL',
    'terminal_id': 1,
    'received_at': '2025-09-02T07:55:20'
}

producer.produce('transactions', value=transaction_data)
producer.flush()
```


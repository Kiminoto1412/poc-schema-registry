# 🧩 Kafka + Schema Registry (Confluent-Compatible) Setup

This repository provides a **ready-to-use Docker Compose setup** for running
Kafka, Zookeeper, and Confluent Schema Registry locally.  
You can use it to **register, validate, and manage Avro/JSON/Protobuf schemas**
for microservices that communicate through Kafka.

---

## 📦 Components

| Service | Image | Port | Description |
|----------|--------|------|-------------|
| **Zookeeper** | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Keeps metadata and coordinates Kafka brokers |
| **Kafka Broker** | `confluentinc/cp-kafka:7.6.0` | 9092 | The Kafka message broker |
| **Schema Registry** | `confluentinc/cp-schema-registry:7.6.0` | 8081 | REST service for registering and validating schemas |

---

## 🚀 Getting Started

### 1. Start Services

```bash
docker compose up -d
```

This command will start:

- Zookeeper
- Kafka broker
- Schema Registry (HTTP endpoint available at `http://localhost:8081`)

Check containers:
```bash
docker ps
```

---

### 2. Verify Schema Registry is Running

```bash
curl http://localhost:8081/subjects
```

Expected response:
```json
[]
```

This means the service is up and ready ✅

---

## 🧠 Registering Your First Schema

Example Avro schema (`UserCreated`):

```json
{
  "type": "record",
  "name": "UserCreated",
  "namespace": "com.auct.user",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "name", "type": "string" },
    { "name": "email", "type": ["null", "string"], "default": null }
  ]
}
```

Register the schema via REST API:

```bash
curl -X POST http://localhost:8081/subjects/user-created-value/versions   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{
    "schema": "{
      \"type\": \"record\",
      \"name\": \"UserCreated\",
      \"namespace\": \"com.auct.user\",
      \"fields\": [
        {\"name\": \"id\", \"type\": \"string\"},
        {\"name\": \"name\", \"type\": \"string\"},
        {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null}
      ]
    }"
  }'
```

Response:
```json
{ "id": 1 }
```

Now your schema is registered with **ID = 1** 🎉

---

## 🔍 Checking Schemas

| Action | HTTP Request | Example |
|--------|---------------|----------|
| Get all subjects | `GET /subjects` | `curl http://localhost:8081/subjects` |
| Get versions of a subject | `GET /subjects/{subject}/versions` | `curl http://localhost:8081/subjects/user-created-value/versions` |
| Get latest version | `GET /subjects/{subject}/versions/latest` | `curl http://localhost:8081/subjects/user-created-value/versions/latest` |
| Get schema by ID | `GET /schemas/ids/{id}` | `curl http://localhost:8081/schemas/ids/1` |
| Delete subject | `DELETE /subjects/{subject}` | `curl -X DELETE http://localhost:8081/subjects/user-created-value` |

---

## ⚙️ Compatibility Modes

Schema Registry enforces schema evolution rules to prevent breaking changes.

| Mode | Description |
|------|--------------|
| `BACKWARD` | New schema must be able to read old data ✅ (recommended) |
| `FORWARD` | Old schema must be able to read new data |
| `FULL` | Both backward and forward compatible |
| `NONE` | Disable checks (not recommended) |

Set mode per subject:
```bash
curl -X PUT http://localhost:8081/config/user-created-value   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{"compatibility": "BACKWARD"}'
```

Or globally:
```bash
curl -X PUT http://localhost:8081/config   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{"compatibility": "BACKWARD"}'
```

---

## 🧩 Example: Check Compatibility

You can validate if a new schema is compatible with the latest version:

```bash
curl -X POST http://localhost:8081/compatibility/subjects/user-created-value/versions/latest   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{
    "schema": "{\"type\":\"record\",\"name\":\"UserCreated\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}"
  }'
```

Response:
```json
{"is_compatible": true}
```

---

## 🧰 Wire Format (For Custom Clients)

When producing messages manually (without a Confluent client library),  
each Kafka message’s value should follow this **Schema Registry wire format:**

```
[ magic byte (1B, always 0) ][ schema id (4B, big-endian) ][ serialized payload... ]
```

- Magic byte = 0
- Schema ID = the integer returned when you registered
- Payload = binary Avro/JSON data

---

## 🧑‍💻 Recommended Dev Workflow

1. Create a central repo (e.g. `schema-registry/` or `kafka-schemas/`)
2. Organize schemas by domain:  
   ```
   auction/
   member/
   payment/
   ```
3. In CI/CD:
   - Validate new schema (e.g., using `avro-tools` or `confluent schema` CLI)
   - Check compatibility with the registry
   - If valid → auto-register to DEV schema registry
4. Services should import schemas from a **shared module or generated code**
   (don’t hardcode structs manually)
5. Use `BACKWARD` compatibility mode by default
6. Separate Schema Registry instances per environment (DEV / UAT / PROD)

---

## 🧩 Docker Compose Reference

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "broker:9092"
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

---

## 🧭 Useful Links

- [Confluent Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [riferrei/srclient (Go Client)](https://github.com/riferrei/srclient)
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

---

## 🧾 License

MIT License © 2025 — AUCT Dev Team

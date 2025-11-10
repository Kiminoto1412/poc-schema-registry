# Transaction Flow Diagram

```mermaid
sequenceDiagram
    participant TxService as Transaction Service
    participant SchemaReg as Schema Registry
    participant Kafka as Kafka Broker
    participant CRS as CRS Consumer
    participant DLQ as DLQ Topic

    Note over TxService,SchemaReg: Startup Phase
    TxService->>SchemaReg: GET /subjects/transactions-value/versions/latest
    SchemaReg-->>TxService: return schema (ID: 101)
    
    CRS->>SchemaReg: GET /subjects/transactions-value/versions/latest
    SchemaReg-->>CRS: return schema (ID: 101)
    CRS->>SchemaReg: GET /subjects/transactions-dlq-value/versions/latest
    SchemaReg-->>CRS: return DLQ schema (ID: 102)

    Note over TxService,Kafka: Message Production
    TxService->>Kafka: Produce message<br/>(topic: transactions, schema_id=101)

    Note over Kafka,DLQ: Message Consumption
    Kafka->>CRS: Push message
    
    alt Schema ID differs from cached
        CRS->>SchemaReg: GET /subjects/transactions-value/versions/{schemaID}
        SchemaReg-->>CRS: return schema
    end
    
    alt Processing succeeds
        CRS->>CRS: Process transaction
    else Processing fails (after retries)
        CRS->>DLQ: Send to DLQ<br/>(topic: transactions-dlq)
    end
```

## Key Differences from Original Diagram

1. **No HTTP endpoint**: Transaction Service generates transactions in a loop, not via POST /transactions
2. **No Transaction DB**: Transactions are generated programmatically
3. **Only CRS Consumer**: No GL or GOLD consumers exist in the codebase
4. **Topic name**: `transactions` (not `transaction-events`)
5. **Subject name**: `transactions-value` (not `transaction-event-value`)
6. **Schema fetching**: Consumer fetches schema by ID only when schema ID doesn't match cached schema
7. **DLQ handling**: Failed messages are sent to `transactions-dlq` topic using Avro format with a separate DLQ schema
8. **Startup phase**: Both producer and consumer fetch schemas during startup and cache them


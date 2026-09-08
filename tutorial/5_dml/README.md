# Tutorial 5: Data manipulation (DML)

Demonstrates writing and modifying data with `MilvusClientV2`: insert rows, upsert to replace or
partially update an entity, delete by primary key or by filter, and verify the remaining rows with
`query`.

## What you learn

1. Create and load a collection with a vector index.
2. Insert row-oriented JSON entities with `insert`.
3. Replace or insert entities with `upsert`.
4. Partially update an entity with `upsert` plus `partialUpdate(true)`.
5. Delete entities by primary key or by filter expression with `delete`.
6. Read the remaining rows back with `query`.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.dml.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.dml.App"
```

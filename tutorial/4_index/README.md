# Tutorial 4: Index

Demonstrates the index lifecycle with `MilvusClientV2`: create a collection without indexes, build
one vector index (HNSW) and two scalar indexes with `createIndex`, list them, inspect one with
`describeIndex`, and remove one with `dropIndex`.

## What you learn

1. Create a collection without supplying indexes at creation time.
2. Build multiple indexes in one `createIndex` call with `IndexParam` entries.
3. Configure an HNSW index with metric type and build parameters.
4. List index names with `listIndexes`.
5. Inspect index metadata such as type, metric, and build state with `describeIndex`.
6. Remove a single index with `dropIndex` without affecting the other indexes.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.index.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.index.App"
```

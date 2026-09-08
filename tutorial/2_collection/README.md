# Tutorial 2: Collection management

Walks through the full collection lifecycle with `MilvusClientV2`: create a collection with a
description and a vector index, inspect it, change its properties, load and release it, read its
load state and row count, rename it, truncate it, and finally drop it.

## What you learn

1. Create a collection with a schema, a vector index, and a default consistency level.
2. Check existence with `hasCollection` and inspect metadata with `describeCollection`.
3. Add and remove collection properties such as TTL with
   `alterCollectionProperties` / `dropCollectionProperties`.
4. Load and release a collection and read its load state and row count.
5. Rename and truncate a collection.
6. Drop the collection during cleanup.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.collection.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.collection.App"
```

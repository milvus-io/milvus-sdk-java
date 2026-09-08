# Tutorial 1: Quick start

The smallest complete `MilvusClientV2` application. It connects to Milvus, creates a collection
with an integer primary key, a text field, and a float vector, inserts two rows, performs a
vector search, and removes the collection.

## What you learn

1. Create and connect a `MilvusClientV2` instance.
2. Define a collection schema with a primary key, a scalar field, and a vector field.
3. Insert row-oriented data.
4. Search for the nearest neighbors of a query vector.
5. Drop the collection during cleanup.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.quickstart.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.quickstart.App"
```

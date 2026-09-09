# Tutorial 7: Database

Walks through logical database administration with `MilvusClientV2`: list existing databases, create
a database, inspect it with `describeDatabase`, set and remove a property, switch the client's
selected database with `useDatabase`, and finally drop the tutorial database.

## What you learn

1. List databases with `listDatabases`.
2. Create a logical database with `createDatabase`.
3. Inspect database metadata with `describeDatabase`.
4. Add and remove database properties such as the default replica number.
5. Switch the database used by requests that omit a database name with `useDatabase`.
6. Drop the database during cleanup.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.database.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.database.App"
```

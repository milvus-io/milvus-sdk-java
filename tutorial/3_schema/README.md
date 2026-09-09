# Tutorial 3: Schema

Shows the field data types that a collection schema can declare. Three collections are created and
read back with `describeCollection` so the server's representation of each field is printed:
scalar and container types, vector types, and sparse / struct types.

## What you learn

1. Build a `CreateCollectionReq.CollectionSchema` with `AddFieldReq` entries.
2. Declare scalar fields: `Bool`, `Int8`, `Int32`, `Int64`, `Float`, `Double`, `VarChar`,
   `JSON`, `Timestamptz`.
3. Declare nullable and default-valued fields.
4. Declare vector fields: `FloatVector`, `BinaryVector`, `Float16Vector`, `BFloat16Vector`,
   `SparseFloatVector`, `Int8Vector`.
5. Declare an array field with an element type and capacity.
6. Declare a struct field (array of struct) with nested sub-fields.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.schema.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.schema.App"
```

# Milvus Java SDK Examples

This is a standalone Maven project containing example code for the Milvus Java SDK. It is **not** part of the main SDK build — it has its own `pom.xml` and runs independently.

## Prerequisites

- Java 8 or higher
- Apache Maven
- A running Milvus instance (default: `localhost:19530`)

## Build

```shell
cd examples
mvn compile
```

## Run an example

```shell
mvn exec:java -Dexec.mainClass="io.milvus.v2.SimpleExample"
```

## Project structure

- `src/main/java/io/milvus/v2/` — Examples using the V2 SDK (`MilvusClientV2`)
- `src/main/java/io/milvus/v1/` — Examples using the V1 SDK (`MilvusServiceClient`)
- `src/main/java/io/milvus/v2/bulkwriter/` — BulkWriter examples

## Notes

- This project depends on `milvus-sdk-java` and `milvus-sdk-java-bulkwriter` from your local Maven repository. If you've made local changes to the SDK, run `mvn install -DskipTests` from the project root first.
- Some examples require additional services (e.g., MinIO for BulkWriter, external table examples). Check the comments at the top of each example file for specific requirements.

# Milvus Java SDK tutorials

This directory contains beginner-oriented tutorials for the Milvus Java SDK. Each tutorial is an
independent Maven project that depends on the **published** `milvus-sdk-java` artifact from Maven
Central, rather than on this repository's source tree. This keeps the tutorials close to the
experience of an application developer installing the SDK.

## Prerequisites

- Java 8 or higher
- Apache Maven
- A running Milvus server

## Run a tutorial

From the repository root:

```bash
cd tutorial/1_quickstart
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.quickstart.App"
```

Each tutorial expects Milvus at `http://localhost:19530` with the default `root:Milvus` credentials.
To point at a different server, set the `MILVUS_URI` and `MILVUS_TOKEN` environment variables:

```bash
MILVUS_URI="https://your-milvus-endpoint" \
MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.quickstart.App"
```

## Start here

If this is your first Milvus Java SDK program, start with the quickstart:

```bash
cd tutorial/1_quickstart
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.quickstart.App"
```

## Beginner tutorials

- [`1_quickstart`](1_quickstart/): connect, create a collection, insert data, search, and clean up.
- [`2_collection`](2_collection/): manage the collection lifecycle.
- [`3_schema`](3_schema/): define collection fields for the supported V2 data types.
- [`4_index`](4_index/): create and inspect vector and scalar indexes.
- [`5_dml`](5_dml/): insert, upsert, and delete collection data.
- [`6_dql`](6_dql/): query, search, hybrid search, and iterate through results.

## Advanced tutorials

- [`7_database`](7_database/): create, configure, select, and remove databases.
- [`8_rbac`](8_rbac/): manage users, roles, privilege groups, and grants.

## SDK version

The `milvus-sdk-java` version used by all tutorials is declared once in the parent
[`pom.xml`](pom.xml) (`milvus.sdk.version`). When a new SDK version is released, update it there so
the tutorials keep consuming the latest published artifact.

The source-linked examples used for SDK development and testing remain under
[`examples/`](../examples/README.md).

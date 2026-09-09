# Tutorial 6: Data query language (DQL)

Demonstrates reading data with `MilvusClientV2`: scalar filtering with `query`, dense
nearest-neighbor search with `search`, combined dense + sparse retrieval with `hybridSearch`, and
paged reads with `queryIterator` and `searchIterator`.

## What you learn

1. Create and load a collection with a dense index and a sparse inverted index.
2. Filter rows with `query` and read the returned entities.
3. Search nearest neighbors with `search`, optionally restricted by a filter.
4. Combine dense and sparse sub-searches with `hybridSearch` and a weighted reranker.
5. Page through query results with `queryIterator` (`batchSize` / `limit`).
6. Page through search results with `searchIterator`, preserving one search session.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.dql.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.dql.App"
```

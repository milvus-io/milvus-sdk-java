# Milvus Java SDK Performance Benchmark

This project contains performance benchmarks for the Milvus Java SDK.

## Prerequisites

- Java 8+
- Maven 3.x (must be available in `PATH`)
- A running Milvus instance (default: `localhost:19530`)

## Quick Start

Run all benchmarks for SDK v2.6.1 and v2.6.14:

```bash
cd benchmark
./run_benchmarks.sh
```

Results are written to the `results/` directory as timestamped markdown files (e.g., `SearchBenchmark_2.6.14_20260303_101530.md`).

## Run All Benchmarks

The `run_benchmarks.sh` script runs all benchmark cases across multiple SDK versions.

```bash
# Default: SDK 2.6.1 and 2.6.14, localhost:19530
./run_benchmarks.sh

# Single SDK version
./run_benchmarks.sh -v 2.6.14

# Multiple SDK versions (comma-separated)
./run_benchmarks.sh -v 2.6.1,2.6.13,2.6.14

# With a custom Milvus URI
./run_benchmarks.sh -v 2.6.14 -u http://myhost:19530

# With URI and token
./run_benchmarks.sh -v 2.6.1,2.6.14 -u http://myhost:19530 -t root:Milvus

# Show help
./run_benchmarks.sh -h
```

| Option | Description | Default |
|--------|-------------|---------|
| `-v, --versions` | Comma-separated SDK versions | `2.6.1,2.6.14` |
| `-u, --uri` | Milvus server URI | `http://localhost:19530` |
| `-t, --token` | Authentication token | `root:Milvus` |
| `-h, --help` | Show help message | - |

The script performs a `mvn clean compile` before each benchmark to avoid class incompatibility between SDK versions.

## Run Individual Benchmarks

Both benchmarks accept two optional arguments via `-Dexec.args`:

| Argument | Position | Default | Description |
|----------|----------|---------|-------------|
| URI | 1st | `http://localhost:19530` | Milvus server address |
| Token | 2nd | `root:Milvus` | Authentication token |

The SDK version is controlled by the `revision` property (default: `2.6.14`). Override it with `-Drevision` to benchmark any published version. Use `clean` to ensure the previous version's classes are cleared before compiling against a different SDK version.

---

### PoolBenchmark

Benchmarks `MilvusClientV2Pool` (pool) vs a single shared `MilvusClientV2` (no-pool) for concurrent **search**, **query**, and **insert** operations.

```bash
cd benchmark
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.PoolBenchmark"

# With a custom Milvus URI and token
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.PoolBenchmark" \
    -Dexec.args="http://myhost:19530 root:Milvus"

# With a specific SDK version
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.PoolBenchmark" \
    -Drevision=2.6.13
```

**What It Does:**

1. **Setup** -- Creates a collection with a FLAT index, inserts 100,000 base rows
2. **No-Pool benchmark** -- A single shared `MilvusClientV2` used by 32 concurrent threads
3. **Pool benchmark** -- A `MilvusClientV2Pool` (maxTotalPerKey=20) with get/return per request
4. **Repeat** -- Alternates No-Pool and Pool benchmarks for 10 rounds
5. **Summary** -- Prints results split by operation (Search, Query, Insert), each comparing No-Pool vs Pool

---

### SearchBenchmark

Benchmarks **search** operations on a complex collection with multiple vector types and scalar fields. Measures how topK and output fields affect search latency across 4 vector field types.

```bash
cd benchmark
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.SearchBenchmark"

# With a custom Milvus URI and token
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.SearchBenchmark" \
    -Dexec.args="http://myhost:19530 root:Milvus"

# With a specific SDK version
mvn clean compile exec:java -Dexec.mainClass="io.milvus.benchmark.SearchBenchmark" \
    -Drevision=2.6.13
```

**Collection Schema:**

| Field | Type | Details |
|-------|------|---------|
| `id` | Int64 | Primary key (user-provided) |
| `float_vector` | FloatVector | dim=768, FLAT/COSINE |
| `binary_vector` | BinaryVector | dim=2048, BIN_FLAT/HAMMING |
| `float16_vector` | Float16Vector | dim=768, FLAT/COSINE |
| `sparse_vector` | SparseFloatVector | SPARSE_INVERTED_INDEX/BM25 (auto-generated) |
| `text` | VarChar | maxLength=65535, analyzer enabled |
| `array_double` | Array(Double) | maxCapacity=100 |
| `json` | JSON | Structured data |

**Function:** BM25 (`text` -> `sparse_vector`)

**What It Does:**

1. **Setup** -- Creates the collection with indexes, inserts 100,000 rows
2. **Query benchmark** -- Retrieves 10 entities to extract vectors and texts for search
3. **Search benchmark** -- Tests 4 vector fields (float_vector, binary_vector, float16_vector, sparse_vector), each with 4 variants:

| Variant | topK | Output Fields |
|---------|------|---------------|
| A | 10 | none |
| B | 10 | all fields |
| C | 1000 | none |
| D | 1000 | all fields |

4. **Summary** -- Prints results split by vector field, each variant runs 10 iterations

## Result Files

Each benchmark writes a timestamped markdown file to `results/` containing:

- **Date**, **SDK Version**, **URI**, **Rows**, **Repeats**
- **Collection Schema**
- **Benchmark results** in markdown tables

The `results/` directory is git-ignored.

## Configuration Files

Benchmark parameters can be customized via JSON config files. Each benchmark automatically looks for `config/<ClassName>.json` (e.g., `config/PoolBenchmark.json` for `PoolBenchmark`). If the file is not found, hardcoded defaults are used. All fields are optional — missing keys keep the defaults.

### `config/PoolBenchmark.json`

```json
{
  "vectorDim": 128,
  "rowCount": 100000,
  "threadCount": 32,
  "requestCount": 10000,
  "topK": 10,
  "rounds": 10,
  "maxIdlePerKey": 10,
  "maxTotalPerKey": 20,
  "maxTotal": 100,
  "maxBlockWaitSeconds": 30
}
```

| Key | Default | Description |
|-----|---------|-------------|
| `vectorDim` | 128 | Vector dimension |
| `rowCount` | 100000 | Number of base rows to insert |
| `threadCount` | 32 | Number of concurrent threads |
| `requestCount` | 10000 | Number of requests per benchmark |
| `topK` | 10 | Search top-K parameter |
| `rounds` | 10 | Number of benchmark rounds |
| `maxIdlePerKey` | 10 | Pool: max idle clients per key |
| `maxTotalPerKey` | 20 | Pool: max total clients per key |
| `maxTotal` | 100 | Pool: max total clients across all keys |
| `maxBlockWaitSeconds` | 30 | Pool: max seconds to wait when pool is exhausted |

### `config/SearchBenchmark.json`

```json
{
  "floatVectorDim": 768,
  "binaryVectorDim": 2048,
  "float16VectorDim": 768,
  "rowCount": 100000,
  "repeat": 10
}
```

| Key | Default | Description |
|-----|---------|-------------|
| `floatVectorDim` | 768 | Float vector dimension |
| `binaryVectorDim` | 2048 | Binary vector dimension |
| `float16VectorDim` | 768 | Float16 vector dimension |
| `rowCount` | 100000 | Number of rows to insert |
| `repeat` | 10 | Number of iterations per search variant |

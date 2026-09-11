# AGENTS.md

Repository guidance for AI agents and contributors working in `milvus-sdk-java`.

## Repository layout

The repo builds two Maven artifacts plus standalone projects:

| Module | Artifact | Status | Location |
|---|---|---|---|
| sdk-core | `milvus-sdk-java` | Current development | `sdk-core/` |
| sdk-bulkwriter | `milvus-sdk-java-bulkwriter` | Current development | `sdk-bulkwriter/` |
| examples | standalone Maven project | docs/examples | `examples/` |
| benchmark | standalone Maven project | bench | `benchmark/` |
| tests | standalone legacy/TestNG projects | not in reactor | `tests/` |

Only `sdk-core` and `sdk-bulkwriter` are root reactor modules. Since v2.5.2, BulkWriter ships as a
separate artifact because it pulls in heavy Hadoop/Parquet/MinIO dependencies.

## Examples

`examples/` is a standalone Maven project showing SDK usage for both API generations. It is **not** part
of the main reactor build and runs independently:

```text
examples/
├── pom.xml                          # standalone; depends on local milvus-sdk-java + bulkwriter
└── src/main/java/io/milvus/
    ├── v1/                          # V1 SDK (MilvusServiceClient) examples
    ├── v2/                          # V2 SDK (MilvusClientV2) examples
    └── v2/bulkwriter/               # BulkWriter examples
```

Build and run (requires a Milvus at `localhost:19530`, Java 8+):

```bash
cd examples
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.v2.SimpleExample"
```

- The examples project depends on `milvus-sdk-java` and `milvus-sdk-java-bulkwriter` from the local Maven
  repository. After local SDK changes, run `mvn install -Dmaven.test.skip=true` at the repo root first.
- ~19 V1 and ~44 V2 example classes cover the common API surface; BulkWriter examples live under
  `v2/bulkwriter/`. Some examples need extra services (MinIO for BulkWriter, external tables) — check the
  header comment of each class for its prerequisites.
- Prefer `io.milvus.v2.*` for new example code, matching the V2-first development policy below.

## Tutorials

`tutorial/` is a set of beginner-oriented, standalone Maven projects for learning the SDK, mirroring the
`tutorial/` layout of the C++ and Rust SDKs. Unlike `examples/`, each tutorial depends on the **published**
`milvus-sdk-java` artifact (from Maven Central), not the local source tree, so the learner's experience
matches an application developer installing the SDK.

```text
tutorial/
├── pom.xml                    # parent POM; single source of the milvus-sdk-java version
├── 1_quickstart/
├── 2_collection/
├── 3_schema/
├── 4_index/
├── 5_dml/
├── 6_dql/
├── 7_database/
└── 8_rbac/
```

- `tutorial/pom.xml` is the aggregator/parent POM. The `milvus-sdk-java` version is declared once there
  (e.g. `<milvus.sdk.version>3.0.9</milvus.sdk.version>` in `<dependencyManagement>`) and inherited by the
  eight child modules — it must **not** be duplicated in each child `pom.xml`.
- **When releasing a new SDK version, update the `milvus-sdk-java` version in `tutorial/pom.xml`
  (`milvus.sdk.version`) so tutorials keep consuming the latest published release.**
- Each child module is runnable standalone: `cd tutorial/1_quickstart && mvn exec:java`.
- Connection settings use `MILVUS_URI` / `MILVUS_TOKEN` env vars, defaulting to
  `http://localhost:19530` / `root:Milvus` (same convention as the C++/Rust SDK tutorials).
- The source-linked examples used for SDK development and testing remain under `examples/`.

## SDK generations

Two API generations coexist inside `sdk-core`:

- **V1 (legacy, maintenance mode)** — `client/`, `param/`, `response/`, `connection/`, `exception/`.
  Entry point `io.milvus.client.MilvusServiceClient` (and `MilvusMultiServiceClient`), request params in
  `param/*`, response wrappers in `response/`. V1 is kept only for backward compatibility: it receives
  bug fixes and critical maintenance, but **no new features are added to V1**. New capabilities are
  implemented on V2 only.
- **V2 (active, long-term maintained)** — `io.milvus.v2/`. Entry point `io.milvus.v2.client.MilvusClientV2`
  (+ `MilvusClientV2Session`). Request/response DTO pattern like the C++/Rust/C# V2 SDKs: each operation is a
  `*Req` / `*Resp`, with conversions in `v2/service/<domain>/` and shared helpers in `v2/utils/`.
  `v2/client/globalcluster/` implements global-cluster topology discovery and primary swap. All new
  features, API additions, and pymilvus parity work target V2.

Cross-cutting shared code: `common/utils/cache/` (SchemaCache, CollectionTsCache), `orm/iterator/`
(V1/V2 query/search iterators), `pool/` (client pools), `common/interceptor/` (gRPC interceptors),
`common/utils/` (JSON, Float16, redaction, bloom/roaring bitmaps), `telemetry/` (client telemetry).

## Protos

- `sdk-core/src/main/milvus-proto/` is a git submodule holding `proto/*.proto`.
- Generated Java lives in `sdk-core/target/generated-sources/protobuf/{java,grpc-java}` and is produced at
  build time by `protobuf-maven-plugin`. Do not edit generated files.
- Generated classes land in `io.milvus.grpc` and `milvus.proto.*`; these are excluded from JaCoCo coverage.

## Building

```bash
git submodule update --init          # proto submodule
mvn install                          # builds sdk-core + sdk-bulkwriter
```

- Published artifacts and test sources target Java 8 (`maven.compiler.source/target=8`); `mvn test`
  works with JDK 8 or later. Use JDK 11+ only if you need extra toolchain features.
- Use `-Dmaven.test.skip=true` to skip tests entirely; `-DskipTests` is unreliable here because root
  Surefire sets `skipTests=false`.
- `protobuf-maven-plugin` generates gRPC stubs; `maven-shade-plugin` relocates `io.grpc` to
  `io.milvus.shaded.io.grpc` in the packaged jar.

## Testing

Since the sdk-core test reorganization on `master` (#2070) the test tree is split into three layers
under `sdk-core/src/test/java/io/milvus/`, tagged with JUnit 5 `@Tag`:

| Layer | Tag | Depends on | Examples |
|---|---|---|---|
| Unit | `@Tag("unit")` | none | request/response DTO builders, utils, caches, enums, interceptors |
| Integration | `@Tag("integration")` | mock gRPC stub (`support/v2/BaseTest` for V2, in-process `MockMilvusServer` for V1) | facade method forwarding, error mapping, ts/schema caches |
| System | `@Tag("system")` | real Milvus standalone via Docker | end-to-end |

Before the v3.0.9-era rework the tests lived in a single tree mirroring the `io.milvus.*` production
packages (`v2/`, `client/`, `param/`, ...) with no type separation and no tag-driven
filtering. The rework instead groups tests by layer (`unit/`, `integration/`,
`system/`), then by SDK generation (`v1/`, `v2/`, `common/`), and makes each layer
selectable via JUnit tags.

Run a layer with:

```bash
mvn -pl sdk-core test                # unit only (default)
mvn -pl sdk-core -Pintegration test  # unit + integration
mvn -pl sdk-core -Psystem test       # unit + integration + system (Docker)
```

- Test source layout groups by type then generation: `unit/`, `integration/`, `system/`, each with
  `v1/`, `v2/`, `common/`. Shared fixtures live in `support/` (`TestUtils`, `BaseTest`, `MockMilvusServer`,
  Docker base classes). A few white-box unit tests that need package-private access stay in their production
  package (e.g. `telemetry/`, `v2/client/globalcluster/`, `common/utils/BloomFilterUtilsTest`).
- Integration V2 tests extend `BaseTest`, which injects a mocked `MilvusServiceBlockingStub` into a
  `MilvusClientV2`. Integration tests cover every public `MilvusClientV2` method.
- System tests share one Milvus standalone per JVM via an idempotent `TestUtils.startMilvusStandalone` and a
  JVM shutdown hook; `*DockerTest` classes extend `MilvusV2DockerTestBase` / `MilvusV1DockerTestBase` /
  `MilvusMultiDockerTestBase`. Docker compose files live in `sdk-core/src/test/resources/docker/`.
- sdk-bulkwriter tests are tagged `@Tag("unit")` and run with `mvn -pl sdk-bulkwriter test`.

## CI and coverage

- `.github/workflows/maven.yml` splits into two jobs:
  - `Build and test on ubuntu` (`ubuntu-latest`): bulkwriter unit tests + system tests (`-Psystem`).
  - `Build and test on windows` (`windows-2022`): tutorials compile + sdk-core unit + integration
    tests (`-Pintegration`).
- Coverage is collected with JaCoCo (`-Pcoverage`); each step writes a per-step exec/report via
  `-Djacoco.destFile` / `-Djacoco.reportDir`, and codecov uploads the ut/it/st reports.
- The JaCoCo report excludes proto-generated packages (`io/milvus/grpc/**`, `milvus/proto/**`) so codecov
  reflects SDK code only.
- `.github/mergify.yml` labels `ci-passed` when both `Build and test on ubuntu` and
  `Build and test on windows` succeed; the PR must pass DCO.
- `.github/workflows/publish-release.yaml` is the tag-driven release flow (Temurin 8, signs and publishes to
  Maven Central, then creates the GitHub release).

## Git / PR conventions

- The PR branch must contain exactly one commit; the commit message must carry a `Signed-off-by` trailer
  (DCO check). Squash new work into the single PR commit and force-push with
  `git commit --amend -s` / `git push --force-with-lease`.
- Primary remote for upstream is `source` (`milvus-io/milvus-sdk-java`); `origin` is the contributor fork.

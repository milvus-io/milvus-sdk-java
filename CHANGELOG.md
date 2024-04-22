# Changelog
## milvus-sdk-java 2.4.0 (2024-04-22)

### Feature

- Support new index type INVERTED/GPU_CAGRA
- Support SparseFloatVector
- Support Float16Vector/BFloat16Vector
- Support SearchIterator/QueryIterator
- Support multiple vector fields in one collection
- Support hybrid-search on multiple vector fields

### Improvement

- Upgrade dependencies to fix some CVEs
- Provide new methods withFloatVectors/withBinaryVectors/withFloat16Vectors/withBFloat16Vectors/withSparseFloatVectors for SearchParam to explicitly input different type vectors

## milvus-sdk-java 2.3.6 (2024-04-22)

### Feature

- Support SearchIterator/QueryIterator

### Improvement

- Upgrade dependencies to fix some CVEs

## milvus-sdk-java 2.3.5 (2024-03-29)

### Feature

- New MilvusClientV2 class to encapsulate RPC interfaces for good usability
- Support ListAlias interface
- Provide a BulkWriter tool for easily generating data files for import() interface

### Improvement

- Replace grpc-netty to grpc-netty-shared
- Support creating index without specifying metricType and indexType
- Support searching without specifying metricType
- Fix a crash bug of SearchResutsWrapper when primary key is varchar type
- Fix a bug of retry that doesn't return server errors
- Fix some vulnerabilities

## milvus-sdk-java 2.3.4 (2024-01-02)

### Improvement

- Support backoff retry for RPC interfaces(consist with pymilvus)
- Upgrade grpc from 1.46 to 1.59.1
- Add withPartitionName for DeleteIdsParam

## milvus-sdk-java 2.3.3 (2023-11-08)

### Feature

- Support resource group interfaces

## milvus-sdk-java 2.3.2 (2023-10-25)

### Feature

- Support Array type field(new feature of Milvus v2.3.2)

### Improvement

- Fix a bug that consistency level is ignored in search() interface
- Fix a bug the MilvusMultiServiceClient cannot specify database name
- Fix a bug that high-level search API can only return the first vector's results
- Upgrade dependencies to fix some vulnerabilities
- Set log level for MilvusMultiServiceClient in runtime
- Support insert dynamic values by column-based

## milvus-sdk-java 2.3.1 (2023-09-05)

### Improvement

- Support COSINE metric type
- Fix a bug that could not get binary vectors from search result
- Fix a bug of high-level get/delete api

## milvus-sdk-java 2.3.0 (2023-08-24)

### Feature

- Support Upsert interface
- New GPU index type(only works when server is GPU mode): GPU_IVF_FLAT, GPU_IVF_PQ

### Deprecated

- No longer support index: ANNOY, RHNSW_FLAT, RHNSW_PQ, RHNSW_SQ
- No longer support metric: TANIMOTO, SUPERSTRUCTURE, SUBSTRUCTURE


## milvus-sdk-java 2.2.15 (2023-11-08)

### Feature

- Support resource group interfaces

### Improvement

- Fix a bug that high-level search API can only return the first vector's results
- Upgrade dependencies to fix some vulnerabilities
- Support insert dynamic values by column-based

## milvus-sdk-java 2.2.14 (2023-09-15)

### Improvement

- Fix the bug that consistency level is ignored in search() interface
- Set log level for MilvusMultiServiceClient in runtime
- Fix the bug the MilvusMultiServiceClient cannot specify database name

## milvus-sdk-java 2.2.13 (2023-09-04)

### Improvement

- Fix a bug that could not get binary vectors from search result
- Fix a bug of high-level get/delete api

## milvus-sdk-java 2.2.12 (2023-08-10)

### Improvement

- Fix a bug that could not create index for scalar field with Milvus v2.2.12(change IndexType.SORT to IndexType.STL_SORT)

## milvus-sdk-java 2.2.11 (2023-08-09)

### Improvement

- Fix a bug that could not create index for VARCHAR field with Milvus v2.2.12

### Deprecated

- withGuaranteeTimestamp()/withGracefulTime() are marked as Deprecated for SearchParam/QueryParam. From Milvus v2.2.9, the time settings are determined by the server side.

## milvus-sdk-java 2.2.10 (2023-08-08)

### Feature

- Support TLS connection
- Support retry for interface

## milvus-sdk-java 2.2.9 (2023-07-03)

### Improvement

- Fix a bug of listBulkInsertTasks()
- Set default shard number to be 1

## milvus-sdk-java 2.2.8 (2023-06-29)

### Improvement

- Fix bug of high-level API
- Add index type SORT for scalar field
- Set log level in runtime

## milvus-sdk-java 2.2.7 (2023-06-21)

### Improvement

- Provide easy to used high-level interfaces
- Add more examples

## milvus-sdk-java 2.2.6 (2023-06-05)

### Improvement

- Support JSON type field
- Support dynamic field
- Support partition key
- Support database management: createDatabase/dropDatabase/listDatabases

## milvus-sdk-java 2.2.5 (2023-04-04)

### Improvement

- Implement flushAll() interface
- Add ignoreGrowing flag for query/search

## milvus-sdk-java 2.2.4 (2023-03-26)

### Improvement

- Implement alterCollection() interface
- Use the same grpc version v1.46.0 as milvus-proto repo

## milvus-sdk-java 2.2.3 (2023-02-11)

### Improvement

- Implement getLoadState() interface
- Add refresh parameter to load() interface
- Add getProcess() for bulkinsert task state
- Fix example error

## milvus-sdk-java 2.2.2 (2023-01-04)

### Bug

- Fix search param offset not avaliable bug


## milvus-sdk-java 2.2.1 (2022-11-22)

### Improvement

- Support pagination for query() interface
- Upgrade commons-text to 1.10.0 to avoid security vulnerabilities


## milvus-sdk-java 2.2.0 (2022-11-18)

### Improvement

- Supports Role-Based Access Control (RBAC)
- Support bulk insert data
- Support DISKANN index


## milvus-sdk-java 2.1.0 (2022-08-31)

### Bug

- Fix keepAliveTimeout timeunit error for ConnectParam

### Improvement

- Remove withGuaranteeTimestamp/withGracefulTime of SearchParam/QueryParam. User only need to provide consistency level
- Change the default consistency level from Strong to Bounded in SearchParam/QueryParam


## milvus-sdk-java 2.1.0-beta4 (2022-07-22)

### Feature

- Refine the InsertParam for better usability, no need to specify data type for each field
- Remove the calcDistance interface because no one use this interface 

## milvus-sdk-java 2.1.0-beta3 (2022-07-15)

### Feature

- Specify index name to replace field name in DescribeIndexParam/GetIndexBuildProgressParam/GetIndexStateParam
- Remove the index type IVF_HNSW(no longer supported)

## milvus-sdk-java 2.1.0-beta2 (2022-07-12)

### Feature

- Support search consistency level

## milvus-sdk-java 2.1.0-beta1 (2022-07-09)

### Feature

- Support Varchar type field
- Implement authentication interfaces: CreateCredential/UpdateCredential/DeleteCredential/ListCredUsers
- Multiserver failover control(optional function)

## milvus-sdk-java 2.0.4 (2022-02-14)

### Feature

- \#260 - Implement async interfaces for insert/search/query
- Expose flush() interface

## milvus-sdk-java 2.0.3 (2022-01-27)

### Improvement

- \#255 - Rename io.milvus.Response to io.milvus.response

## milvus-sdk-java 2.0.2 (2022-01-24)

### Improvement

- \#250 - Upgrade log4j-core to 2.17.1


## milvus-sdk-java 2.0.1 (2022-01-18)

### Improvement

- \#248 - Pass travel timestamp and guarantee timestamp for query/search interface

## milvus-sdk-java 2.0.0 (2021-12-31)

### Feature

- \#183 - java sdk for milvus 2.0

## milvus-sdk-java 0.8.5 (2020-08-26)

### Feature

- \#128 - GRPC timeout support

## milvus-sdk-java 0.8.3 (2020-07-15)

### Improvement

- \#117 - Remove isConnect() API

## milvus-sdk-java 0.8.0 (2020-05-15)

### Feature

- \#93 - Add/Improve getVectorByID, collectionInfo and hasPartition API
- \#2295 - Rename SDK interfaces

## milvus-sdk-java 0.7.0 (2020-04-15)

### Feature

- \#261 - Integrate ANNOY into Milvus
- \#1828 - Add searchAsync / createIndexAsync / insertAsync / flushAsync / compactAsync API

## milvus-sdk-java 0.6.0 (2020-03-31)

### Bug

- \#1641 - Fix incorrect error logging message
- \#1642 - Fix compilation error of ByteBuffer

### Feature

- \#1603 - Add binary metrics: Substructure & Superstructure

## milvus-sdk-java 0.5.0 (2020-03-11)

## milvus-sdk-java 0.4.1 (2019-12-16)

### Bug

- \#78 - Partition tag not working when searching

## milvus-sdk-java 0.4.0 (2019-12-7)

### Bug

- \#74 - Partition tag not working when inserting

### Improvement

- \#61 - Add partition
- \#70 - Add IndexType IVF_PQ
- \#72 - Add more getters in ShowPartitionResponse
- \#73 - Add @Deprecated for DateRanges in SearchParam

## milvus-sdk-java 0.3.0 (2019-11-13)

### Bug

- \#64 - Search failed with exception if search result is empty

### Improvement

- \#56 - Add keepalive and idleTimeout settings
- \#57 - add ok() in other types of Response
- \#62 - Change GRPC proto (and related code) to increase search result's transmission speed
- \#63 - Make some functions and constructors package-private if necessary

## milvus-sdk-java 0.2.2 (2019-11-4)

### Improvement

- \#49 - Add waitTime option in ConnectParam
- \#51 - Change connect waitTime to timeout
- \#52 - Change IVF_SQ8H to IVF_SQ8_H

## milvus-sdk-java 0.2.0 (2019-10-21)

### Bug

- \#42 - fix search result validation
    
### Improvement

- \#3 - Force channel to request connection in connect()  and some code cleanup
- \#6 - Update pom & fix deleteByRange error message & update unittest
- \#8 - change default timeout to 24 hour
- \#9 - Add more getters in SearchResponse & add normalize method in unittest
- \#10 - fix connected() & add port range check & add @nonnull annotation & set maxInboundMessageSize
- \#17 - change IndexParam in DescribeIndexResponse to Index
- \#27 - change proto package to io.milvus.grpc
- \#32 - fix README format
- \#35 - Fix client version in readme and src code
- \#38 - Update examples
- \#40 - Remove timeout parameter & Several API changes    

### Feature

- \#16 - add IVF_SQ8_H index type

### Task

- \#1 - First implementation
- \#21 - Add javadoc
- \#23 - Format code with Google-java-style and add Apache 2.0 license header
- \#28 - add examples
- \#29 - add README
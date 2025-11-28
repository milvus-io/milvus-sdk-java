# Changelog

## milvus-sdk-java 2.6.9 (2025-11-28)
### Improvement
- Add timezone parameter for query/search/QueryIterator/SearchIterator
- Add filterTemplate for QueryIterator/SearchIterator
- Add GetCompactionPlans interface
- Avoid frequent calls of getFlushState() in the flush() loop

### Bug
- Add missed parameter "timestamp" for describeIndex()
- Fix a struct filed bug that fail to insert if two struct fields have the same name subfield

## milvus-sdk-java 2.6.8 (2025-11-21)
### Improvement
- Adding exception handling on connection for MilvusClientV2
- BulkWriter supports Struct/Geometry field
- Add db_name parameter for listImportJobs()

### Bug
- Fox a OutOfBound bug for SearchIteratorV2

## milvus-sdk-java 2.6.7 (2025-11-03)
### Improvement
- Remove Lombok dependency

## milvus-sdk-java 2.6.6 (2025-10-17)
### Feature
- Support NGRAM index
- Support BoostRanker

### Improvement
- Support new metric types MAX_SIM_COSINE/MAX_SIM_IP/MAX_SIM_L2/MAX_SIM_JACCARD/MAX_SIM_HAMMING for vector fields inside Struct field
- Delete index type EMB_LIST_HNSW due to design change of server-side, use normal index types for vector fields inside Struct field

### Bug
- Fix a critical bug that partial upsert override field value to null

## milvus-sdk-java 2.6.5 (2025-09-30)
### Feature
- Support Struct type field
- Support Timestamptz type field
- Support Goemetry type field
- Add MilvusClientV2.updateReplicateConfiguration() for new CDC server

### Improvement
- Add parameter "databaseName" for the following requests: GetCollectionStatsReq, GetLoadStateReq, HasCollectionReq, ListCollectionsReq, ReleaseCollectionReq, RenameCollectionReq, DropIndexReq, ListIndexesReq, CreatePartitionReq, DropPartitionReq, GetPartitinStatsReq, HasPartitionReq, ListPartitionsReq, LoadPartitionsReq, ReleasePartitionsReq, CompactReq, FlushReq, GetPErsistentSegmentInfoReq, GetQuerySegmentInfoReq,
- Increase ClientPool default value of maxTotal from 50 to 1000, maxTotalPerKey from 10 to 50, maxIdlePerKey from 5 to 10

### Bug
- Fix a bug of delete() that databaseName of DeleteReq doesn't work

## milvus-sdk-java 2.5.14 (2025-09-30)
### Improvement
- Add parameter "databaseName" for the following requests: GetCollectionStatsReq, GetLoadStateReq, HasCollectionReq, ListCollectionsReq, ReleaseCollectionReq, RenameCollectionReq, DropIndexReq, ListIndexesReq, CreatePartitionReq, DropPartitionReq, GetPartitinStatsReq, HasPartitionReq, ListPartitionsReq, LoadPartitionsReq, ReleasePartitionsReq, CompactReq, FlushReq, GetPErsistentSegmentInfoReq, GetQuerySegmentInfoReq,
- Increase ClientPool default value of maxTotal from 50 to 1000, maxTotalPerKey from 10 to 50, maxIdlePerKey from 5 to 10

### Bug
- Fix a bug of delete() that databaseName of DeleteReq doesn't work

## milvus-sdk-java 2.6.4 (2025-09-17)
### Feature
- Support MINHASH_LSH/IVF_RABITQ index type
- Support MHJACCARD metric type
- Support passing request-id and unixmsec to server for MilvusClientV2
- Support batchDescribeCollection() interface for MilvusClientV2
- Support FunctionScore, multi-reranker for search/hybridSearch of MilvusClientV2
- MilvusClientPool supports different ConnectConfig for different key

### Improvement
- Return shards number of each collection for MilvusClientV2.listCollections()

### Bug
- Fix a defect of MilvusClientV2.query() that always requires an empty filter expression
- Fix a bug of QueryIterator that offset value cannot exceed 16384

## milvus-sdk-java 2.5.13 (2025-09-17)
### Feature
- Support passing request-id and unixmsec to server for MilvusClientV2
- MilvusClientPool supports different ConnectConfig for different key

### Bug
- Fix a defect of MilvusClientV2.query() that always requires an empty filter expression
- Fix a bug of QueryIterator that offset value cannot exceed 16384

## milvus-sdk-java 2.6.3 (2025-08-20)
### Improvement
- Support stageManager & stageFileManager

## milvus-sdk-java 2.6.2 (2025-08-14)
### Improvement
- Alias interface support database
- Allows upsert when autoid=true for MilvusClientV1
- Support more Rerank types for hybridSearch(), including decay and model rerank
- Support ignoring growing segments for MilvusClientV2.query()
- Use cached collection schema for query by ids to avoid calling describe_collection()
- Add objectUrls parameter for bulkimport interfaces
- Add a method to return the current used database name for MilvusClientV2
### Bug
- Fix a bug of Function.multiAnalyzerParams
- Fix a bug of FunctionType.TEXTEMBEDDING

## milvus-sdk-java 2.5.12 (2025-08-14)
### Improvement
- Alias interface support database
- Allows upsert when autoid=true for MilvusClientV1
- Support ignoring growing segments for MilvusClientV2.query()
- Use cached collection schema for query by ids to avoid calling describe_collection()
- Add objectUrls parameter for bulkimport interfaces
- Add a method to return the current used database name for MilvusClientV2
### Bug
- Fix a bug of Function.multiAnalyzerParams

## milvus-sdk-java 2.6.1 (2025-07-15)
### Feature
- Support uploading file to Zilliz Cloud Stage for BulkWriter

### Bug
- Fix a bug of SearchResultsWrapper.getRowRecords() that returns wrong data for output fields
- Fix a null pointer bug of CreateCollectionReq.indexParam()
- Fix a no such key bug of BulkWriter
- Fix potential bugs of ConsistencyLevel when dml/dql requests across databases
- Fix potential bugs of collection schema cache when dml requests across databases

### Improvement
- Deprecate topK for search/hybridSearch/iterator, replaced with limit
- Deprecate expr of AnnSearchReq, replaced with filter
- Add database parameter for HybridSearchParam/QueryParam/SearchParam in V1
- Add database parameter for LoadCollectionReq/RefreshLoadReq in V2
- Avoid exception when search result is empty
- BulkWriter supports Int8Vector

## milvus-sdk-java 2.5.11 (2025-07-15)
### Feature
- Support uploading file to Zilliz Cloud Stage for BulkWriter

### Bug
- Fix a bug of SearchResultsWrapper.getRowRecords() that returns wrong data for output fields
- Fix a bug of flush that timestamp is not correctly passed
- Fix a null pointer bug of CreateCollectionReq.indexParam()
- Fix a no such key bug of BulkWriter
- Fix potential bugs of ConsistencyLevel when dml/dql requests across databases
- Fix potential bugs of collection schema cache when dml requests across databases

### Improvement
- Deprecate topK of search/hybridSearch/iterator, replaced with limit
- Deprecate expr of AnnSearchReq, replaced with filter
- Add database parameter for HybridSearchParam/QueryParam/SearchParam in V1
- Add database parameter for LoadCollectionReq/RefreshLoadReq in V2
- Avoid exception when search result is empty
- Support jsonPath index
- Support run analyzer by collection and Field

## milvus-sdk-java 2.6.0 (2025-06-13)
### Feature
- Support jsonPath index
- Support addCollectionField()
- Support FunctionType.RERANK
- Support run analyzer by collection and Field
- Support Int8Vector

### Bug
- Fix a bug of flush that timestamp is not correctly passed

## milvus-sdk-java 2.5.10 (2025-06-05)
### Feature
- Support dropCollectionFieldProperties interface
- Support multi-language analyzer

### Bug
- Reformat SearchResult/IDScore print content to show primary key
- Fix a bug of listIndexes()

## milvus-sdk-java 2.5.9 (2025-05-09)
### Feature
- Support runAnalyzer() interface

### Bug
- Fix a bug that index property was not correctly passed
- Fix an exception to LocalBulkWriter in Java 24 env

### Improvement
- Add dbName for DescribeIndexReq
- Bump protobuf/protoc version from 3.24.0 to 3.25.5
- Bump Gson version from 2.10.0 to 2.13.1
- describeCollection returns collection createUtcTime
- Add a static method CreateSchema() to replace non-static method createSchema() in MilvusClientV2

## milvus-sdk-java 2.5.8 (2025-04-25)
### Feature
- Support getPersistentSegmentInfo/getQuerySegmentInfo interfaces for V2
- Support transferNode interface for V2
- Support checkHealth interface for V2

## milvus-sdk-java 2.5.7 (2025-04-09)
### Bug
- Fix a compatible bug with milvus v2.5.8 that QueryResp.QueryResult doesn't return primary key
- Fix a bug of nullable Array field that null value should be returned instead of an empty list

### Improvement
- Add proxy setting for connection to milvus
- Support offset parameter for hybridSearch() interface
- Add options parameter for bulkImport() interface

## milvus-sdk-java 2.5.6 (2025-03-20)
### Improvement
- Optimize MilvusClientV2 search/query to support databaseName
- Add SearchIteratorV2 to get better performance and recall compare to old SearchIterator
- Add sync parameter for loadCollection/loadPartitions/createIndex

## milvus-sdk-java 2.5.5 (2025-03-07)
### Feature
- Support HNSW_PQ/HNSW_SQ/HNSW_PRQ
- Support describeReplicas() interface
- BulkWriter supports nullable/default_value

### Improvement
- Add shardsNum for describeCollection
- LoadPartitions support replicas
- Optimize BulkWriter to reduce memory usage

## milvus-sdk-java 2.5.4 (2025-01-09)
### Improvement
- HybridSearch supports full text search

## milvus-sdk-java 2.5.3 (2024-12-31)
### Feature
- Support alterCollectionField interface for V2
- Support refreshLoad/getPartitionStats interfaces for V2
- Support dropIndexProperties/dropDatabaseProperties/dropCollectionProperties for V2
- Support resource group interfaces for V2

### Improvement
- Return recall rate in search result for V2
- QueryIterator/SearchIterator support retry

## milvus-sdk-java 2.5.2 (2024-12-11)
### Improvement
- Split milvus Java SDK to two packages to reduce dependency complexity

### Bug
- Fix a bug of listPrivilegeGroups

## milvus-sdk-java 2.5.1 (2024-12-04)
### Improvement
- Support upsert items with auto-id primary key

### Bug
- Critical: Fix a bug that dynamic values are skipped with enableDynamicField is true

## milvus-sdk-java 2.5.0 (2024-11-26)
### Feature
- BulkWriter supports JSON/CSV format
- Add new RBAC interfaces grantPrivilegeV2/revokePrivilegeV2
- Support doc-in-doc-out function(embedded BM25 in server-side)
- Support varchar analyzer (tokenizer in server-side)
- Support nullable and default value
- Support expression template

### Improvement
- QueryIterator/SearchIterator supports mvcc
- ClientPool throws exception if fail to create/get clients

## milvus-sdk-java 2.4.11 (2025-04-09)
### Bug
- Fix a bug that consistency level is missed for createCollection()
- Return shards_num for describeCollection()

## milvus-sdk-java 2.4.10 (2024-12-31)
### Feature
- Support alterCollectionField interface for V2
- Support refreshLoad/getPartitionStats interfaces for V2
- Support dropIndexProperties/dropDatabaseProperties/dropCollectionProperties for V2
- Support resource group interfaces for V2
- 
## milvus-sdk-java 2.4.9 (2024-11-26)

### Feature
- BulkWriter supports JSON format
- Add new RBAC interfaces grantPrivilegeV2/revokePrivilegeV2

### Improvement
- ClientPool throws exception if fail to create/get clients

## milvus-sdk-java 2.4.8 (2024-10-31)

### Improvement
- Fix a conflict bug with Gson v2.11.0
- Support group by for hybrid search

### Bug
- Fix a dimension check error for binary vector
- Fix a null pointer bug for query iterator

## milvus-sdk-java 2.4.7 (2024-10-25)

### Improvement
- Support setting properties for V2 CreateCollectionReq
- Support Session consistency level for V1 and V2
- Return entities ids for V2 InsertResp

### Bug
- Fix a RpcDeadline bug for V2

### Feature
- Support flush() interface for V2
- Support compact()/getCompactionState() interfaces for V2

## milvus-sdk-java 2.4.6 (2024-10-18)

### Improvement
- Refine BulkWriter/BulkImport interfaces
- Remove Jackson dependency

### Bug
- Fix "one second timeout issue" of pre-connection

## milvus-sdk-java 2.4.5 (2024-10-11)

### Feature
- Support partial load
- Support clustering key

### Bug
- Fix a bug for MilvusClientV2 that index parameters were not passed to server correctly

## milvus-sdk-java 2.4.4 (2024-09-19)

### Improvement
- Upgrade the bulkWriter cloud API call from v1 to v2
- Improve usability of AlterCollectionReq & CreateCollectionReq
- Check connection when MilvusClientV2 is initialized
- Support customized SSLContext for MilvusClientV2
- Reduce time-consuming log of search/insert/upsert for MilvusClientV1 

### Bug
- Fix a bug or QueryIterator with special expression

## milvus-sdk-java 2.4.3 (2024-08-09)

### Feature
- Implement database related interfaces including createDatabase/dropDatabase/alterDatabase for MilvusClientV2
- Implement getServerVersion interface for MilvusClientV2
- Provide client/connection pool for V1 and V2

### Improvement
- Implement retry machinery for MilvusClientV2

### Bug
- Fix thread-safe bug of insert/upsert interfaces for MilvusClientV2
- Fix a bug of describeCollection that collection properties not returned for MilvusClientV2

## milvus-sdk-java 2.4.2 (2024-07-11)

### Feature
- Support AlterDatabase/DescribeDatabase for V1
- Cache collection schema in client side for insert/upsert interfaces
- Support AlterCollection/AlterIndex for MilvusClientV2
- Support propagate traceid from client
- BulkWriter supports SparseVector/Float16Vector/BFloat16Vector
- Support SparseVector/Float16Vector/BFloat16Vector for MilvusClientV2
- Support GroupBy search for MilvusClientV2
- Support SearchIterator/QueryIterator for MilvusClientV2
- Optimize DescribeIndex interface of MilvusClientV2
- Optimize DescribeCollection interface of MilvusClientV2
- Support enableVirtualStyleEndpoint for BulkWriter

### Bug
- Fix a bug of max_capacity range

### Break changes
- Replace FastJSON by Gson according to issue [#878](https://github.com/milvus-io/milvus-sdk-java/issues/878). InsertParam.withRows()/UpsertParam.withRows()/InsertReq.data() are redefined.
- Rename "distance" to "score" for search result. SearchResp.distance() of V2 is renamed to be score().

## milvus-sdk-java 2.4.1 (2024-05-11)

### Bug

- Unable to connect Zilliz cloud new severless instances
- SearchIterator cannot work for Varchar type primary key
- Fix some minor bugs of SearchIterator

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

## milvus-sdk-java 2.3.11 (2024-10-11)

### Bug
- Fix a bug for MilvusClientV2 that index parameters were not passed to server correctly

## milvus-sdk-java 2.3.10 (2024-09-19)

### Improvement
- Improve usability of CreateCollectionReq
- Check connection when MilvusClientV2 is initialized
- Reduce time-consuming log of search/insert/upsert for MilvusClientV1

### Bug
- Fix a bug or QueryIterator with special expression

## milvus-sdk-java 2.3.9 (2024-08-09)

### Feature
- Implement database related interfaces including createDatabase/dropDatabase/listDatabases for MilvusClientV2
- Implement getServerVersion interface for MilvusClientV2
- Provide client/connection pool for V1 and V2

### Improvement
- Implement retry machinery for MilvusClientV2

### Bug
- Fix thread-safe bug of insert/upsert interfaces for MilvusClientV2

## milvus-sdk-java 2.3.8 (2024-07-11)

### Feature
- Support SearchIterator/QueryIterator for MilvusClientV2
- Optimize DescribeIndex interface of MilvusClientV2
- Optimize DescribeCollection interface of MilvusClientV2
- Support enableVirtualStyleEndpoint for BulkWriter

### Bug
- Fix a bug of max_capacity range

### Break changes
- Replace FastJSON by Gson according to issue [#878](https://github.com/milvus-io/milvus-sdk-java/issues/878). InsertParam.withRows()/UpsertParam.withRows()/InsertReq.data() are redefined.
- Rename "distance" to "score" for search result. SearchResp.distance() of V2 is renamed to be score().


## milvus-sdk-java 2.3.7 (2024-05-11)

### Bug

- Unable to connect Zilliz cloud new severless instances
- SearchIterator cannot work for Varchar type primary key
- Fix some minor bugs of SearchIterator

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
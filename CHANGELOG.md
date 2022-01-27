# Changelog

## milvus-sdk-java 2.0.3 (2021-01-27)

### Improvement

- \#255 - Rename io.milvus.Response to io.milvus.response

## milvus-sdk-java 2.0.2 (2021-01-24)

### Improvement

- \#250 - Upgrade log4j-core to 2.17.1


## milvus-sdk-java 2.0.1 (2021-01-18)

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
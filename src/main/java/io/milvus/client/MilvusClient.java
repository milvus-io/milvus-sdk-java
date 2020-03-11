/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.client;

import java.util.List;


/** The Milvus Client Interface */
public interface MilvusClient {

  String clientVersion = "0.5.0";

  /** @return current Milvus client version： 0.5.0 */
  default String getClientVersion() {
    return clientVersion;
  }

  /**
   * Connects to Milvus server
   *
   * @param connectParam the <code>ConnectParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * ConnectParam connectParam = new ConnectParam.Builder()
   *                                             .withHost("localhost")
   *                                             .withPort(19530)
   *                                             .withConnectTimeout(10, TimeUnit.SECONDS)
   *                                             .withKeepAliveTime(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
   *                                             .withKeepAliveTimeout(20, TimeUnit.SECONDS)
   *                                             .keepAliveWithoutCalls(false)
   *                                             .withIdleTimeout(24, TimeUnit.HOURS)
   *                                             .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @throws ConnectFailedException if client failed to connect
   * @see ConnectParam
   * @see Response
   * @see ConnectFailedException
   */
  Response connect(ConnectParam connectParam) throws ConnectFailedException;

  /**
   * @return <code>true</code> if the client is connected to Milvus server and the channel's
   *     connectivity state is READY.
   */
  boolean isConnected();

  /**
   * Disconnects from Milvus server
   *
   * @return <code>Response</code>
   * @throws InterruptedException
   * @see Response
   */
  Response disconnect() throws InterruptedException;

  /**
   * Creates collection specified by <code>collectionMapping</code>
   *
   * @param collectionMapping the <code>CollectionMapping</code> object
   *     <pre>
   * example usage:
   * <code>
   * CollectionMapping collectionMapping = new CollectionMapping.Builder(collectionName, dimension)
   *                                          .withIndexFileSize(1024)
   *                                          .withMetricType(MetricType.IP)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see CollectionMapping
   * @see MetricType
   * @see Response
   */
  Response createCollection(CollectionMapping collectionMapping);

  /**
   * Checks whether the collection exists
   *
   * @param collectionName collection to check
   * @return <code>HasCollectionResponse</code>
   * @see HasCollectionResponse
   * @see Response
   */
  HasCollectionResponse hasCollection(String collectionName);

  /**
   * Drops collection
   *
   * @param collectionName collection to drop
   * @return <code>Response</code>
   * @see Response
   */
  Response dropCollection(String collectionName);

  /**
   * Creates index specified by <code>index</code>
   *
   * @param index the <code>Index</code> object
   *     <pre>
   * example usage:
   * <code>
   * Index index = new Index.Builder(collectionName, IndexType.IVF_SQ8)
   *                        .withParamsInJson("{\"nlist\": 19384}")
   *                        .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see Index
   * @see IndexType
   * @see Response
   */
  Response createIndex(Index index);

  /**
   * Creates a partition specified by <code>collectionName</code> and <code>tag</code>
   *
   * @param collectionName collection name
   * @param tag partition tag
   * @return <code>Response</code>
   * @see Response
   */
  Response createPartition(String collectionName, String tag);

  /**
   * Shows current partitions of a collection
   *
   * @param collectionName collection name
   * @return <code>ShowPartitionsResponse</code>
   * @see ShowPartitionsResponse
   * @see Response
   */
  ShowPartitionsResponse showPartitions(String collectionName);

  /**
   * Drops partition specified by <code>collectionName</code> and <code>tag</code>
   *
   * @param collectionName collection name
   * @param tag partition tag
   * @see Response
   */
  Response dropPartition(String collectionName, String tag);

  /**
   * Inserts data specified by <code>insertParam</code>
   *
   * @param insertParam the <code>InsertParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * InsertParam insertParam = new InsertParam.Builder(collectionName)
   *                                          .withFloatVectors(floatVectors)
   *                                          .withVectorIds(vectorIds)
   *                                          .withPartitionTag(tag)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>InsertResponse</code>
   * @see InsertParam
   * @see InsertResponse
   * @see Response
   */
  InsertResponse insert(InsertParam insertParam);

  /**
   * Searches vectors specified by <code>searchParam</code>
   *
   * @param searchParam the <code>SearchParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(collectionName)
   *                                          .withFloatVectors(floatVectors)
   *                                          .withTopK(topK)
   *                                          .withPartitionTags(partitionTagsList)
   *                                          .withParamsInJson("{\"nprobe\": 20}")
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>SearchResponse</code>
   * @see SearchParam
   * @see SearchResponse
   * @see SearchResponse.QueryResult
   * @see Response
   */
  SearchResponse search(SearchParam searchParam);

  /**
   * Searches vectors in specific files specified by <code>searchInFilesParam</code>
   *
   * @param fileIds list of file ids to search from
   * @param searchParam the <code>SearchParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(collectionName)
   *                                          .withFloatVectors(floatVectors)
   *                                          .withTopK(topK)
   *                                          .withPartitionTags(partitionTagsList)
   *                                          .withParamsInJson("{\"nprobe\": 20}")
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>SearchResponse</code>
   * @see SearchParam
   * @see SearchResponse
   * @see SearchResponse.QueryResult
   * @see Response
   */
  SearchResponse searchInFiles(List<String> fileIds, SearchParam searchParam);

  /**
   * Describes the collection
   *
   * @param collectionName collection to describe
   * @see DescribeCollectionResponse
   * @see CollectionMapping
   * @see Response
   */
  DescribeCollectionResponse describeCollection(String collectionName);

  /**
   * Shows current collections
   *
   * @return <code>ShowCollectionsResponse</code>
   * @see ShowCollectionsResponse
   * @see Response
   */
  ShowCollectionsResponse showCollections();

  /**
   * Gets current row count of a collection
   *
   * @param collectionName collection to get row count
   * @return <code>GetCollectionRowCountResponse</code>
   * @see GetCollectionRowCountResponse
   * @see Response
   */
  GetCollectionRowCountResponse getCollectionRowCount(String collectionName);

  /**
   * Get server status
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response getServerStatus();

  /**
   * Get server version
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response getServerVersion();

  /**
   * Sends a command to server
   *
   * @return <code>Response</code> command's response will be return in <code>message</code>
   * @see Response
   */
  Response command(String command);

  /**
   * Pre-loads collection to memory
   *
   * @param collectionName collection to preload
   * @return <code>Response</code>
   * @see Response
   */
  Response preloadCollection(String collectionName);

  /**
   * Describes collection index
   *
   * @param collectionName collection to describe index of
   * @see DescribeIndexResponse
   * @see Index
   * @see Response
   */
  DescribeIndexResponse describeIndex(String collectionName);

  /**
   * Drops collection index
   *
   * @param collectionName collection to drop index of
   * @see Response
   */
  Response dropIndex(String collectionName);

  /**
   * Shows collection information. A collection consists of one or multiple partitions (including
   * the default partition), and a partitions consists of one or more segments. Each partition or
   * segment can be uniquely identified by its partition tag or segment name respectively.
   *
   * @param collectionName collection to show info from
   * @see ShowCollectionInfoResponse
   * @see CollectionInfo
   * @see CollectionInfo.PartitionInfo
   * @see CollectionInfo.PartitionInfo.SegmentInfo
   * @see Response
   */
  ShowCollectionInfoResponse showCollectionInfo(String collectionName);

  /**
   * Gets either a float or binary vector by id.
   *
   * @param collectionName collection to get vector from
   * @param id vector id
   * @see GetVectorByIdResponse
   * @see Response
   */
  GetVectorByIdResponse getVectorById(String collectionName, Long id);

  /**
   * Gets all vector ids in a segment
   *
   * @param collectionName collection to get vector ids from
   * @param segmentName segment name
   * @see GetVectorIdsResponse
   * @see Response
   */
  GetVectorIdsResponse getVectorIds(String collectionName, String segmentName);

  /**
   * Deletes data in a collection by a list of ids
   *
   * @param collectionName collection to delete ids from
   * @param ids a <code>List</code> of vector ids to delete
   * @see Response
   */
  Response deleteByIds(String collectionName, List<Long> ids);

  /**
   * Deletes data in a collection by a single id
   *
   * @param collectionName collection to delete id from
   * @param id vector id to delete
   * @see Response
   */
  Response deleteById(String collectionName, Long id);

  /**
   * Flushes data in a list collections. Newly inserted or modifications on data will be visible
   * after <code>flush</code> returned
   *
   * @param collectionNames a <code>List</code> of collections to flush
   * @see Response
   */
  Response flush(List<String> collectionNames);

  /**
   * Flushes data in a collection. Newly inserted or modifications on data will be visible after
   * <code>flush</code> returned
   *
   * @param collectionName name of collection to flush
   * @see Response
   */
  Response flush(String collectionName);

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   *
   * @param collectionName name of collection to compact
   * @see Response
   */
  Response compact(String collectionName);
}

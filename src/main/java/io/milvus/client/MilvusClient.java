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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** The Milvus Client Interface */
public interface MilvusClient extends AutoCloseable {

  String extraParamKey = "params";

  String clientVersion = new Supplier<String>() {

    @Override
    /** @return current Milvus client version */
    public String get() {
      Properties properties = new Properties();
      try (InputStream inputStream =
               MilvusClient.class.getClassLoader()
                   .getResourceAsStream("milvus-client.properties")) {
        properties.load(inputStream);
      } catch (IOException ex) {
        ExceptionUtils.wrapAndThrow(ex);
      }
      return properties.getProperty("version");
    }
  }.get();

  String target();

  /** @return current Milvus client version： 0.9.0 */
  default String getClientVersion() {
    return clientVersion;
  }

  /**
   * Close this MilvusClient. Wait at most 1 minute for graceful shutdown.
   */
  default void close() {
    close(TimeUnit.MINUTES.toSeconds(1));
  }

  /**
   * Close this MilvusClient. Wait at most `maxWaitSeconds` for graceful shutdown.
   */
  void close(long maxWaitSeconds);

  MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit);

  /**
   * Creates collection specified by <code>collectionMapping</code>
   *
   * @param collectionMapping the <code>CollectionMapping</code> object
   * <pre>
   * example usage:
   * <code>
   * CollectionMapping collectionMapping = new CollectionMapping.Builder(collectionName)
   *                                                            .withFields(fields)
   *                                                            .withParamsInJson("{"segment_row_limit": 100000}")
   *                                                            .build();
   * </code>
   * Refer to <code>withFields</code> method for example <code>fields</code> usage.
   * </pre>
   *
   * @see CollectionMapping
   */
  void createCollection(CollectionMapping collectionMapping);

  /**
   * Checks whether the collection exists
   *
   * @param collectionName collection to check
   * @return true if the collection exists, false otherwise.
   */
  boolean hasCollection(String collectionName);

  /**
   * Drops collection
   *
   * @param collectionName collection to drop
   */
  void dropCollection(String collectionName);

  /**
   * Creates index specified by <code>index</code>
   *
   * @param index the <code>Index</code> object
   * <pre>
   * example usage:
   * <code>
   * Index index = new Index.Builder(collectionName, fieldName)
   *                        .withParamsInJson(
   *                            "{"index_type": "IVF_FLAT", "metric_type": "L2",
   *                              "params": {"nlist": 16384}}")
   *                        .build();
   * </code>
   * </pre>
   *
   * @see Index
   */
  void createIndex(Index index);

  /**
   * Creates index specified by <code>index</code> asynchronously
   *
   * @param index the <code>Index</code> object
   * <pre>
   * example usage:
   * <code>
   * Index index = new Index.Builder(collectionName, fieldName)
   *                        .withParamsInJson(
   *                            "{"index_type": "IVF_FLAT", "metric_type": "L2",
   *                              "params\": {"nlist": 16384}}")
   *                        .build();
   * </code>
   * </pre>
   *
   * @return a <code>ListenableFuture</code> object which holds the <code>Response</code>
   * @see Index
   * @see ListenableFuture
   */
  ListenableFuture<Void> createIndexAsync(Index index);

  /**
   * Creates a partition specified by <code>collectionName</code> and <code>tag</code>
   *
   * @param collectionName collection name
   * @param tag partition tag
   */
  void createPartition(String collectionName, String tag);

  /**
   * Checks whether the partition exists
   *
   * @param collectionName collection name
   * @param tag partition tag
   * @return true if the partition exists, false otherwise.
   */
  boolean hasPartition(String collectionName, String tag);

  /**
   * Lists current partitions of a collection
   *
   * @param collectionName collection name
   * @return a list of partition names
   */
  List<String> listPartitions(String collectionName);

  /**
   * Drops partition specified by <code>collectionName</code> and <code>tag</code>
   *
   * @param collectionName collection name
   * @param tag partition tag
   */
  void dropPartition(String collectionName, String tag);

  /**
   * Inserts data specified by <code>insertParam</code>
   *
   * @param insertParam the <code>InsertParam</code> object
   * <pre>
   * example usage:
   * <code>
   * InsertParam insertParam = new InsertParam.Builder(collectionName)
   *                                          .withFields(fields)
   *                                          .withEntityIds(entityIds)
   *                                          .withPartitionTag(tag)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return a list of ids for the inserted entities
   * @see InsertParam
   */
  List<Long> insert(InsertParam insertParam);

  /**
   * Inserts data specified by <code>insertParam</code> asynchronously
   *
   * @param insertParam the <code>InsertParam</code> object
   * <pre>
   * example usage:
   * <code>
   * InsertParam insertParam = new InsertParam.Builder(collectionName)
   *                                          .withFields(fields)
   *                                          .withEntityIds(entityIds)
   *                                          .withPartitionTag(tag)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return a <code>ListenableFuture</code> object which holds the list of ids for the inserted entities.
   * @see InsertParam
   * @see ListenableFuture
   */
  ListenableFuture<List<Long>> insertAsync(InsertParam insertParam);

  /**
   * Searches entities specified by <code>searchParam</code>
   *
   * @param searchParam the <code>SearchParam</code> object
   * <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(collectionName)
   *                                          .withDSL(dslStatement)
   *                                          .withPartitionTags(partitionTagsList)
   *                                          .withParamsInJson("{"fields": ["B"]}")
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>SearchResult</code>
   * @see SearchParam
   * @see SearchResult
   */
  SearchResult search(SearchParam searchParam);

  /**
   * Searches entities specified by <code>searchParam</code> asynchronously
   *
   * @param searchParam the <code>SearchParam</code> object
   * <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(collectionName)
   *                                          .withDSL(dslStatement)
   *                                          .withPartitionTags(partitionTagsList)
   *                                          .withParamsInJson("{"fields": ["B"]}")
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return a <code>ListenableFuture</code> object which holds the <code>SearchResult</code>
   * @see SearchParam
   * @see SearchResult
   * @see ListenableFuture
   */
  ListenableFuture<SearchResult> searchAsync(SearchParam searchParam);

  /**
   * Gets collection info
   *
   * @param collectionName collection to describe
   * @return <code>CollectionMapping</code>
   * @see CollectionMapping
   */
  CollectionMapping getCollectionInfo(String collectionName);

  /**
   * Lists current collections
   *
   * @return a list of collection names
   */
  List<String> listCollections();

  /**
   * Gets current entity count of a collection
   *
   * @param collectionName collection name
   * @return a count of entities in the collection
   */
  long countEntities(String collectionName);

  /**
   * Gets server status
   *
   * @return a server status string
   */
  String getServerStatus();

  /**
   * Gets server version
   *
   * @return a server version string.
   */
  String getServerVersion();

  /**
   * Sends a command to server
   *
   * @return a message string
   */
  String command(String command);

  /**
   * Pre-loads collection to memory
   *
   * @param collectionName collection to load
   */
  void loadCollection(String collectionName);

  /**
   * Drops collection index
   *
   * @param collectionName The collection to drop index.
   * @param fieldName Name of the field to drop index for. If this is set to empty string,
   *                  index of all fields in the collection will be dropped.
   */
  void dropIndex(String collectionName, String fieldName);

  /**
   * Shows collection information. A collection consists of one or multiple partitions (including
   * the default partition), and a partitions consists of one or more segments. Each partition or
   * segment can be uniquely identified by its partition tag or segment name respectively. The
   * result will be returned as JSON string.
   *
   * @param collectionName collection to show info from
   */
  String getCollectionStats(String collectionName);

  /**
   * Gets entities data by id array
   *
   * @param collectionName collection to get entities from
   * @param ids a <code>List</code> of entity ids
   * @param fieldNames  a <code>List</code> of field names. Server will only return entity
   *                    information for these fields.
   * @return a map of entity id to entity properties
   */
  Map<Long, Map<String, Object>> getEntityByID(String collectionName, List<Long> ids, List<String> fieldNames);

  /**
   * Gets entities data by id array
   *
   * @param collectionName collection to get entities from
   * @param ids a <code>List</code> of entity ids
   * @return a map of entity id to entity properties
   */
  Map<Long, Map<String, Object>> getEntityByID(String collectionName, List<Long> ids);

  /**
   * Gets all entity ids in a segment
   *
   * @param collectionName collection to get entity ids from
   * @param segmentId segment id in the collection
   * @return a list of entity ids in the segment
   */
  List<Long> listIDInSegment(String collectionName, Long segmentId);

  /**
   * Deletes data in a collection by a list of ids
   *
   * @param collectionName collection to delete ids from
   * @param ids a <code>List</code> of entity ids to delete
   */
  void deleteEntityByID(String collectionName, List<Long> ids);

  /**
   * Flushes data in a list collections. Newly inserted or modifications on data will be visible
   * after <code>flush</code> returned
   *
   * @param collectionNames a <code>List</code> of collections to flush
   */
  void flush(List<String> collectionNames);

  /**
   * Flushes data in a list collections asynchronously. Newly inserted or modifications on data will
   * be visible after <code>flush</code> returned
   *
   * @param collectionNames a <code>List</code> of collections to flush
   * @return <code>ListenableFuture</code>
   * @see ListenableFuture
   */
  ListenableFuture<Void> flushAsync(List<String> collectionNames);

  /**
   * Flushes data in a collection. Newly inserted or modifications on data will be visible after
   * <code>flush</code> returned
   *
   * @param collectionName name of collection to flush
   */
  void flush(String collectionName);

  /**
   * Flushes data in a collection asynchronously. Newly inserted or modifications on data will be
   * visible after <code>flush</code> returned
   *
   * @param collectionName name of collection to flush
   * @return <code>ListenableFuture</code>
   * @see ListenableFuture
   */
  ListenableFuture<Void> flushAsync(String collectionName);

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   *
   * @param compactParam the <code>CompactParam</code> object
   * <pre>
   * example usage:
   * <code>
   * CompactParam compactParam = new CompactParam.Builder(collectionName)
   *                                             .withThreshold(0.3)
   *                                             .build();
   * </code>
   * </pre>
   *
   * @see CompactParam
   */
  void compact(CompactParam compactParam);

  /**
   * Compacts the collection, erasing deleted data from disk and rebuild index in background (if the
   * data size after compaction is still larger than indexFileSize). Data was only soft-deleted
   * until you call compact.
   *
   * @param compactParam the <code>CompactParam</code> object
   * <pre>
   * example usage:
   * <code>
   * CompactParam compactParam = new CompactParam.Builder(collectionName)
   *                                             .withThreshold(0.3)
   *                                             .build();
   * </code>
   * </pre>
   *
   * @return a <code>ListenableFuture</code> object which holds the <code>Response</code>
   * @see CompactParam
   * @see ListenableFuture
   */
  ListenableFuture<Void> compactAsync(CompactParam compactParam);
}

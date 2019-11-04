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

/** The Milvus Client Interface */
public interface MilvusClient {

  String clientVersion = "0.2.1";

  /** @return the current Milvus client version */
  default String getClientVersion() {
    return clientVersion;
  }

  /**
   * Connects to Milvus server
   *
   * @param connectParam the <code>ConnectParam</code> object
   * <pre>
   * example usage:
   * <code>
   * ConnectParam connectParam = new ConnectParam.Builder()
   *                                             .withHost("localhost")
   *                                             .withPort("19530")
   *                                             .withTimeout(10000)
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

  /** @return <code>true</code> if the client is connected to Milvus server */
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
   * Creates table specified by <code>tableSchemaParam</code>
   *
   * @param tableSchema the <code>TableSchema</code> object
   * <pre>
   * example usage:
   * <code>
   * TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
   *                                          .withIndexFileSize(1024)
   *                                          .withMetricType(MetricType.IP)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see TableSchema
   * @see MetricType
   * @see Response
   */
  Response createTable(TableSchema tableSchema);

  /**
   * Check whether table exists
   *
   * @param tableName table to check
   * @return <code>HasTableResponse</code>
   * @see HasTableResponse
   * @see Response
   */
  HasTableResponse hasTable(String tableName);

  /**
   * Drops table
   *
   * @param tableName table to drop
   * @return <code>Response</code>
   * @see Response
   */
  Response dropTable(String tableName);

  /**
   * Creates index specified by <code>indexParam</code>
   *
   * @param createIndexParam the <code>CreateIndexParam</code> object
   * <pre>
   * example usage:
   * <code>
   * Index index = new Index.Builder()
   *                        .withIndexType(IndexType.IVF_SQ8)
   *                        .withNList(16384)
   *                        .build();
   * CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName)
   *                                                         .withIndex(index)
   *                                                         .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see Index
   * @see CreateIndexParam
   * @see IndexType
   * @see Response
   */
  Response createIndex(CreateIndexParam createIndexParam);

  /**
   * Inserts data specified by <code>insertParam</code>
   *
   * @param insertParam the <code>InsertParam</code> object
   * <pre>
   * example usage:
   * <code>
   * InsertParam insertParam = new InsertParam.Builder(tableName, vectors)
   *                                          .withVectorIds(vectorIds)
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
   * <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(tableName, vectorsToSearch)
   *                                          .withTopK(topK)
   *                                          .withNProbe(nProbe)
   *                                          .withDateRanges(dateRanges)
   *                                          .build();
   * </code>
   * </pre>
   *
   * @return <code>SearchResponse</code>
   * @see SearchParam
   * @see DateRange
   * @see SearchResponse
   * @see SearchResponse.QueryResult
   * @see Response
   */
  SearchResponse search(SearchParam searchParam);

  /**
   * Searches vectors in specific files specified by <code>searchInFilesParam</code>
   *
   * @param searchInFilesParam the <code>SearchInFilesParam</code> object
   * <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(tableName, vectorsToSearch)
   *                                          .withTopK(topK)
   *                                          .withNProbe(nProbe)
   *                                          .withDateRanges(dateRanges)
   *                                          .build();
   * SearchInFilesParam searchInFilesParam = new SearchInFilesParam.Builder(fileIds, searchParam)
   *                                                               .build();
   * </code>
   * </pre>
   *
   * @return <code>SearchResponse</code>
   * @see SearchInFilesParam
   * @see SearchParam
   * @see DateRange
   * @see SearchResponse
   * @see SearchResponse.QueryResult
   * @see Response
   */
  SearchResponse searchInFiles(SearchInFilesParam searchInFilesParam);

  /**
   * Describes table
   *
   * @param tableName table to describe
   * @see DescribeTableResponse
   * @see Response
   */
  DescribeTableResponse describeTable(String tableName);

  /**
   * Shows current tables
   *
   * @return <code>ShowTablesResponse</code>
   * @see ShowTablesResponse
   * @see Response
   */
  ShowTablesResponse showTables();

  /**
   * Gets current row count of table
   *
   * @param tableName table to count
   * @return <code>GetTableRowCountResponse</code>
   * @see GetTableRowCountResponse
   * @see Response
   */
  GetTableRowCountResponse getTableRowCount(String tableName);

  /**
   * Prints server status
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response getServerStatus();

  /**
   * Prints server version
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response getServerVersion();

  /**
   * Pre-loads table to memory
   *
   * @param tableName table to preload
   * @return <code>Response</code>
   * @see Response
   */
  Response preloadTable(String tableName);

  /**
   * Describes table index
   *
   * @param tableName table to describe index of
   * @see DescribeIndexResponse
   * @see Index
   * @see Response
   */
  DescribeIndexResponse describeIndex(String tableName);

  /**
   * Drops table index
   *
   * @param tableName table to drop index of
   * @see Response
   */
  Response dropIndex(String tableName);
}

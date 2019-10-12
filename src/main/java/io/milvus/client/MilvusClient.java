/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.milvus.client;

/** The Milvus Client Interface */
public interface MilvusClient {

  String clientVersion = "0.1.0";

  /** @return the current Milvus client version */
  default String clientVersion() {
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
   *                                             .withPort("19530")
   *                                             .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see ConnectParam
   * @see Response
   */
  Response connect(ConnectParam connectParam);

  /** @return <code>true</code> if the client is connected to Milvus server */
  boolean connected();

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
   * @param tableSchemaParam the <code>TableSchemaParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
   *                                          .withIndexFileSize(1024)
   *                                          .withMetricType(MetricType.IP)
   *                                          .build();
   * TableSchemaParam tableSchemaParam = new TableSchemaParam.Builder(tableSchema)
   *                                                         .withTimeout(timeout)
   *                                                         .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see TableSchema
   * @see TableSchemaParam
   * @see MetricType
   * @see Response
   */
  Response createTable(TableSchemaParam tableSchemaParam);

  /**
   * Check whether the table specified by <code>tableParam</code> exists
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>HasTableResponse</code>
   * @see TableParam
   * @see HasTableResponse
   * @see Response
   */
  HasTableResponse hasTable(TableParam tableParam);

  /**
   * Drops the table specified by <code>tableParam</code>
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see TableParam
   * @see Response
   */
  Response dropTable(TableParam tableParam);

  /**
   * Creates index specified by <code>indexParam</code>
   *
   * @param createIndexParam the <code>CreateIndexParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * Index index = new Index.Builder()
   *                        .withIndexType(IndexType.IVF_SQ8)
   *                        .withNList(16384)
   *                        .build();
   * CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName)
   *                                                         .withIndex(index)
   *                                                         .withTimeout(timeout)
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
   *     <pre>
   * example usage:
   * <code>
   * InsertParam insertParam = new InsertParam.Builder(tableName, vectors)
   *                                          .withVectorIds(vectorIds)
   *                                          .withTimeout(timeout)
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
   * SearchParam searchParam = new SearchParam.Builder(tableName, vectorsToSearch)
   *                                          .withTopK(topK)
   *                                          .withNProbe(nProbe)
   *                                          .withDateRanges(dateRanges)
   *                                          .withTimeout(timeout)
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
   *     <pre>
   * example usage:
   * <code>
   * SearchParam searchParam = new SearchParam.Builder(tableName, vectorsToSearch)
   *                                          .withTopK(topK)
   *                                          .withNProbe(nProbe)
   *                                          .withDateRanges(dateRanges)
   *                                          .build();
   * SearchInFilesParam searchInFilesParam = new SearchInFilesParam.Builder(fileIds, searchParam)
   *                                                               .withTimeout(timeout)
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
   * Describes table specified by <code>tableParam</code>
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>DescribeTableResponse</code>
   * @see TableParam
   * @see DescribeTableResponse
   * @see Response
   */
  DescribeTableResponse describeTable(TableParam tableParam);

  /**
   * Shows current tables
   *
   * @return <code>ShowTablesResponse</code>
   * @see ShowTablesResponse
   * @see Response
   */
  ShowTablesResponse showTables();

  /**
   * Gets current row count of table specified by <code>tableParam</code>
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>GetTableRowCountResponse</code>
   * @see GetTableRowCountResponse
   * @see Response
   */
  GetTableRowCountResponse getTableRowCount(TableParam tableParam);

  /**
   * Prints server status
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response serverStatus();

  /**
   * Prints server version
   *
   * @return <code>Response</code>
   * @see Response
   */
  Response serverVersion();

  /**
   * Deletes vectors by date range, specified by <code>deleteByRangeParam</code>
   *
   * @param deleteByRangeParam the <code>DeleteByRangeParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * DeleteByRangeParam deleteByRangeParam = new DeleteByRangeParam.Builder(dateRange, tableName)
   *                                                               .withTimeout(timeout)
   *                                                               .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see DeleteByRangeParam
   * @see DateRange
   * @see Response
   */
  Response deleteByRange(DeleteByRangeParam deleteByRangeParam);

  /**
   * Pre-loads table to memory
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>Response</code>
   * @see TableParam
   * @see Response
   */
  Response preloadTable(TableParam tableParam);

  /**
   * Describes table index specified by <code>tableParam</code>
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>DescribeIndexResponse</code>
   * @see TableParam
   * @see DescribeIndexResponse
   * @see Index
   * @see Response
   */
  DescribeIndexResponse describeIndex(TableParam tableParam);

  /**
   * Drops table index specified by <code>tableParam</code>
   *
   * @param tableParam the <code>TableParam</code> object
   *     <pre>
   * example usage:
   * <code>
   * TableParam tableParam = new TableParam.Builder(tableName)
   *                                       .withTimeout(timeout)
   *                                       .build();
   * </code>
   * </pre>
   *
   * @return <code>dropIndex</code>
   * @see TableParam
   * @see Response
   */
  Response dropIndex(TableParam tableParam);
}

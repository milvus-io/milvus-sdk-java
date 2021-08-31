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

import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.DeleteParam;
import io.milvus.param.InsertParam;
import io.milvus.param.R;
import io.milvus.param.SearchParam;

import java.util.List;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.GetIndexBuildProgressResponse;
import io.milvus.grpc.GetIndexStateResponse;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.collection.ShowCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexBuildProgressParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.HasPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
import io.milvus.param.partition.ShowPartitionParam;

import java.util.concurrent.TimeUnit;

/** The Milvus Client Interface */
public interface MilvusClient {

  default void close() {
    close(TimeUnit.MINUTES.toSeconds(1));
  }

  void close(long maxWaitSeconds);

  R<MutationResult> insert(InsertParam insertParam);

  R<FlushResponse> flush(String collectionName,String dbName);

  R<FlushResponse> flush(String collectionName);

  R<FlushResponse> flush(List<String> collectionNames);

  R<FlushResponse> flush(List<String> collectionNames, String dbName);

  R<MutationResult> delete(DeleteParam deleteParam);

  R<SearchResults> search(SearchParam searchParam);

    /**
     * Check if a collection exists.
     *
     * @param requestParam {@link HasCollectionParam}
     * @return {status:result code,data: boolean, whether if has collection or not}
     */
    R<Boolean> hasCollection(HasCollectionParam requestParam);

    /**
     * Create a collection in Milvus.
     *
     * @param requestParam {@link CreateCollectionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createCollection(CreateCollectionParam requestParam);

    /**
     * Drop a collection. Note that this drops all data in the collection.
     *
     * @param requestParam {@link DropCollectionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropCollection(DropCollectionParam requestParam);

    /**
     * Load collection to cache before search.
     *
     * @param requestParam {@link LoadCollectionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadCollection(LoadCollectionParam requestParam);

    /**
     * Release a collection from cache to reduce cache usage. Note that you cannot
     * search while the corresponding collection is unloaded.
     *
     * @param requestParam {@link ReleaseCollectionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam);

    /**
     * Show the details of a collection, e.g. name, schema.
     *
     * @param requestParam {@link DescribeCollectionParam}
     * @return {status:result code,data:DescribeCollectionResponse{schema,collectionID}}
     */
    R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam);

    /**
     * Show the statistics information of a collection.
     *
     * @param requestParam {@link GetCollectionStatisticsParam}
     * @return {status:result code, data: GetCollectionStatisticsResponse{status,stats}}
     */
    R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam);

    /**
     * List all collections or get collection loading status.
     *
     * @param requestParam {@link ShowCollectionParam}
     * @return {status:result code, data: ShowCollectionsResponse{status,collection_names,collection_ids,created_timestamps,created_utc_timestamps}}
     */
    R<ShowCollectionsResponse> showCollections(ShowCollectionParam requestParam);

    /**
     * Create a partition in a collection.
     *
     * @param requestParam {@link CreatePartitionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createPartition(CreatePartitionParam requestParam);

    /**
     * To drop a partition will drop all data in this partition and the _default partition cannot be dropped.
     *
     * @param requestParam {@link DropPartitionParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropPartition(DropPartitionParam requestParam);

    /**
     * Check if a partition exists in a collection.
     *
     * @param requestParam {@link HasPartitionParam}
     * @return {status:result code,data: boolean, whether if has collection or not}
     */
    R<Boolean> hasPartition(HasPartitionParam requestParam);

    /**
     * Load a partition into cache.
     *
     * @param requestParam {@link LoadPartitionsParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam);

    /**
     * Release a partition from cache.
     *
     * @param requestParam {@link ReleasePartitionsParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam);

    /**
     * Show the statistics information of a partition.
     *
     * @param requestParam {@link GetPartitionStatisticsParam}
     * @return  {status:result code,data:GetPartitionStatisticsResponse{status,stats}}
     */
    R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam);

    /**
     * Show all partitions in a collection.
     *
     * @param requestParam {@link ShowPartitionParam}
     * @return {status:result code,data:ShowPartitionsResponse{partition_names,partitionIDs,created_timestamps,created_utc_timestamps}}
     */
    R<ShowPartitionsResponse> showPartitions(ShowPartitionParam requestParam);

    /**
     * Create an index on a vector field. Note that index building is an async progress.
     *
     * @param requestParam {@link CreateIndexParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createIndex(CreateIndexParam requestParam);

    /**
     * Drop an index.
     *
     * @param requestParam {@link DropIndexParam}
     * @return {status:result code,data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropIndex(DropIndexParam requestParam);

    /**
     * Show index information. Current release of Milvus only supports showing latest built index.
     *
     * @param requestParam {@link DescribeIndexParam}
     * @return {status:result code,data:DescribeIndexResponse{status,index_descriptions}}
     */
    R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam);

    /**
     * Show index building state.
     *
     * @param requestParam {@link GetIndexStateParam}
     * @return {status:result code,data:GetIndexStateResponse{status,state}}
     */
    R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam);

    /**
     * Show index building progress.
     *
     * @param requestParam {@link GetIndexBuildProgressParam}
     * @return {status:result code,data:GetIndexStateResponse{status,indexed_rows}}
     */
    R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam);
}

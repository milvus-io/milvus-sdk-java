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

import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;

import java.util.concurrent.TimeUnit;

/** The Milvus Client Interface */
public interface MilvusClient {

    /** Close Milvus client channel, timeout: 1 minute */
    default void close() {
        try {
            close(TimeUnit.MINUTES.toSeconds(1));
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown Milvus client!");
        }
    }

    /**
     * Close Milvus client channel.
     *
     * @param maxWaitSeconds timeout unit: second
     */
    void close(long maxWaitSeconds) throws InterruptedException;

    /**
     * Check if a collection exists.
     *
     * @param requestParam {@link HasCollectionParam}
     * @return {status:result code, data: boolean, whether if has collection or not}
     */
    R<Boolean> hasCollection(HasCollectionParam requestParam);

    /**
     * Create a collection in Milvus.
     *
     * @param requestParam {@link CreateCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createCollection(CreateCollectionParam requestParam);

    /**
     * Drop a collection. Note that this drops all data in the collection.
     *
     * @param requestParam {@link DropCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropCollection(DropCollectionParam requestParam);

    /**
     * Load collection to cache before search/query.
     *
     * @param requestParam {@link LoadCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadCollection(LoadCollectionParam requestParam);

    /**
     * Release a collection from cache to reduce cache usage. Note that you cannot
     * search while the corresponding collection is unloaded.
     *
     * @param requestParam {@link ReleaseCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam);

    /**
     * Show the details of a collection, e.g. name, schema.
     *
     * @param requestParam {@link DescribeCollectionParam}
     * @return {status:result code, data:DescribeCollectionResponse{schema,collectionID}}
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
     * @param requestParam {@link ShowCollectionsParam}
     * @return {status:result code, data: ShowCollectionsResponse{status,collection_names,collection_ids,created_timestamps,created_utc_timestamps}}
     */
    R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam);

//    /**
//     * Flush collections.
//     * Currently we don't allow client call this method since server side has no compaction function
//     *
//     * @param requestParam {@link FlushParam}
//     * @return {status:result code,data: FlushResponse{flush segment ids}}
//     */
//    R<FlushResponse> flush(FlushParam requestParam);

    /**
     * Create a partition in a collection.
     *
     * @param requestParam {@link CreatePartitionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createPartition(CreatePartitionParam requestParam);

    /**
     * To drop a partition will drop all data in this partition and the _default partition cannot be dropped.
     *
     * @param requestParam {@link DropPartitionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropPartition(DropPartitionParam requestParam);

    /**
     * Check if a partition exists in a collection.
     *
     * @param requestParam {@link HasPartitionParam}
     * @return {status:result code, data: boolean, whether if has collection or not}
     */
    R<Boolean> hasPartition(HasPartitionParam requestParam);

    /**
     * Load a partition into cache.
     *
     * @param requestParam {@link LoadPartitionsParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam);

    /**
     * Release a partition from cache.
     *
     * @param requestParam {@link ReleasePartitionsParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam);

    /**
     * Show the statistics information of a partition.
     *
     * @param requestParam {@link GetPartitionStatisticsParam}
     * @return {status:result code,data:GetPartitionStatisticsResponse{status,stats}}
     */
    R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam);

    /**
     * Show all partitions in a collection.
     *
     * @param requestParam {@link ShowPartitionsParam}
     * @return {status:result code, data:ShowPartitionsResponse{partition_names,partitionIDs,created_timestamps,created_utc_timestamps}}
     */
    R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam);

    /**
     * Create an alias for a collection.
     * Alias can be used in search/query to replace collection name
     *
     * @param requestParam {@link CreateAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createAlias(CreateAliasParam requestParam);

    /**
     * Drop an alias.
     *
     * @param requestParam {@link DropAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropAlias(DropAliasParam requestParam);

    /**
     * Alter alias from a collection to another.
     *
     * @param requestParam {@link AlterAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> alterAlias(AlterAliasParam requestParam);

    /**
     * Create an index on a vector field. Note that index building is an async progress.
     *
     * @param requestParam {@link CreateIndexParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createIndex(CreateIndexParam requestParam);

    /**
     * Drop an index.
     *
     * @param requestParam {@link DropIndexParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropIndex(DropIndexParam requestParam);

    /**
     * Show index information. Current release of Milvus only supports showing latest built index.
     *
     * @param requestParam {@link DescribeIndexParam}
     * @return {status:result code, data:DescribeIndexResponse{status,index_descriptions}}
     */
    R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam);

    /**
     * Show index building state(in-progress/finished/failed), failed reason.
     *
     * @param requestParam {@link GetIndexStateParam}
     * @return {status:result code, data:GetIndexStateResponse{status,state}}
     */
    R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam);

    /**
     * Show index building progress, such as how many rows are indexed.
     *
     * @param requestParam {@link GetIndexBuildProgressParam}
     * @return {status:result code, data:GetIndexBuildProgressResponse{status,indexed_rows}}
     */
    R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam);

    /**
     * Insert entities into collection. Note that you don't need to input values for auto-id field.
     *
     * @param requestParam {@link InsertParam}
     * @return {status:result code, data: MutationResult{insert results}}
     */
    R<MutationResult> insert(InsertParam requestParam);

    /**
     * Delete entities by expression. Currently release of Milvus only support expression like "xxx in [1, 2, ...]"
     *
     * @param requestParam {@link DeleteParam}
     * @return {status:result code, data: MutationResult{delete results}}
     */
    R<MutationResult> delete(DeleteParam requestParam);

    /**
     * Do ANN search base on a vector field. Use expression to do filtering before search.
     *
     * @param requestParam {@link SearchParam}
     * @return {status:result code, data: SearchResults{topK results}}
     */
    R<SearchResults> search(SearchParam requestParam);

    /**
     * Query entities by expression. Note that the returned entities sequence cannot be guaranteed.
     *
     * @param requestParam {@link QueryParam}
     * @return {status:result code,data: QueryResults{filter results}}
     */
    R<QueryResults> query(QueryParam requestParam);

    /**
     * Calculate distance between specified vectors.
     *
     * @param requestParam {@link CalcDistanceParam}
     * @return {status:result code, data: CalcDistanceResults{distances}}
     */
    R<CalcDistanceResults> calcDistance(CalcDistanceParam requestParam);

    /**
     * Get runtime metrics information of Milvus, return in json format.
     *
     * @param requestParam {@link GetMetricsParam}
     * @return {status:result code, data:GetMetricsResponse{status,metrics}}
     */
    R<GetMetricsResponse> getMetrics(GetMetricsParam requestParam);

    /**
     * Get information of persistent segments, including row count, persist state(growing or flushed), etc.
     *
     * @param requestParam {@link GetPersistentSegmentInfoParam}
     * @return {status:result code, data:GetPersistentSegmentInfoResponse{status,info}}
     */
    R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(GetPersistentSegmentInfoParam requestParam);

    /**
     * Get query information of segments in a collection, including row count, mem size, index name, etc.
     *
     * @param requestParam {@link GetQuerySegmentInfoParam}
     * @return {status:result code, data:GetQuerySegmentInfoResponse{status,info}}
     */
    R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(GetQuerySegmentInfoParam requestParam);

    /**
     * Move segment from a query node to another, to keep load balance.
     *
     * @param requestParam {@link LoadBalanceParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadBalance(LoadBalanceParam requestParam);

    /**
     * Get compaction action state by id.
     *
     * @param requestParam {@link GetCompactionStateParam}
     * @return {status:result code, data:GetCompactionStateResponse{status,info}}
     */
    R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam);

    /**
     * Ask server to perform a compaction action.
     *
     * @param requestParam {@link ManualCompactionParam}
     * @return {status:result code, data:ManualCompactionResponse{status,info}}
     */
    R<ManualCompactionResponse> manualCompaction(ManualCompactionParam requestParam);

    /**
     * Get compaction action state with its plan.
     *
     * @param requestParam {@link GetCompactionPlansParam}
     * @return {status:result code, data:GetCompactionPlansResponse{status,info}}
     */
    R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam);
}

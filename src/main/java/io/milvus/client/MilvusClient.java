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
import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.*;
import io.milvus.param.bulkinsert.*;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.credential.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.param.role.*;

import java.util.concurrent.TimeUnit;

/**
 * The Milvus Client Interface
 */
public interface MilvusClient {
    /**
     * Timeout setting for rpc call.
     *
     * @param timeout     set time waiting for a rpc call.
     * @param timeoutUnit time unit
     */
    MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit);

    /**
     * Disconnects from a Milvus server with timeout of 1 minute
     */
    default void close() {
        try {
            close(TimeUnit.MINUTES.toSeconds(1));
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown Milvus client!");
        }
    }

    /**
     * Disconnects from a Milvus server with configurable timeout.
     *
     * @param maxWaitSeconds timeout unit: second
     */
    void close(long maxWaitSeconds) throws InterruptedException;

    /**
     * Checks if a collection exists.
     *
     * @param requestParam {@link HasCollectionParam}
     * @return {status:result code, data: boolean, whether if has collection or not}
     */
    R<Boolean> hasCollection(HasCollectionParam requestParam);

    /**
     * Creates a collection in Milvus.
     *
     * @param requestParam {@link CreateCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createCollection(CreateCollectionParam requestParam);

    /**
     * Drops a collection. Note that this method drops all data in the collection.
     *
     * @param requestParam {@link DropCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropCollection(DropCollectionParam requestParam);

    /**
     * Loads a collection to memory before search or query.
     *
     * @param requestParam {@link LoadCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadCollection(LoadCollectionParam requestParam);

    /**
     * Releases a collection from memory to reduce memory usage. Note that you
     * cannot search while the corresponding collection is released from memory.
     *
     * @param requestParam {@link ReleaseCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam);

    /**
     * Shows the details of a collection, e.g. name, schema.
     *
     * @param requestParam {@link DescribeCollectionParam}
     * @return {status:result code, data:DescribeCollectionResponse{schema,collectionID}}
     */
    R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam);

    /**
     * Shows the statistics information of a collection.
     *
     * @param requestParam {@link GetCollectionStatisticsParam}
     * @return {status:result code, data: GetCollectionStatisticsResponse{status,stats}}
     */
    R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam);

    /**
     * Lists all collections or gets collection loading status.
     *
     * @param requestParam {@link ShowCollectionsParam}
     * @return {status:result code, data: ShowCollectionsResponse{status,collection_names,collection_ids,created_timestamps,created_utc_timestamps}}
     */
    R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam);

    /**
     * Alter collection.
     * Currently, only support setting collection TTL with key `collection.ttl.seconds`
     *
     * @param requestParam {@link AlterCollectionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> alterCollection(AlterCollectionParam requestParam);

    /**
     * Flushes inserted data in buffer into storage.
     *
     * @param requestParam {@link FlushParam}
     * @return {status:result code,data: FlushResponse{flush segment ids}}
     */
    R<FlushResponse> flush(FlushParam requestParam);

    /**
     * Flush all collections. All insertions, deletions, and upserts before `flushAll` will be synced.
     *
     * @param syncFlushAll {flushAll synchronously or asynchronously}
     * @param syncFlushAllWaitingInterval {wait intervel when flushAll synchronously}
     * @param syncFlushAllTimeout {timeout when flushAll synchronously}
     * @return {status:result code,data: FlushAllResponse{flushAllTs}}
     */
    R<FlushAllResponse> flushAll(boolean syncFlushAll, long syncFlushAllWaitingInterval, long syncFlushAllTimeout);

    /**
     * Creates a partition in the specified collection.
     *
     * @param requestParam {@link CreatePartitionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createPartition(CreatePartitionParam requestParam);

    /**
     * Drops a partition. Note that this method drops all data in this partition
     * and the _default partition cannot be dropped.
     *
     * @param requestParam {@link DropPartitionParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropPartition(DropPartitionParam requestParam);

    /**
     * Checks if a partition exists in the specified collection.
     *
     * @param requestParam {@link HasPartitionParam}
     * @return {status:result code, data: boolean, whether if has collection or not}
     */
    R<Boolean> hasPartition(HasPartitionParam requestParam);

    /**
     * Loads a partition into memory.
     *
     * @param requestParam {@link LoadPartitionsParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam);

    /**
     * Releases a partition from memory.
     *
     * @param requestParam {@link ReleasePartitionsParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam);

    /**
     * Shows the statistics information of a partition.
     *
     * @param requestParam {@link GetPartitionStatisticsParam}
     * @return {status:result code,data:GetPartitionStatisticsResponse{status,stats}}
     */
    R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam);

    /**
     * Shows all partitions in the specified collection.
     *
     * @param requestParam {@link ShowPartitionsParam}
     * @return {status:result code, data:ShowPartitionsResponse{partition_names,partitionIDs,created_timestamps,created_utc_timestamps}}
     */
    R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam);

    /**
     * Creates an alias for a collection.
     * Alias can be used in search or query to replace the collection name
     *
     * @param requestParam {@link CreateAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createAlias(CreateAliasParam requestParam);

    /**
     * Drops an alias for the specified collection.
     *
     * @param requestParam {@link DropAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropAlias(DropAliasParam requestParam);

    /**
     * Alters alias from a collection to another.
     *
     * @param requestParam {@link AlterAliasParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> alterAlias(AlterAliasParam requestParam);

    /**
     * Creates an index on a vector field in the specified collection.
     * Note that index building is an async progress.
     *
     * @param requestParam {@link CreateIndexParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createIndex(CreateIndexParam requestParam);

    /**
     * Drops the index on a vector field in the specified collection.
     *
     * @param requestParam {@link DropIndexParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropIndex(DropIndexParam requestParam);

    /**
     * Shows the information of the specified index. Current release of Milvus
     * only supports showing latest built index.
     *
     * @param requestParam {@link DescribeIndexParam}
     * @return {status:result code, data:DescribeIndexResponse{status,index_descriptions}}
     */
    R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam);

    /**
     * Shows the index building state(in-progress/finished/failed), and the reason for failure (if any).
     *
     * @param requestParam {@link GetIndexStateParam}
     * @return {status:result code, data:GetIndexStateResponse{status,state}}
     */
    R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam);

    /**
     * Shows the index building progress, such as how many rows are indexed.
     *
     * @param requestParam {@link GetIndexBuildProgressParam}
     * @return {status:result code, data:GetIndexBuildProgressResponse{status,indexed_rows}}
     */
    R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam);

    /**
     * Inserts entities into a specified collection . Note that you don't need to
     * input primary key field if auto_id is enabled.
     *
     * @param requestParam {@link InsertParam}
     * @return {status:result code, data: MutationResult{insert results}}
     */
    R<MutationResult> insert(InsertParam requestParam);

    /**
     * Inserts entities into a specified collection asynchronously. Note that you don't need to
     * input primary key field if auto_id is enabled.
     *
     * @param requestParam {@link InsertParam}
     * @return a <code>ListenableFuture</code> object which holds the object {status:result code, data: MutationResult{insert results}}
     */
    ListenableFuture<R<MutationResult>> insertAsync(InsertParam requestParam);

    /**
     * Deletes entity(s) based on primary key(s) filtered by boolean expression. Current release
     * of Milvus only supports expression in the format "pk_field in [1, 2, ...]"
     *
     * @param requestParam {@link DeleteParam}
     * @return {status:result code, data: MutationResult{delete results}}
     */
    R<MutationResult> delete(DeleteParam requestParam);

    /**
     * Conducts ANN search on a vector field. Use expression to do filtering before search.
     *
     * @param requestParam {@link SearchParam}
     * @return {status:result code, data: SearchResults{topK results}}
     */
    R<SearchResults> search(SearchParam requestParam);

    /**
     * Conducts ANN search on a vector field asynchronously. Use expression to do filtering before search.
     *
     * @param requestParam {@link SearchParam}
     * @return a <code>ListenableFuture</code> object which holds the object {status:result code, data: SearchResults{topK results}}
     */
    ListenableFuture<R<SearchResults>> searchAsync(SearchParam requestParam);

    /**
     * Queries entity(s) based on scalar field(s) filtered by boolean expression.
     * Note that the order of the returned entities cannot be guaranteed.
     *
     * @param requestParam {@link QueryParam}
     * @return {status:result code,data: QueryResults{filter results}}
     */
    R<QueryResults> query(QueryParam requestParam);

    /**
     * Queries entity(s) asynchronously based on scalar field(s) filtered by boolean expression.
     * Note that the order of the returned entities cannot be guaranteed.
     *
     * @param requestParam {@link QueryParam}
     * @return {status:result code,data: QueryResults{filter results}}
     */
    ListenableFuture<R<QueryResults>> queryAsync(QueryParam requestParam);

    /**
     * Gets the runtime metrics information of Milvus, returns the result in .json format.
     *
     * @param requestParam {@link GetMetricsParam}
     * @return {status:result code, data:GetMetricsResponse{status,metrics}}
     */
    R<GetMetricsResponse> getMetrics(GetMetricsParam requestParam);

    /**
     * Get flush state of specified segments.
     *
     * @param requestParam {@link GetFlushStateParam}
     * @return {status:result code, data:GetMetricsResponse{status,metrics}}
     */
    R<GetFlushStateResponse> getFlushState(GetFlushStateParam requestParam);

    /**
     * Gets the information of persistent segments from data node, including row count,
     * persistence state(growing or flushed), etc.
     *
     * @param requestParam {@link GetPersistentSegmentInfoParam}
     * @return {status:result code, data:GetPersistentSegmentInfoResponse{status,info}}
     */
    R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(GetPersistentSegmentInfoParam requestParam);

    /**
     * Gets the query information of segments in a collection from query node, including row count,
     * memory usage size, index name, etc.
     *
     * @param requestParam {@link GetQuerySegmentInfoParam}
     * @return {status:result code, data:GetQuerySegmentInfoResponse{status,info}}
     */
    R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(GetQuerySegmentInfoParam requestParam);

    /**
     * Returns the collection's replica information
     *
     * @param requestParam {@link GetReplicasParam}
     * @return {status:result code, data:GetReplicasResponse{status,info}}
     */
    R<GetReplicasResponse> getReplicas(GetReplicasParam requestParam);

    /**
     * Moves segment from a query node to another to keep the load balanced.
     *
     * @param requestParam {@link LoadBalanceParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> loadBalance(LoadBalanceParam requestParam);

    /**
     * Gets the compaction state by id.
     *
     * @param requestParam {@link GetCompactionStateParam}
     * @return {status:result code, data:GetCompactionStateResponse{status,info}}
     */
    R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam);

    /**
     * Performs a manual compaction.
     *
     * @param requestParam {@link ManualCompactParam}
     * @return {status:result code, data:ManualCompactionResponse{status,info}}
     */
    R<ManualCompactionResponse> manualCompact(ManualCompactParam requestParam);

    /**
     * Gets compaction state with its plan.
     *
     * @param requestParam {@link GetCompactionPlansParam}
     * @return {status:result code, data:GetCompactionPlansResponse{status,info}}
     */
    R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam);

    /**
     * Create credential using the given user and password.
     *
     * @param requestParam {@link CreateCredentialParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createCredential(CreateCredentialParam requestParam);

    /**
     * Update credential using the given user and password.
     * You must provide the original password to check if the operation is valid.
     * Note: after this operation, client won't change the related header of this connection.
     * So if you update credential for this connection, the connection may be invalid.
     *
     * @param requestParam {@link UpdateCredentialParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> updateCredential(UpdateCredentialParam requestParam);

    /**
     * Delete credential corresponding to the user.
     *
     * @param requestParam {@link DeleteCredentialParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> deleteCredential(DeleteCredentialParam requestParam);

    /**
     * List all user names.
     *
     * @param requestParam {@link ListCredUsersParam}
     * @return {status:result code, data:ListCredUsersResponse{status,info}}
     */
    R<ListCredUsersResponse> listCredUsers(ListCredUsersParam requestParam);


    /**
     * It will success if the role isn't existed, otherwise fail.
     *
     * @param requestParam {@link CreateRoleParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> createRole(CreateRoleParam requestParam);


    /**
     * It will success if the role is existed, otherwise fail.
     *
     * @param requestParam {@link DropRoleParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> dropRole(DropRoleParam requestParam);


    /**
     * The user will get permissions that the role are allowed to perform operations.
     *
     * @param requestParam {@link AddUserToRoleParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> addUserToRole(AddUserToRoleParam requestParam);


    /**
     * The user will remove permissions that the role are allowed to perform operations.
     *
     * @param requestParam {@link AddUserToRoleParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> removeUserFromRole(RemoveUserFromRoleParam requestParam);


    /**
     * Get all users who are added to the role.
     *
     * @param requestParam {@link SelectRoleParam}
     * @return {status:result code, data:SelectRoleResponse{status,info}}
     */
    R<SelectRoleResponse> selectRole(SelectRoleParam requestParam);


    /**
     * Get all roles the user has.
     *
     * @param requestParam {@link SelectUserParam}
     * @return {status:result code, data:SelectUserResponse{status,info}}
     */
    R<SelectUserResponse> selectUser(SelectUserParam requestParam);


    /**
     * Grant Role Privilege.
     *
     * @param requestParam {@link GrantRolePrivilegeParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam);


    /**
     * Revoke Role Privilege.
     *
     * @param requestParam {@link RevokeRolePrivilegeParam}
     * @return {status:result code, data:RpcStatus{msg: result message}}
     */
    R<RpcStatus> revokeRolePrivilege(RevokeRolePrivilegeParam requestParam);


    /**
     * List a grant info for the role and the specific object
     *
     * @param requestParam {@link SelectGrantForRoleParam}
     * @return {status:result code, data:SelectRoleResponse{status,info}}
     */
    R<SelectGrantResponse> selectGrantForRole(SelectGrantForRoleParam requestParam);


    /**
     * List a grant info for the role
     *
     * @param requestParam {@link SelectGrantForRoleAndObjectParam}
     * @return {status:result code, data:SelectRoleResponse{status,info}}
     */
    R<SelectGrantResponse> selectGrantForRoleAndObject(SelectGrantForRoleAndObjectParam requestParam);

    /**
     * Import data from external files, currently support JSON format
     *
     * @param requestParam {@link BulkInsertParam}
     * @return {status:result code, data:ImportResponse{status,info}}
     */
    R<ImportResponse> bulkInsert(BulkInsertParam requestParam);

    /**
     * Get state of bulk insert task
     *
     * @param requestParam {@link GetBulkInsertStateParam}
     * @return {status:result code, data:GetImportStateResponse{status,info}}
     */
    R<GetImportStateResponse> getBulkInsertState(GetBulkInsertStateParam requestParam);

    /**
     * List bulk insert tasks
     *
     * @param requestParam {@link ListBulkInsertTasksParam}
     * @return {status:result code, data:ListImportTasksResponse{status,info}}
     */
    R<ListImportTasksResponse> listBulkInsertTasks(ListBulkInsertTasksParam requestParam);

    /**
     * Check server health
     *
     * @return {status:result code, data:CheckHealthResponse{status,info}}
     */
    R<CheckHealthResponse> checkHealth();


    /**
     * Get server version
     *
     * @return {status:result code, data:GetVersionResponse{status,info}}
     */
    R<GetVersionResponse> getVersion();

    /**
     * Get collection loading progress
     *
     * @param requestParam {@link GetLoadingProgressParam}
     * @return {status:result code, data:GetLoadingProgressResponse{status}}
     */
    R<GetLoadingProgressResponse> getLoadingProgress(GetLoadingProgressParam requestParam);

    /**
     * Get collection loading state
     *
     * @param requestParam {@link GetLoadStateParam}
     * @return {status:result code, data:GetLoadStateResponse{status}}
     */
    R<GetLoadStateResponse> getLoadState(GetLoadStateParam requestParam);
}

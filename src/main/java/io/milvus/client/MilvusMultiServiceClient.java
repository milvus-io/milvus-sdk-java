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
import io.milvus.connection.ClusterFactory;
import io.milvus.connection.ServerSetting;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.alias.*;
import io.milvus.param.bulkinsert.*;
import io.milvus.param.collection.*;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.control.*;
import io.milvus.param.credential.*;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.dml.*;
import io.milvus.param.highlevel.dml.response.*;
import io.milvus.param.index.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.partition.*;
import io.milvus.param.resourcegroup.*;
import io.milvus.param.role.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MilvusMultiServiceClient implements MilvusClient {

    private final ClusterFactory clusterFactory;

    /**
     * Sets connect param for multi milvus clusters.
     * @param multiConnectParam multi server connect param
     */
    public MilvusMultiServiceClient(@NonNull MultiConnectParam multiConnectParam) {

        List<ServerSetting> serverSettings = multiConnectParam.getHosts().stream()
                .map(host -> {

                    MilvusClient milvusClient = buildMilvusClient(host, multiConnectParam);

                    return ServerSetting.newBuilder()
                            .withHost(host)
                            .withMilvusClient(milvusClient).build();

                }).collect(Collectors.toList());

        boolean keepMonitor = serverSettings.size() > 1;

        this.clusterFactory = ClusterFactory.newBuilder()
                .withServerSetting(serverSettings)
                .keepMonitor(keepMonitor)
                .withQueryNodeSingleSearch(multiConnectParam.getQueryNodeSingleSearch())
                .build();
    }

    private MilvusClient buildMilvusClient(ServerAddress host, MultiConnectParam multiConnectParam) {
        long connectTimeoutMsm = multiConnectParam.getConnectTimeoutMs();
        long keepAliveTimeMs = multiConnectParam.getKeepAliveTimeMs();
        long keepAliveTimeoutMs = multiConnectParam.getKeepAliveTimeoutMs();
        boolean keepAliveWithoutCalls = multiConnectParam.isKeepAliveWithoutCalls();
        boolean secure = multiConnectParam.isSecure();
        long idleTimeoutMs = multiConnectParam.getIdleTimeoutMs();

        ConnectParam clusterConnectParam = ConnectParam.newBuilder()
                .withHost(host.getHost())
                .withPort(host.getPort())
                .withConnectTimeout(connectTimeoutMsm, TimeUnit.MILLISECONDS)
                .withKeepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS)
                .withKeepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(keepAliveWithoutCalls)
                .withSecure(secure)
                .withIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS)
                .withAuthorization(multiConnectParam.getAuthorization())
                .withDatabaseName(multiConnectParam.getDatabaseName())
                .build();
        return new MilvusServiceClient(clusterConnectParam);
    }

    @Override
    public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
        return clusterFactory.getMaster().getClient().withTimeout(timeout, timeoutUnit);
    }

    @Override
    public MilvusClient withRetry(RetryParam retryParam) {
        return clusterFactory.getMaster().getClient().withRetry(retryParam);
    }

    @Override
    public MilvusClient withRetry(int retryTimes) {
        return clusterFactory.getMaster().getClient().withRetry(retryTimes);
    }

    @Override
    public MilvusClient withRetryInterval(long interval, TimeUnit timeUnit) {
        return clusterFactory.getMaster().getClient().withRetryInterval(interval, timeUnit);
    }

    @Override
    public void close(long maxWaitSeconds) throws InterruptedException {
        this.clusterFactory.getAvailableServerSettings().parallelStream()
                .forEach(serverSetting -> serverSetting.getClient().close());
        this.clusterFactory.close();
    }

    @Override
    public void setLogLevel(LogLevel level) {
        this.clusterFactory.getAvailableServerSettings().parallelStream()
                .forEach(serverSetting -> serverSetting.getClient().setLogLevel(level));
    }

    @Override
    public R<Boolean> hasCollection(HasCollectionParam requestParam) {
        return this.clusterFactory.getMaster().getClient().hasCollection(requestParam);
    }

    @Override
    public R<RpcStatus> createDatabase(CreateDatabaseParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().createDatabase(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> dropDatabase(DropDatabaseParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().dropDatabase(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<ListDatabasesResponse> listDatabases() {
        List<R<ListDatabasesResponse>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().listDatabases())
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> createCollection(CreateCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().createCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> dropCollection(DropCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().dropCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> loadCollection(LoadCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().loadCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().releaseCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> renameCollection(RenameCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().renameCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam) {
        return this.clusterFactory.getMaster().getClient().describeCollection(requestParam);
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getCollectionStatistics(requestParam);
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().showCollections(requestParam);
    }

    @Override
    public R<RpcStatus> alterCollection(AlterCollectionParam requestParam) {
        return this.clusterFactory.getMaster().getClient().alterCollection(requestParam);
    }

    @Override
    public R<FlushResponse> flush(FlushParam requestParam) {
        List<R<FlushResponse>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().flush(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<FlushAllResponse> flushAll(boolean syncFlushAll, long syncFlushAllWaitingInterval, long syncFlushAllTimeout) {
        List<R<FlushAllResponse>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().flushAll(syncFlushAll, syncFlushAllWaitingInterval, syncFlushAllTimeout))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> createPartition(CreatePartitionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().createPartition(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> dropPartition(DropPartitionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().dropPartition(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<Boolean> hasPartition(HasPartitionParam requestParam) {
        return this.clusterFactory.getMaster().getClient().hasPartition(requestParam);
    }

    @Override
    public R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().loadPartitions(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().releasePartitions(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getPartitionStatistics(requestParam);
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().showPartitions(requestParam);
    }

    @Override
    public R<RpcStatus> createAlias(CreateAliasParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().createAlias(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> dropAlias(DropAliasParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().dropAlias(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> alterAlias(AlterAliasParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().alterAlias(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<ListAliasesResponse> listAliases(ListAliasesParam requestParam) {
        List<R<ListAliasesResponse>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().listAliases(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> createIndex(CreateIndexParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().createIndex(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<RpcStatus> dropIndex(DropIndexParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().dropIndex(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam) {
        return this.clusterFactory.getMaster().getClient().describeIndex(requestParam);
    }

    @Override
    public R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getIndexState(requestParam);
    }

    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getIndexBuildProgress(requestParam);
    }

    @Override
    public R<RpcStatus> alterIndex(AlterIndexParam requestParam) {
        return this.clusterFactory.getMaster().getClient().alterIndex(requestParam);
    }

    @Override
    public R<MutationResult> insert(InsertParam requestParam) {
        List<R<MutationResult>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().insert(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public ListenableFuture<R<MutationResult>> insertAsync(InsertParam requestParam) {
        List<ListenableFuture<R<MutationResult>>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().insertAsync(requestParam))
                .collect(Collectors.toList());
        return response.get(0);
    }

    @Override
    public R<MutationResult> upsert(UpsertParam requestParam) {
        List<R<MutationResult>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().upsert(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public ListenableFuture<R<MutationResult>> upsertAsync(UpsertParam requestParam) {
        List<ListenableFuture<R<MutationResult>>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().upsertAsync(requestParam))
                .collect(Collectors.toList());
        return response.get(0);
    }

    @Override
    public R<MutationResult> delete(DeleteParam requestParam) {
        List<R<MutationResult>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().delete(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

//    @Override
//    public R<ImportResponse> bulkload(@NonNull BulkloadParam requestParam) {
//        List<R<ImportResponse>> response = this.clusterFactory.getAvailableServerSettings().stream()
//                .map(serverSetting -> serverSetting.getClient().bulkload(requestParam))
//                .collect(Collectors.toList());
//        return handleResponse(response);
//    }
//
//    @Override
//    public R<GetImportStateResponse> getBulkloadState(GetBulkloadStateParam requestParam) {
//        return this.clusterFactory.getMaster().getClient().getBulkloadState(requestParam);
//    }
//
//    @Override
//    public R<ListImportTasksResponse> listBulkloadTasks(@NonNull ListBulkloadTasksParam requestParam) {
//        return this.clusterFactory.getMaster().getClient().listBulkloadTasks(requestParam);
//    }

    @Override
    public R<SearchResults> search(SearchParam requestParam) {
        return this.clusterFactory.getMaster().getClient().search(requestParam);
    }

    @Override
    public ListenableFuture<R<SearchResults>> searchAsync(SearchParam requestParam) {
        return this.clusterFactory.getMaster().getClient().searchAsync(requestParam);
    }

    @Override
    public R<SearchResults> hybridSearch(HybridSearchParam requestParam) {
        return this.clusterFactory.getMaster().getClient().hybridSearch(requestParam);
    }

    @Override
    public ListenableFuture<R<SearchResults>> hybridSearchAsync(HybridSearchParam requestParam) {
        return this.clusterFactory.getMaster().getClient().hybridSearchAsync(requestParam);
    }

    @Override
    public R<QueryResults> query(QueryParam requestParam) {
        return this.clusterFactory.getMaster().getClient().query(requestParam);
    }

    @Override
    public ListenableFuture<R<QueryResults>> queryAsync(QueryParam requestParam) {
        return this.clusterFactory.getMaster().getClient().queryAsync(requestParam);
    }

    @Override
    public R<GetMetricsResponse> getMetrics(GetMetricsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getMetrics(requestParam);
    }

    @Override
    public R<GetFlushStateResponse> getFlushState(GetFlushStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getFlushState(requestParam);
    }

    @Override
    public R<GetFlushAllStateResponse> getFlushAllState(GetFlushAllStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getFlushAllState(requestParam);
    }

    @Override
    public R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(GetPersistentSegmentInfoParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getPersistentSegmentInfo(requestParam);
    }

    @Override
    public R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(GetQuerySegmentInfoParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getQuerySegmentInfo(requestParam);
    }

    @Override
    public R<GetReplicasResponse> getReplicas(GetReplicasParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getReplicas(requestParam);
    }

    @Override
    public R<RpcStatus> loadBalance(LoadBalanceParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().loadBalance(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getCompactionState(requestParam);
    }

    @Override
    public R<ManualCompactionResponse> manualCompact(ManualCompactParam requestParam) {
        return null;
    }

    @Override
    public R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getCompactionStateWithPlans(requestParam);
    }

    @Override
    public R<RpcStatus> createCredential(CreateCredentialParam requestParam) {
        return this.clusterFactory.getMaster().getClient().createCredential(requestParam);
    }

    @Override
    public R<RpcStatus> updateCredential(UpdateCredentialParam requestParam) {
        return this.clusterFactory.getMaster().getClient().updateCredential(requestParam);
    }

    @Override
    public R<RpcStatus> deleteCredential(DeleteCredentialParam requestParam) {
        return this.clusterFactory.getMaster().getClient().deleteCredential(requestParam);
    }

    @Override
    public R<ListCredUsersResponse> listCredUsers(ListCredUsersParam requestParam) {
        return this.clusterFactory.getMaster().getClient().listCredUsers(requestParam);
    }

    @Override
    public R<RpcStatus> createRole(CreateRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().createRole(requestParam);
    }

    @Override
    public R<RpcStatus> dropRole(DropRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().dropRole(requestParam);
    }

    @Override
    public R<RpcStatus> addUserToRole(AddUserToRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().addUserToRole(requestParam);
    }

    @Override
    public R<RpcStatus> removeUserFromRole(RemoveUserFromRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().removeUserFromRole(requestParam);
    }

    @Override
    public R<SelectRoleResponse> selectRole(SelectRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().selectRole(requestParam);
    }

    @Override
    public R<SelectUserResponse> selectUser(SelectUserParam requestParam) {
        return this.clusterFactory.getMaster().getClient().selectUser(requestParam);
    }

    @Override
    public R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam) {
        return this.clusterFactory.getMaster().getClient().grantRolePrivilege(requestParam);
    }

    @Override
    public R<RpcStatus> revokeRolePrivilege(RevokeRolePrivilegeParam requestParam) {
        return this.clusterFactory.getMaster().getClient().revokeRolePrivilege(requestParam);
    }

    @Override
    public R<SelectGrantResponse> selectGrantForRole(SelectGrantForRoleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().selectGrantForRole(requestParam);
    }

    @Override
    public R<SelectGrantResponse> selectGrantForRoleAndObject(SelectGrantForRoleAndObjectParam requestParam) {
        return this.clusterFactory.getMaster().getClient().selectGrantForRoleAndObject(requestParam);
    }

    @Override
    public R<ImportResponse> bulkInsert(BulkInsertParam requestParam) {
        return this.clusterFactory.getMaster().getClient().bulkInsert(requestParam);
    }

    @Override
    public R<GetImportStateResponse> getBulkInsertState(GetBulkInsertStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getBulkInsertState(requestParam);
    }

    @Override
    public R<ListImportTasksResponse> listBulkInsertTasks(ListBulkInsertTasksParam requestParam) {
        return this.clusterFactory.getMaster().getClient().listBulkInsertTasks(requestParam);
    }

    public R<CheckHealthResponse> checkHealth(){
        return this.clusterFactory.getMaster().getClient().checkHealth();
    }

    public R<GetVersionResponse> getVersion() {
        return this.clusterFactory.getMaster().getClient().getVersion();
    }

    @Override
    public R<GetLoadingProgressResponse> getLoadingProgress(GetLoadingProgressParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getLoadingProgress(requestParam);
    }

    @Override
    public R<GetLoadStateResponse> getLoadState(GetLoadStateParam requestParam) {
        return this.clusterFactory.getMaster().getClient().getLoadState(requestParam);
    }

    @Override
    public R<RpcStatus> createResourceGroup(CreateResourceGroupParam requestParam) {
        return this.clusterFactory.getMaster().getClient().createResourceGroup(requestParam);
    }

    @Override
    public R<RpcStatus> updateResourceGroups(UpdateResourceGroupsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().updateResourceGroups(requestParam);
    }

    @Override
    public R<RpcStatus> dropResourceGroup(DropResourceGroupParam requestParam) {
        return this.clusterFactory.getMaster().getClient().dropResourceGroup(requestParam);
    }

    @Override
    public R<ListResourceGroupsResponse> listResourceGroups(ListResourceGroupsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().listResourceGroups(requestParam);
    }

    @Override
    public R<DescribeResourceGroupResponse> describeResourceGroup(DescribeResourceGroupParam requestParam) {
        return this.clusterFactory.getMaster().getClient().describeResourceGroup(requestParam);
    }

    @Override
    public R<RpcStatus> transferNode(TransferNodeParam requestParam) {
        return this.clusterFactory.getMaster().getClient().transferNode(requestParam);
    }

    @Override
    public R<RpcStatus> transferReplica(TransferReplicaParam requestParam) {
        return this.clusterFactory.getMaster().getClient().transferReplica(requestParam);
    }

    ///////////////////// High Level API//////////////////////


    @Override
    public R<RpcStatus> createCollection(CreateSimpleCollectionParam requestParam) {
        List<R<RpcStatus>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().createCollection(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<ListCollectionsResponse> listCollections(ListCollectionsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().listCollections(requestParam);
    }

    @Override
    public R<InsertResponse> insert(InsertRowsParam requestParam) {
        List<R<InsertResponse>> response = this.clusterFactory.getAvailableServerSettings().parallelStream()
                .map(serverSetting -> serverSetting.getClient().insert(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<DeleteResponse> delete(DeleteIdsParam requestParam) {
        List<R<DeleteResponse>> response = this.clusterFactory.getAvailableServerSettings().stream()
                .map(serverSetting -> serverSetting.getClient().delete(requestParam))
                .collect(Collectors.toList());
        return handleResponse(response);
    }

    @Override
    public R<GetResponse> get(GetIdsParam requestParam) {
        return this.clusterFactory.getMaster().getClient().get(requestParam);
    }

    @Override
    public R<QueryResponse> query(QuerySimpleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().query(requestParam);
    }

    @Override
    public R<SearchResponse> search(SearchSimpleParam requestParam) {
        return this.clusterFactory.getMaster().getClient().search(requestParam);
    }

    @Override
    public R<QueryIterator> queryIterator(QueryIteratorParam requestParam) {
        return this.clusterFactory.getMaster().getClient().queryIterator(requestParam);
    }

    @Override
    public R<SearchIterator> searchIterator(SearchIteratorParam requestParam) {
        return this.clusterFactory.getMaster().getClient().searchIterator(requestParam);
    }

    private <T> R<T> handleResponse(List<R<T>> response) {
        if (CollectionUtils.isNotEmpty(response)) {
            R<T> rSuccess = null;
            for (R<T> singleRes : response) {
                if (R.Status.Success.getCode() == singleRes.getStatus()) {
                    rSuccess = singleRes;
                } else {
                    return singleRes;
                }
            }
            if (null != rSuccess) {
                return rSuccess;
            }
        }
        return R.failed(R.Status.Unknown, "Response is empty.");
    }
}


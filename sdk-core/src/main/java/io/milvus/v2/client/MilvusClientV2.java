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

package io.milvus.v2.client;

import io.grpc.ManagedChannel;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;

import io.milvus.v2.service.database.DatabaseService;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.database.response.*;
import io.milvus.v2.service.collection.CollectionService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.*;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.*;
import io.milvus.v2.service.partition.PartitionService;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.partition.response.*;
import io.milvus.v2.service.rbac.RBACService;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.*;
import io.milvus.v2.service.resourcegroup.ResourceGroupService;
import io.milvus.v2.service.resourcegroup.request.*;
import io.milvus.v2.service.resourcegroup.response.*;
import io.milvus.v2.service.utility.UtilityService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.ClientUtils;
import io.milvus.v2.utils.RpcUtils;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class MilvusClientV2 {
    private static final Logger logger = LoggerFactory.getLogger(MilvusClientV2.class);
    private ManagedChannel channel;
    @Setter
    private MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final ClientUtils clientUtils = new ClientUtils();
    private final DatabaseService databaseService = new DatabaseService();
    private final CollectionService collectionService = new CollectionService();
    private final IndexService indexService = new IndexService();
    private final VectorService vectorService = new VectorService();
    private final PartitionService partitionService = new PartitionService();
    private final RBACService rbacService = new RBACService();
    private final ResourceGroupService rgroupService = new ResourceGroupService();
    private final UtilityService utilityService = new UtilityService();
    private RpcUtils rpcUtils = new RpcUtils();
    private ConnectConfig connectConfig;

    /**
     * Creates a Milvus client instance.
     * @param connectConfig Milvus server connection configuration
     */
    public MilvusClientV2(ConnectConfig connectConfig) {
        if (connectConfig != null) {
            connect(connectConfig);
        }
    }
    /**
     * connect to Milvus server
     *
     * @param connectConfig Milvus server connection configuration
     */
    private void connect(ConnectConfig connectConfig){
        this.connectConfig = connectConfig;
        try {
            if(this.channel != null) {
                // close channel first
                close(3);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        channel = clientUtils.getChannel(connectConfig);

        blockingStub = MilvusServiceGrpc.newBlockingStub(channel).withWaitForReady();
        connect(connectConfig, blockingStub);

        if (connectConfig.getDbName() != null) {
            // check if database exists
            clientUtils.checkDatabaseExist(this.blockingStub, connectConfig.getDbName());
        }
    }

    // The withDeadlineAfter() need to be reset for each RPC call.
    // If we set a blockingStub for multiple rpc calls, it eventually will timeout since the timeout is calculated
    // begin the first call and end with the last call.
    // A related discussion: https://github.com/grpc/grpc-java/issues/4305
    private MilvusServiceGrpc.MilvusServiceBlockingStub getRpcStub() {
        if (connectConfig != null && connectConfig.getRpcDeadlineMs() > 0) {
            return blockingStub.withDeadlineAfter(connectConfig.getRpcDeadlineMs(), TimeUnit.MILLISECONDS);
        } else {
            return blockingStub;
        }
    }

    /**
     * This method is internal used, it calls a RPC Connect() to the remote server,
     * and sends the client info to the server so that the server knows which client is interacting,
     * especially for accesses log.
     *
     * The info includes:
     * 1. username(if Authentication is enabled)
     * 2. the client computer's name
     * 3. sdk language type and version
     * 4. the client's local time
     */
    private void connect(ConnectConfig connectConfig, MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String userName = connectConfig.getUsername();
        if (userName == null) {
            userName = ""; // ClientInfo.setUser() requires non-null value
        }

        ClientInfo info = ClientInfo.newBuilder()
                .setSdkType("Java")
                .setSdkVersion(clientUtils.getSDKVersion())
                .setUser(userName)
                .setHost(clientUtils.getHostName())
                .setLocalTime(clientUtils.getLocalTimeStr())
                .build();
        ConnectRequest req = ConnectRequest.newBuilder().setClientInfo(info).build();
        ConnectResponse resp = blockingStub.withDeadlineAfter(connectConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .connect(req);
        if (resp.getStatus().getCode() != 0 || !resp.getStatus().getErrorCode().equals(io.milvus.grpc.ErrorCode.Success)) {
            throw new RuntimeException("Failed to initialize connection. Error: " + resp.getStatus().getReason());
        }
    }

    public void retryConfig(RetryConfig retryConfig) {
        rpcUtils.retryConfig(retryConfig);
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Database Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * use Database
     * @param dbName databaseName
     */
    public void useDatabase(@NonNull String dbName) throws InterruptedException {
        // check if database exists
        clientUtils.checkDatabaseExist(this.getRpcStub(), dbName);
        try {
            this.vectorService.cleanCollectionCache();
            this.connectConfig.setDbName(dbName);
            this.close(3);
            this.connect(this.connectConfig);
        } catch (InterruptedException e){
            logger.error("close connect error");
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a database in Milvus.
     * @param request create database request
     */
    public void createDatabase(CreateDatabaseReq request) {
        rpcUtils.retry(()-> databaseService.createDatabase(this.getRpcStub(), request));
    }
    /**
     * Drops a database. Note that this method drops all data in the database.
     * @param request drop database request
     */
    public void dropDatabase(DropDatabaseReq request) {
        rpcUtils.retry(()-> databaseService.dropDatabase(this.getRpcStub(), request));
    }
    /**
     * List all databases.
     * @return List of String database names
     */
    public ListDatabasesResp listDatabases() {
        return rpcUtils.retry(()-> databaseService.listDatabases(this.getRpcStub()));
    }
    /**
     * Alter database with key value pair. (Available from Milvus v2.4.4)
     * Deprecated, replaced by alterDatabaseProperties from SDK v2.5.3, to keep consistence with other SDKs
     * @param request alter database request
     */
    @Deprecated
    public void alterDatabase(AlterDatabaseReq request) {
        alterDatabaseProperties(AlterDatabasePropertiesReq.builder()
                .databaseName(request.getDatabaseName())
                .properties(request.getProperties())
                .build());
    }
    /**
     * Alter a database's properties.
     * @param request alter database properties request
     */
    public void alterDatabaseProperties(AlterDatabasePropertiesReq request) {
        rpcUtils.retry(()-> databaseService.alterDatabaseProperties(this.getRpcStub(), request));
    }
    /**
     * drop a database's properties.
     * @param request alter database properties request
     */
    public void dropDatabaseProperties(DropDatabasePropertiesReq request) {
        rpcUtils.retry(()-> databaseService.dropDatabaseProperties(this.getRpcStub(), request));
    }
    /**
     * Show detail of database base, such as replica number and resource groups. (Available from Milvus v2.4.4)
     * @param request describe database request
     *
     * @return DescribeDatabaseResp
     */
    public DescribeDatabaseResp describeDatabase(DescribeDatabaseReq request) {
        return rpcUtils.retry(()-> databaseService.describeDatabase(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Collection Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Creates a collection in Milvus.
     * @param request create collection request
     */
    public void createCollection(CreateCollectionReq request) {
        rpcUtils.retry(()-> collectionService.createCollection(this.getRpcStub(), request));
    }

    /**
     * Creates a collection schema.
     * @return CreateCollectionReq.CollectionSchema
     */
    public CreateCollectionReq.CollectionSchema createSchema() {
        return collectionService.createSchema();
    }
    /**
     * list milvus collections
     *
     * @return List of String collection names
     */
    public ListCollectionsResp listCollections() {
        return rpcUtils.retry(()-> collectionService.listCollections(this.getRpcStub()));
    }
    /**
     * Drops a collection in Milvus.
     *
     * @param request drop collection request
     */
    public void dropCollection(DropCollectionReq request) {
        rpcUtils.retry(()-> collectionService.dropCollection(this.getRpcStub(), request));
    }
    /**
     * Alter a collection in Milvus.
     * Deprecated, replaced by alterCollectionProperties from SDK v2.5.3, to keep consistence with other SDKs
     *
     * @param request alter collection request
     */
    @Deprecated
    public void alterCollection(AlterCollectionReq request) {
        alterCollectionProperties(AlterCollectionPropertiesReq.builder()
                .collectionName(request.getCollectionName())
                .databaseName(request.getDatabaseName())
                .properties(request.getProperties())
                .build());
    }
    /**
     * Alter a collection's properties.
     *
     * @param request alter collection properties request
     */
    public void alterCollectionProperties(AlterCollectionPropertiesReq request) {
        rpcUtils.retry(()-> collectionService.alterCollectionProperties(this.getRpcStub(), request));
    }
    /**
     * Alter a field's properties.
     *
     * @param request alter field properties request
     */
    public void alterCollectionField(AlterCollectionFieldReq request) {
        rpcUtils.retry(()-> collectionService.alterCollectionField(this.getRpcStub(), request));
    }
    /**
     * drop a collection's properties.
     * @param request drop collection properties request
     */
    public void dropCollectionProperties(DropCollectionPropertiesReq request) {
        rpcUtils.retry(()-> collectionService.dropCollectionProperties(this.getRpcStub(), request));
    }
    /**
     * Checks whether a collection exists in Milvus.
     *
     * @param request has collection request
     * @return Boolean
     */
    public Boolean hasCollection(HasCollectionReq request) {
        return rpcUtils.retry(()-> collectionService.hasCollection(this.getRpcStub(), request));
    }
    /**
     * Gets the collection info in Milvus.
     *
     * @param request describe collection request
     * @return DescribeCollectionResp
     */
    public DescribeCollectionResp describeCollection(DescribeCollectionReq request) {
        return rpcUtils.retry(()-> collectionService.describeCollection(this.getRpcStub(), request));
    }
    /**
     * get collection stats for a collection in Milvus.
     *
     * @param request get collection stats request
     * @return GetCollectionStatsResp
     */
    public GetCollectionStatsResp getCollectionStats(GetCollectionStatsReq request) {
        return rpcUtils.retry(()-> collectionService.getCollectionStats(this.getRpcStub(), request));
    }
    /**
     * rename collection in a collection in Milvus.
     *
     * @param request rename collection request
     */
    public void renameCollection(RenameCollectionReq request) {
        rpcUtils.retry(()-> collectionService.renameCollection(this.getRpcStub(), request));
    }
    /**
     * Loads a collection into memory in Milvus.
     *
     * @param request load collection request
     */
    public void loadCollection(LoadCollectionReq request) {
        rpcUtils.retry(()-> collectionService.loadCollection(this.getRpcStub(), request));
    }
    /**
     * Refresh loads a collection. Mainly used when there are new segments generated by bulkinsert request.
     * Force the new segments to be loaded into memory.
     * Note: this interface will ignore the LoadCollectionReq.refresh flag
     *
     * @param request refresh load collection request
     */
    public void refreshLoad(RefreshLoadReq request) {
        rpcUtils.retry(()-> collectionService.refreshLoad(this.getRpcStub(), request));
    }
    /**
     * Releases a collection from memory in Milvus.
     *
     * @param request release collection request
     */
    public void releaseCollection(ReleaseCollectionReq request) {
        rpcUtils.retry(()-> collectionService.releaseCollection(this.getRpcStub(), request));
    }
    /**
     * Checks whether a collection is loaded in Milvus.
     *
     * @param request get load state request
     * @return Boolean
     */
    public Boolean getLoadState(GetLoadStateReq request) {
        return rpcUtils.retry(()->collectionService.getLoadState(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Index Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Creates an index for a specified field in a collection in Milvus.
     *
     * @param request create index request
     */
    public void createIndex(CreateIndexReq request) {
        rpcUtils.retry(()->indexService.createIndex(this.getRpcStub(), request));
    }
    /**
     * Drops an index for a specified field in a collection in Milvus.
     *
     * @param request drop index request
     */
    public void dropIndex(DropIndexReq request) {
        rpcUtils.retry(()->indexService.dropIndex(this.getRpcStub(), request));
    }
    /**
     * Alter an index in Milvus.
     * Deprecated, replaced by alterIndexProperties from SDK v2.5.3, to keep consistence with other SDKs
     *
     * @param request alter index request
     */
    @Deprecated
    public void alterIndex(AlterIndexReq request) {
        alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName(request.getCollectionName())
                .databaseName(request.getDatabaseName())
                .indexName(request.getIndexName())
                .properties(request.getProperties())
                .build());
    }
    /**
     * Alter an index's properties.
     *
     * @param request alter index request
     */
    public void alterIndexProperties(AlterIndexPropertiesReq request) {
        rpcUtils.retry(()->indexService.alterIndexProperties(this.getRpcStub(), request));
    }
    /**
     * drop an index's properties.
     * @param request drop index properties request
     */
    public void dropIndexProperties(DropIndexPropertiesReq request) {
        rpcUtils.retry(()-> indexService.dropIndexProperties(this.getRpcStub(), request));
    }
    /**
     * Checks whether an index exists for a specified field in a collection in Milvus.
     *
     * @param request describe index request
     * @return DescribeIndexResp
     */
    public DescribeIndexResp describeIndex(DescribeIndexReq request) {
        return rpcUtils.retry(()->indexService.describeIndex(this.getRpcStub(), request));
    }
    /**
     * Lists all indexes in a collection in Milvus.
     *
     * @param request list indexes request
     * @return List of String names of the indexes
     */
    public List<String> listIndexes(ListIndexesReq request) {
        return rpcUtils.retry(()->indexService.listIndexes(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Vector Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Inserts vectors into a collection in Milvus.
     *
     * @param request insert request
     * @return InsertResp
     */
    public InsertResp insert(InsertReq request) {
        return rpcUtils.retry(()->vectorService.insert(this.getRpcStub(), request));
    }
    /**
     * Upsert vectors into a collection in Milvus.
     *
     * @param request upsert request
     * @return UpsertResp
     */
    public UpsertResp upsert(UpsertReq request) {
        return rpcUtils.retry(()->vectorService.upsert(this.getRpcStub(), request));
    }
    /**
     * Deletes vectors in a collection in Milvus.
     *
     * @param request delete request
     * @return DeleteResp
     */
    public DeleteResp delete(DeleteReq request) {
        return rpcUtils.retry(()->vectorService.delete(this.getRpcStub(), request));
    }
    /**
     * Gets vectors in a collection in Milvus.
     *
     * @param request get request
     * @return GetResp
     */
    public GetResp get(GetReq request) {
        return rpcUtils.retry(()->vectorService.get(this.getRpcStub(), request));
    }

    /**
     * Queries vectors in a collection in Milvus.
     *
     * @param request query request
     * @return QueryResp
     */
    public QueryResp query(QueryReq request) {
        return rpcUtils.retry(()->vectorService.query(this.getRpcStub(), request));
    }
    /**
     * Searches vectors in a collection in Milvus.
     *
     * @param request search request
     * @return SearchResp
     */
    public SearchResp search(SearchReq request) {
        return rpcUtils.retry(()->vectorService.search(this.getRpcStub(), request));
    }
    /**
     * Conducts multi vector similarity search with a ranker for rearrangement.
     *
     * @param request search request
     * @return SearchResp
     */
    public SearchResp hybridSearch(HybridSearchReq request) {
        return rpcUtils.retry(()->vectorService.hybridSearch(this.getRpcStub(), request));
    }

    /**
     * Get queryIterator based on scalar field(s) filtered by boolean expression.
     * Note that the order of the returned entities cannot be guaranteed.
     *
     * @param request {@link QueryIteratorReq}
     * @return {status:result code,data: QueryIterator}
     */
    public QueryIterator queryIterator(QueryIteratorReq request) {
        return rpcUtils.retry(()->vectorService.queryIterator(this.getRpcStub(), request));
    }

    /**
     * Get searchIterator based on a vector field. Use expression to do filtering before search.
     *
     * @param request {@link SearchIteratorReq}
     * @return {status:result code, data: SearchIterator}
     */
    public SearchIterator searchIterator(SearchIteratorReq request) {
        return rpcUtils.retry(()->vectorService.searchIterator(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Partition Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Creates a partition in a collection in Milvus.
     *
     * @param request create partition request
     */
    public void createPartition(CreatePartitionReq request) {
        rpcUtils.retry(()->partitionService.createPartition(this.getRpcStub(), request));
    }

    /**
     * Drops a partition in a collection in Milvus.
     *
     * @param request drop partition request
     */
    public void dropPartition(DropPartitionReq request) {
        rpcUtils.retry(()->partitionService.dropPartition(this.getRpcStub(), request));
    }

    /**
     * Checks whether a partition exists in a collection in Milvus.
     *
     * @param request has partition request
     * @return Boolean
     */
    public Boolean hasPartition(HasPartitionReq request) {
        return rpcUtils.retry(()->partitionService.hasPartition(this.getRpcStub(), request));
    }

    /**
     * Lists all partitions in a collection in Milvus.
     *
     * @param request list partitions request
     * @return List of String partition names
     */
    public List<String> listPartitions(ListPartitionsReq request) {
        return rpcUtils.retry(()->partitionService.listPartitions(this.getRpcStub(), request));
    }

    /**
     * get a partition stats in Milvus.
     *
     * @param request get partition stats request
     * @return GetPartitionStatsResp
     */
    public GetPartitionStatsResp getPartitionStats(GetPartitionStatsReq request) {
        return rpcUtils.retry(()-> partitionService.getPartitionStats(this.getRpcStub(), request));
    }

    /**
     * Loads partitions in a collection in Milvus.
     *
     * @param request load partitions request
     */
    public void loadPartitions(LoadPartitionsReq request) {
        rpcUtils.retry(()->partitionService.loadPartitions(this.getRpcStub(), request));
    }
    /**
     * Releases partitions in a collection in Milvus.
     *
     * @param request release partitions request
     */
    public void releasePartitions(ReleasePartitionsReq request) {
        rpcUtils.retry(()->partitionService.releasePartitions(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // RBAC Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * list users
     *
     * @return List of String usernames
     */
    public List<String> listUsers() {
        return rpcUtils.retry(()->rbacService.listUsers(this.getRpcStub()));
    }
    /**
     * describe user
     *
     * @param request describe user request
     * @return DescribeUserResp
     */
    public DescribeUserResp describeUser(DescribeUserReq request) {
        return rpcUtils.retry(()->rbacService.describeUser(this.getRpcStub(), request));
    }
    /**
     * create user
     *
     * @param request create user request
     */
    public void createUser(CreateUserReq request) {
        rpcUtils.retry(()->rbacService.createUser(this.getRpcStub(), request));
    }
    /**
     * change password
     *
     * @param request change password request
     */
    public void updatePassword(UpdatePasswordReq request) {
        rpcUtils.retry(()->rbacService.updatePassword(this.getRpcStub(), request));
    }
    /**
     * drop user
     *
     * @param request drop user request
     */
    public void dropUser(DropUserReq request) {
        rpcUtils.retry(()->rbacService.dropUser(this.getRpcStub(), request));
    }
    // role operations
    /**
     * list roles
     *
     * @return List of String role names
     */
    public List<String> listRoles() {
        return rpcUtils.retry(()->rbacService.listRoles(this.getRpcStub()));
    }
    /**
     * describe role
     *
     * @param request describe role request
     * @return DescribeRoleResp
     */
    public DescribeRoleResp describeRole(DescribeRoleReq request) {
        return rpcUtils.retry(()->rbacService.describeRole(this.getRpcStub(), request));
    }
    /**
     * create role
     *
     * @param request create role request
     */
    public void createRole(CreateRoleReq request) {
        rpcUtils.retry(()->rbacService.createRole(this.getRpcStub(), request));
    }
    /**
     * drop role
     *
     * @param request drop role request
     */
    public void dropRole(DropRoleReq request) {
        rpcUtils.retry(()->rbacService.dropRole(this.getRpcStub(), request));
    }
    /**
     * grant privilege
     *
     * @param request grant privilege request
     */
    public void grantPrivilege(GrantPrivilegeReq request) {
        rpcUtils.retry(()->rbacService.grantPrivilege(this.getRpcStub(), request));
    }
    /**
     * revoke privilege
     *
     * @param request revoke privilege request
     */
    public void revokePrivilege(RevokePrivilegeReq request) {
        rpcUtils.retry(()->rbacService.revokePrivilege(this.getRpcStub(), request));
    }
    /**
     * grant role
     *
     * @param request grant role request
     */
    public void grantRole(GrantRoleReq request) {
        rpcUtils.retry(()->rbacService.grantRole(this.getRpcStub(), request));
    }
    /**
     * revoke role
     *
     * @param request revoke role request
     */
    public void revokeRole(RevokeRoleReq request) {
        rpcUtils.retry(()->rbacService.revokeRole(this.getRpcStub(), request));
    }

    public void createPrivilegeGroup(CreatePrivilegeGroupReq request) {
        rpcUtils.retry(()->rbacService.createPrivilegeGroup(this.getRpcStub(), request));
    }

    public void dropPrivilegeGroup(DropPrivilegeGroupReq request) {
        rpcUtils.retry(()->rbacService.dropPrivilegeGroup(this.getRpcStub(), request));
    }

    public ListPrivilegeGroupsResp listPrivilegeGroups(ListPrivilegeGroupsReq request) {
        return rpcUtils.retry(()->rbacService.listPrivilegeGroups(this.getRpcStub(), request));
    }

    public void addPrivilegesToGroup(AddPrivilegesToGroupReq request) {
        rpcUtils.retry(()->rbacService.addPrivilegesToGroup(this.getRpcStub(), request));
    }

    public void removePrivilegesFromGroup(RemovePrivilegesFromGroupReq request) {
        rpcUtils.retry(()->rbacService.removePrivilegesFromGroup(this.getRpcStub(), request));
    }

    public void grantPrivilegeV2(GrantPrivilegeReqV2 request) {
        rpcUtils.retry(()->rbacService.grantPrivilegeV2(this.getRpcStub(), request));
    }

    public void revokePrivilegeV2(RevokePrivilegeReqV2 request) {
        rpcUtils.retry(()->rbacService.revokePrivilegeV2(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Resource group Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Create a resource group.
     *
     * @param request {@link CreateResourceGroupReq}
     */
    public void createResourceGroup(CreateResourceGroupReq request){
        rpcUtils.retry(()->rgroupService.createResourceGroup(this.getRpcStub(), request));
    }

    /**
     * Update resource groups.
     *
     * @param request {@link UpdateResourceGroupsReq}
     */
    public void updateResourceGroups(UpdateResourceGroupsReq request) {
        rpcUtils.retry(()->rgroupService.updateResourceGroups(this.getRpcStub(), request));
    }

    /**
     * Drop a resource group.
     *
     * @param request {@link DropResourceGroupReq}
     */
    public void dropResourceGroup(DropResourceGroupReq request) {
        rpcUtils.retry(()->rgroupService.dropResourceGroup(this.getRpcStub(), request));
    }

    /**
     * List resource groups.
     *
     * @param request {@link ListResourceGroupsReq}
     * @return ListResourceGroupsResp
     */
    public ListResourceGroupsResp listResourceGroups(ListResourceGroupsReq request) {
        return rpcUtils.retry(()->rgroupService.listResourceGroups(this.getRpcStub(), request));
    }

    /**
     * Describe a resource group.
     *
     * @param request {@link DescribeResourceGroupReq}
     * @return DescribeResourceGroupResp
     */
    public DescribeResourceGroupResp describeResourceGroup(DescribeResourceGroupReq request) {
        return rpcUtils.retry(()->rgroupService.describeResourceGroup(this.getRpcStub(), request));
    }

    /**
     * Transfer a replica from source resource group to target resource_group.
     *
     * @param request {@link TransferReplicaReq}
     */
    public void transferReplica(TransferReplicaReq request) {
        rpcUtils.retry(()->rgroupService.transferReplica(this.getRpcStub(), request));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
    // Utility Operations
    /////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * create aliases
     *
     * @param request create alias request
     */
    public void createAlias(CreateAliasReq request) {
        rpcUtils.retry(()->utilityService.createAlias(this.getRpcStub(), request));
    }
    /**
     * drop aliases
     *
     * @param request drop alias request
     */
    public void dropAlias(DropAliasReq request) {
        rpcUtils.retry(()->utilityService.dropAlias(this.getRpcStub(), request));
    }
    /**
     * alter aliases
     *
     * @param request alter alias request
     */
    public void alterAlias(AlterAliasReq request) {
        rpcUtils.retry(()->utilityService.alterAlias(this.getRpcStub(), request));
    }
    /**
     * list aliases
     *
     * @param request list aliases request
     * @return List of String alias names
     */
    public ListAliasResp listAliases(ListAliasesReq request) {
        return rpcUtils.retry(()->utilityService.listAliases(this.getRpcStub(), request));
    }
    /**
     * describe aliases
     *
     * @param request describe alias request
     * @return DescribeAliasResp
     */
    public DescribeAliasResp describeAlias(DescribeAliasReq request) {
        return rpcUtils.retry(()->utilityService.describeAlias(this.getRpcStub(), request));
    }

    /**
     * trigger a flush action in server side
     *
     * @param request flush request
     */
    public void flush(FlushReq request) {
        FlushResp response = rpcUtils.retry(()->utilityService.flush(this.getRpcStub(), request));

        // The BlockingStub.flush() api returns immediately after the datanode set all growing segments to be "sealed".
        // The flush state becomes "Completed" after the datanode uploading them to S3 asynchronously.
        // Here we wait the flush action to be "Completed".
        MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub =
                MilvusServiceGrpc.newBlockingStub(channel).withWaitForReady();
        if (request.getWaitFlushedTimeoutMs() > 0L) {
            tempBlockingStub = tempBlockingStub.withDeadlineAfter(request.getWaitFlushedTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        utilityService.waitFlush(tempBlockingStub, response.getCollectionSegmentIDs(), response.getCollectionFlushTs());
    }

    /**
     * trigger an asynchronous compaction in server side
     *
     * @param request compact request
     * @return CompactResp
     */
    public CompactResp compact(CompactReq request) {
        return rpcUtils.retry(()->utilityService.compact(this.getRpcStub(), request));
    }

    /**
     * get a compact task state by its ID
     *
     * @param request get compact state request
     * @return GetCompactStateResp
     */
    public GetCompactionStateResp getCompactionState(GetCompactionStateReq request) {
        return rpcUtils.retry(()->utilityService.getCompactionState(this.getRpcStub(), request));
    }

    /**
     * Get server version
     *
     * @return String
     */
    public String getServerVersion() {
        return rpcUtils.retry(()->clientUtils.getServerVersion(this.getRpcStub()));
    }

    /**
     * Disconnects from a Milvus server with configurable timeout
     *
     * @param maxWaitSeconds max wait seconds
     * @throws InterruptedException throws InterruptedException if the client failed to close connection
     */
    public void close(long maxWaitSeconds) throws InterruptedException {
        if(channel!= null){
            channel.shutdownNow();
            channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Disconnects from a Milvus server with timeout of 1 second
     *
     */
    public void close() {
        try {
            close(TimeUnit.MINUTES.toSeconds(1));
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown Milvus client!");
        }
    }

    public boolean clientIsReady() {
        return channel != null && !channel.isShutdown() && !channel.isTerminated();
    }
}

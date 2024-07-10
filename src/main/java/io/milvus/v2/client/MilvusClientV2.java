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
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.v2.service.collection.CollectionService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.PartitionService;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.rbac.RoleService;
import io.milvus.v2.service.rbac.UserService;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.utility.UtilityService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.ClientUtils;
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
    private final CollectionService collectionService = new CollectionService();
    private final IndexService indexService = new IndexService();
    private final VectorService vectorService = new VectorService();
    private final PartitionService partitionService = new PartitionService();
    private final UserService userService = new UserService();
    private final RoleService roleService = new RoleService();
    private final UtilityService utilityService = new UtilityService();
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

        if (connectConfig.getRpcDeadlineMs() > 0) {
            blockingStub =  MilvusServiceGrpc.newBlockingStub(channel).withWaitForReady()
                    .withDeadlineAfter(connectConfig.getRpcDeadlineMs(), TimeUnit.MILLISECONDS);
        }else {
            blockingStub = MilvusServiceGrpc.newBlockingStub(channel);
        }

        if (connectConfig.getDbName() != null) {
            // check if database exists
            clientUtils.checkDatabaseExist(this.blockingStub, connectConfig.getDbName());
        }
    }

    /**
     * use Database
     * @param dbName databaseName
     */
    public void useDatabase(@NonNull String dbName) {
        // check if database exists
        clientUtils.checkDatabaseExist(this.blockingStub, dbName);
        try {
            this.connectConfig.setDbName(dbName);
            this.close(3);
            this.connect(this.connectConfig);
        }catch (InterruptedException e){
            logger.error("close connect error");
        }
    }

    //Collection Operations
    /**
     * Creates a collection in Milvus.
     * @param request create collection request
     */
    public void createCollection(CreateCollectionReq request) {
        collectionService.createCollection(this.blockingStub, request);
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
        return collectionService.listCollections(this.blockingStub);
    }

    /**
     * Drops a collection in Milvus.
     *
     * @param request drop collection request
     */
    public void dropCollection(DropCollectionReq request) {
        collectionService.dropCollection(this.blockingStub, request);
    }
    /**
     * Checks whether a collection exists in Milvus.
     *
     * @param request has collection request
     * @return Boolean
     */
    public Boolean hasCollection(HasCollectionReq request) {
        return collectionService.hasCollection(this.blockingStub, request);
    }
    /**
     * Gets the collection info in Milvus.
     *
     * @param request describe collection request
     * @return DescribeCollectionResp
     */
    public DescribeCollectionResp describeCollection(DescribeCollectionReq request) {
        return collectionService.describeCollection(this.blockingStub, request);
    }
    /**
     * get collection stats for a collection in Milvus.
     *
     * @param request get collection stats request
     * @return GetCollectionStatsResp
     */
    public GetCollectionStatsResp getCollectionStats(GetCollectionStatsReq request) {
        return collectionService.getCollectionStats(this.blockingStub, request);
    }
    /**
     * rename collection in a collection in Milvus.
     *
     * @param request rename collection request
     */
    public void renameCollection(RenameCollectionReq request) {
        collectionService.renameCollection(this.blockingStub, request);
    }
    /**
     * Loads a collection into memory in Milvus.
     *
     * @param request load collection request
     */
    public void loadCollection(LoadCollectionReq request) {
        collectionService.loadCollection(this.blockingStub, request);
    }
    /**
     * Releases a collection from memory in Milvus.
     *
     * @param request release collection request
     */
    public void releaseCollection(ReleaseCollectionReq request) {
        collectionService.releaseCollection(this.blockingStub, request);
    }
    /**
     * Checks whether a collection is loaded in Milvus.
     *
     * @param request get load state request
     * @return Boolean
     */
    public Boolean getLoadState(GetLoadStateReq request) {
        return collectionService.getLoadState(this.blockingStub, request);
    }

    //Index Operations
    /**
     * Creates an index for a specified field in a collection in Milvus.
     *
     * @param request create index request
     */
    public void createIndex(CreateIndexReq request) {
        indexService.createIndex(this.blockingStub, request);
    }
    /**
     * Drops an index for a specified field in a collection in Milvus.
     *
     * @param request drop index request
     */
    public void dropIndex(DropIndexReq request) {
        indexService.dropIndex(this.blockingStub, request);
    }
    /**
     * Checks whether an index exists for a specified field in a collection in Milvus.
     *
     * @param request describe index request
     * @return DescribeIndexResp
     */
    public DescribeIndexResp describeIndex(DescribeIndexReq request) {
        return indexService.describeIndex(this.blockingStub, request);
    }

    /**
     * Lists all indexes in a collection in Milvus.
     *
     * @param request list indexes request
     * @return List of String indexes names
     */
    public List<String> listIndexes(ListIndexesReq request) {
        return indexService.listIndexes(this.blockingStub, request);
    }
    // Vector Operations

    /**
     * Inserts vectors into a collection in Milvus.
     *
     * @param request insert request
     * @return InsertResp
     */
    public InsertResp insert(InsertReq request) {
        return vectorService.insert(this.blockingStub, request);
    }
    /**
     * Upsert vectors into a collection in Milvus.
     *
     * @param request upsert request
     * @return UpsertResp
     */
    public UpsertResp upsert(UpsertReq request) {
        return vectorService.upsert(this.blockingStub, request);
    }
    /**
     * Deletes vectors in a collection in Milvus.
     *
     * @param request delete request
     * @return DeleteResp
     */
    public DeleteResp delete(DeleteReq request) {
        return vectorService.delete(this.blockingStub, request);
    }
    /**
     * Gets vectors in a collection in Milvus.
     *
     * @param request get request
     * @return GetResp
     */
    public GetResp get(GetReq request) {
        return vectorService.get(this.blockingStub, request);
    }

    /**
     * Queries vectors in a collection in Milvus.
     *
     * @param request query request
     * @return QueryResp
     */
    public QueryResp query(QueryReq request) {
        return vectorService.query(this.blockingStub, request);
    }
    /**
     * Searches vectors in a collection in Milvus.
     *
     * @param request search request
     * @return SearchResp
     */
    public SearchResp search(SearchReq request) {
        return vectorService.search(this.blockingStub, request);
    }

    /**
     * Get queryIterator based on scalar field(s) filtered by boolean expression.
     * Note that the order of the returned entities cannot be guaranteed.
     *
     * @param request {@link QueryIteratorReq}
     * @return {status:result code,data: QueryIterator}
     */
    public QueryIterator queryIterator(QueryIteratorReq request) {
        return vectorService.queryIterator(this.blockingStub, request);
    }

    /**
     * Get searchIterator based on a vector field. Use expression to do filtering before search.
     *
     * @param request {@link SearchIteratorReq}
     * @return {status:result code, data: SearchIterator}
     */
    public SearchIterator searchIterator(SearchIteratorReq request) {
        return vectorService.searchIterator(this.blockingStub, request);
    }

    // Partition Operations
    /**
     * Creates a partition in a collection in Milvus.
     *
     * @param request create partition request
     */
    public void createPartition(CreatePartitionReq request) {
        partitionService.createPartition(this.blockingStub, request);
    }

    /**
     * Drops a partition in a collection in Milvus.
     *
     * @param request drop partition request
     */
    public void dropPartition(DropPartitionReq request) {
        partitionService.dropPartition(this.blockingStub, request);
    }

    /**
     * Checks whether a partition exists in a collection in Milvus.
     *
     * @param request has partition request
     * @return Boolean
     */
    public Boolean hasPartition(HasPartitionReq request) {
        return partitionService.hasPartition(this.blockingStub, request);
    }

    /**
     * Lists all partitions in a collection in Milvus.
     *
     * @param request list partitions request
     * @return List of String partition names
     */
    public List<String> listPartitions(ListPartitionsReq request) {
        return partitionService.listPartitions(this.blockingStub, request);
    }

    /**
     * Loads partitions in a collection in Milvus.
     *
     * @param request load partitions request
     */
    public void loadPartitions(LoadPartitionsReq request) {
        partitionService.loadPartitions(this.blockingStub, request);
    }
    /**
     * Releases partitions in a collection in Milvus.
     *
     * @param request release partitions request
     */
    public void releasePartitions(ReleasePartitionsReq request) {
        partitionService.releasePartitions(this.blockingStub, request);
    }
    // rbac operations
    // user operations
    /**
     * list users
     *
     * @return List of String usernames
     */
    public List<String> listUsers() {
        return userService.listUsers(this.blockingStub);
    }
    /**
     * describe user
     *
     * @param request describe user request
     * @return DescribeUserResp
     */
    public DescribeUserResp describeUser(DescribeUserReq request) {
        return userService.describeUser(this.blockingStub, request);
    }
    /**
     * create user
     *
     * @param request create user request
     */
    public void createUser(CreateUserReq request) {
        userService.createUser(this.blockingStub, request);
    }
    /**
     * change password
     *
     * @param request change password request
     */
    public void updatePassword(UpdatePasswordReq request) {
        userService.updatePassword(this.blockingStub, request);
    }
    /**
     * drop user
     *
     * @param request drop user request
     */
    public void dropUser(DropUserReq request) {
        userService.dropUser(this.blockingStub, request);
    }
    // role operations
    /**
     * list roles
     *
     * @return List of String role names
     */
    public List<String> listRoles() {
        return roleService.listRoles(this.blockingStub);
    }
    /**
     * describe role
     *
     * @param request describe role request
     * @return DescribeRoleResp
     */
    public DescribeRoleResp describeRole(DescribeRoleReq request) {
        return roleService.describeRole(this.blockingStub, request);
    }
    /**
     * create role
     *
     * @param request create role request
     */
    public void createRole(CreateRoleReq request) {
        roleService.createRole(this.blockingStub, request);
    }
    /**
     * drop role
     *
     * @param request drop role request
     */
    public void dropRole(DropRoleReq request) {
        roleService.dropRole(this.blockingStub, request);
    }
    /**
     * grant privilege
     *
     * @param request grant privilege request
     */
    public void grantPrivilege(GrantPrivilegeReq request) {
        roleService.grantPrivilege(this.blockingStub, request);
    }
    /**
     * revoke privilege
     *
     * @param request revoke privilege request
     */
    public void revokePrivilege(RevokePrivilegeReq request) {
        roleService.revokePrivilege(this.blockingStub, request);
    }
    /**
     * grant role
     *
     * @param request grant role request
     */
    public void grantRole(GrantRoleReq request) {
        roleService.grantRole(this.blockingStub, request);
    }
    /**
     * revoke role
     *
     * @param request revoke role request
     */
    public void revokeRole(RevokeRoleReq request) {
        roleService.revokeRole(this.blockingStub, request);
    }

    // Utility Operations

    /**
     * create aliases
     *
     * @param request create alias request
     */
    public void createAlias(CreateAliasReq request) {
        utilityService.createAlias(this.blockingStub, request);
    }
    /**
     * drop aliases
     *
     * @param request drop alias request
     */
    public void dropAlias(DropAliasReq request) {
        utilityService.dropAlias(this.blockingStub, request);
    }
    /**
     * alter aliases
     *
     * @param request alter alias request
     */
    public void alterAlias(AlterAliasReq request) {
        utilityService.alterAlias(this.blockingStub, request);
    }
    /**
     * list aliases
     *
     * @param request list aliases request
     * @return List of String aliases names
     */
    public ListAliasResp listAliases(ListAliasesReq request) {
        return utilityService.listAliases(this.blockingStub, request);
    }
    /**
     * describe aliases
     *
     * @param request describe alias request
     * @return DescribeAliasResp
     */
    public DescribeAliasResp describeAlias(DescribeAliasReq request) {
        return utilityService.describeAlias(this.blockingStub, request);
    }

    /**
     * close client
     *
     * @param maxWaitSeconds max wait seconds
     * @throws InterruptedException if the client failed to close connection
     */
    public void close(long maxWaitSeconds) throws InterruptedException {
        if(channel!= null){
            channel.shutdownNow();
            channel.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
        }
    }
}

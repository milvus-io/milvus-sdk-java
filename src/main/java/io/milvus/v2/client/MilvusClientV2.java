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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
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
import io.milvus.v2.service.rbac.RoleService;
import io.milvus.v2.service.rbac.UserService;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.*;
import io.milvus.v2.service.utility.UtilityService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.ClientUtils;
import lombok.NonNull;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
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
    private final UserService userService = new UserService();
    private final RoleService roleService = new RoleService();
    private final UtilityService utilityService = new UtilityService();
    private ConnectConfig connectConfig;
    private RetryConfig retryConfig = RetryConfig.builder().build();

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

    public void retryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
    }

    private <T> T retry(Callable<T> callable) {
        int maxRetryTimes = retryConfig.getMaxRetryTimes();
        // no retry, direct call the method
        if (maxRetryTimes <= 1) {
            try {
                return callable.call();
            } catch (StatusRuntimeException e) {
                throw new MilvusClientException(ErrorCode.RPC_ERROR, e.getMessage()); // rpc error
            } catch (MilvusClientException e) {
                throw e; // server error or client error
            } catch (Exception e) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e.getMessage()); // others error treated as client error
            }
        }

        // method to check timeout
        long begin = System.currentTimeMillis();
        long maxRetryTimeoutMs = retryConfig.getMaxRetryTimeoutMs();
        Callable<Boolean> timeoutChecker = ()->{
            long current = System.currentTimeMillis();
            long cost = (current - begin);
            if (maxRetryTimeoutMs > 0 && cost >= maxRetryTimeoutMs) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        };

        // retry within timeout
        long retryIntervalMs = retryConfig.getInitialBackOffMs();
        for (int k = 1; k <= maxRetryTimes; k++) {
            try {
                return callable.call();
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.DEADLINE_EXCEEDED.getCode()
                        || code == Status.PERMISSION_DENIED.getCode()
                        || code == Status.UNAUTHENTICATED.getCode()
                        || code == Status.INVALID_ARGUMENT.getCode()
                        || code == Status.ALREADY_EXISTS.getCode()
                        || code == Status.RESOURCE_EXHAUSTED.getCode()
                        || code == Status.UNIMPLEMENTED.getCode()) {
                    String msg = String.format("Encounter rpc error that cannot be retried, reason: %s", e.getMessage());
                    logger.error(msg);
                    throw new MilvusClientException(ErrorCode.RPC_ERROR, msg); // throw rpc error
                }

                try {
                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                        logger.warn(msg);
                        throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                    }
                } catch (Exception ignored) {
                }
            } catch (MilvusClientException e) {
                try {
                    if (timeoutChecker.call() == Boolean.TRUE) {
                        String msg = String.format("Retry timeout: %dms, maxRetry:%d, retries: %d, reason: %s",
                                maxRetryTimeoutMs, maxRetryTimes, k, e.getMessage());
                        logger.warn(msg);
                        throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exit retry for timeout
                    }
                } catch (Exception ignored) {
                }

                // for server-side returned error, only retry for rate limit
                // in new error codes of v2.3, rate limit error value is 8
                if (retryConfig.isRetryOnRateLimit() &&
                        (e.getLegacyServerCode() == io.milvus.grpc.ErrorCode.RateLimit.getNumber() ||
                                e.getServerErrCode() == 8)) {
                    // cannot be retried
                } else {
                    throw e; // exit retry, throw the error
                }
            } catch (Exception e) {
                throw new MilvusClientException(ErrorCode.CLIENT_ERROR, e.getMessage()); // others error treated as client error
            }

            try {
                if (k >= maxRetryTimes) {
                    // finish retry loop, return the response of the last retry
                    String msg = String.format("Finish %d retry times, stop retry", maxRetryTimes);
                    logger.warn(msg);
                    throw new MilvusClientException(ErrorCode.TIMEOUT, msg); // exceed max time, exit retry
                } else {
                    // sleep for interval
                    // print log, follow the pymilvus logic
                    if (k > 3) {
                        logger.warn(String.format("Retry(%d) with interval %dms", k, retryIntervalMs));
                    }
                    TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
                }

                // reset the next interval value
                retryIntervalMs = retryIntervalMs*retryConfig.getBackOffMultiplier();
                if (retryIntervalMs > retryConfig.getMaxBackOffMs()) {
                    retryIntervalMs = retryConfig.getMaxBackOffMs();
                }
            } catch (Exception ignored) {
            }
        }

        return null;
    }

    /**
     * use Database
     * @param dbName databaseName
     */
    public void useDatabase(@NonNull String dbName) throws InterruptedException {
        // check if database exists
        clientUtils.checkDatabaseExist(this.blockingStub, dbName);
        try {
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
        retry(()-> databaseService.createDatabase(this.blockingStub, request));
    }

    /**
     * Drops a database. Note that this method drops all data in the database.
     * @param request drop database request
     */
    public void dropDatabase(DropDatabaseReq request) {
        retry(()-> databaseService.dropDatabase(this.blockingStub, request));
    }

    /**
     * List all databases.
     * @return List of String database names
     */
    public ListDatabasesResp listDatabases() {
        return retry(()-> databaseService.listDatabases(this.blockingStub));
    }

    /**
     * Alter database with key value pair. (Available from Milvus v2.4.4)
     * @param request alter database request
     */
    public void alterDatabase(AlterDatabaseReq request) {
        retry(()-> databaseService.alterDatabase(this.blockingStub, request));
    }

    /**
     * Show detail of database base, such as replica number and resource groups. (Available from Milvus v2.4.4)
     * @param request describe database request
     *
     * @return DescribeDatabaseResp
     */
    public DescribeDatabaseResp describeDatabase(DescribeDatabaseReq request) {
        return retry(()-> databaseService.describeDatabase(this.blockingStub, request));
    }

    //Collection Operations
    /**
     * Creates a collection in Milvus.
     * @param request create collection request
     */
    public void createCollection(CreateCollectionReq request) {
        retry(()-> collectionService.createCollection(this.blockingStub, request));
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
        return retry(()-> collectionService.listCollections(this.blockingStub));
    }

    /**
     * Drops a collection in Milvus.
     *
     * @param request drop collection request
     */
    public void dropCollection(DropCollectionReq request) {
        retry(()-> collectionService.dropCollection(this.blockingStub, request));
    }
    /**
     * Alter a collection in Milvus.
     *
     * @param request alter collection request
     */
    public void alterCollection(AlterCollectionReq request) {
        retry(()-> collectionService.alterCollection(this.blockingStub, request));
    }
    /**
     * Checks whether a collection exists in Milvus.
     *
     * @param request has collection request
     * @return Boolean
     */
    public Boolean hasCollection(HasCollectionReq request) {
        return retry(()-> collectionService.hasCollection(this.blockingStub, request));
    }
    /**
     * Gets the collection info in Milvus.
     *
     * @param request describe collection request
     * @return DescribeCollectionResp
     */
    public DescribeCollectionResp describeCollection(DescribeCollectionReq request) {
        return retry(()-> collectionService.describeCollection(this.blockingStub, request));
    }
    /**
     * get collection stats for a collection in Milvus.
     *
     * @param request get collection stats request
     * @return GetCollectionStatsResp
     */
    public GetCollectionStatsResp getCollectionStats(GetCollectionStatsReq request) {
        return retry(()-> collectionService.getCollectionStats(this.blockingStub, request));
    }
    /**
     * rename collection in a collection in Milvus.
     *
     * @param request rename collection request
     */
    public void renameCollection(RenameCollectionReq request) {
        retry(()-> collectionService.renameCollection(this.blockingStub, request));
    }
    /**
     * Loads a collection into memory in Milvus.
     *
     * @param request load collection request
     */
    public void loadCollection(LoadCollectionReq request) {
        retry(()-> collectionService.loadCollection(this.blockingStub, request));
    }
    /**
     * Releases a collection from memory in Milvus.
     *
     * @param request release collection request
     */
    public void releaseCollection(ReleaseCollectionReq request) {
        retry(()-> collectionService.releaseCollection(this.blockingStub, request));
    }
    /**
     * Checks whether a collection is loaded in Milvus.
     *
     * @param request get load state request
     * @return Boolean
     */
    public Boolean getLoadState(GetLoadStateReq request) {
        return retry(()->collectionService.getLoadState(this.blockingStub, request));
    }

    //Index Operations
    /**
     * Creates an index for a specified field in a collection in Milvus.
     *
     * @param request create index request
     */
    public void createIndex(CreateIndexReq request) {
        retry(()->indexService.createIndex(this.blockingStub, request));
    }
    /**
     * Drops an index for a specified field in a collection in Milvus.
     *
     * @param request drop index request
     */
    public void dropIndex(DropIndexReq request) {
        retry(()->indexService.dropIndex(this.blockingStub, request));
    }
    /**
     * Alter an index in Milvus.
     *
     * @param request alter index request
     */
    public void alterIndex(AlterIndexReq request) {
        retry(()->indexService.alterIndex(this.blockingStub, request));
    }
    /**
     * Checks whether an index exists for a specified field in a collection in Milvus.
     *
     * @param request describe index request
     * @return DescribeIndexResp
     */
    public DescribeIndexResp describeIndex(DescribeIndexReq request) {
        return retry(()->indexService.describeIndex(this.blockingStub, request));
    }

    /**
     * Lists all indexes in a collection in Milvus.
     *
     * @param request list indexes request
     * @return List of String names of the indexes
     */
    public List<String> listIndexes(ListIndexesReq request) {
        return retry(()->indexService.listIndexes(this.blockingStub, request));
    }
    // Vector Operations

    /**
     * Inserts vectors into a collection in Milvus.
     *
     * @param request insert request
     * @return InsertResp
     */
    public InsertResp insert(InsertReq request) {
        return retry(()->vectorService.insert(this.blockingStub, request));
    }
    /**
     * Upsert vectors into a collection in Milvus.
     *
     * @param request upsert request
     * @return UpsertResp
     */
    public UpsertResp upsert(UpsertReq request) {
        return retry(()->vectorService.upsert(this.blockingStub, request));
    }
    /**
     * Deletes vectors in a collection in Milvus.
     *
     * @param request delete request
     * @return DeleteResp
     */
    public DeleteResp delete(DeleteReq request) {
        return retry(()->vectorService.delete(this.blockingStub, request));
    }
    /**
     * Gets vectors in a collection in Milvus.
     *
     * @param request get request
     * @return GetResp
     */
    public GetResp get(GetReq request) {
        return retry(()->vectorService.get(this.blockingStub, request));
    }

    /**
     * Queries vectors in a collection in Milvus.
     *
     * @param request query request
     * @return QueryResp
     */
    public QueryResp query(QueryReq request) {
        return retry(()->vectorService.query(this.blockingStub, request));
    }
    /**
     * Searches vectors in a collection in Milvus.
     *
     * @param request search request
     * @return SearchResp
     */
    public SearchResp search(SearchReq request) {
        return retry(()->vectorService.search(this.blockingStub, request));
    }
    /**
     * Conducts multi vector similarity search with a ranker for rearrangement.
     *
     * @param request search request
     * @return SearchResp
     */
    public SearchResp hybridSearch(HybridSearchReq request) {
        return retry(()->vectorService.hybridSearch(this.blockingStub, request));
    }

    /**
     * Get queryIterator based on scalar field(s) filtered by boolean expression.
     * Note that the order of the returned entities cannot be guaranteed.
     *
     * @param request {@link QueryIteratorReq}
     * @return {status:result code,data: QueryIterator}
     */
    public QueryIterator queryIterator(QueryIteratorReq request) {
        return retry(()->vectorService.queryIterator(this.blockingStub, request));
    }

    /**
     * Get searchIterator based on a vector field. Use expression to do filtering before search.
     *
     * @param request {@link SearchIteratorReq}
     * @return {status:result code, data: SearchIterator}
     */
    public SearchIterator searchIterator(SearchIteratorReq request) {
        return retry(()->vectorService.searchIterator(this.blockingStub, request));
    }

    // Partition Operations
    /**
     * Creates a partition in a collection in Milvus.
     *
     * @param request create partition request
     */
    public void createPartition(CreatePartitionReq request) {
        retry(()->partitionService.createPartition(this.blockingStub, request));
    }

    /**
     * Drops a partition in a collection in Milvus.
     *
     * @param request drop partition request
     */
    public void dropPartition(DropPartitionReq request) {
        retry(()->partitionService.dropPartition(this.blockingStub, request));
    }

    /**
     * Checks whether a partition exists in a collection in Milvus.
     *
     * @param request has partition request
     * @return Boolean
     */
    public Boolean hasPartition(HasPartitionReq request) {
        return retry(()->partitionService.hasPartition(this.blockingStub, request));
    }

    /**
     * Lists all partitions in a collection in Milvus.
     *
     * @param request list partitions request
     * @return List of String partition names
     */
    public List<String> listPartitions(ListPartitionsReq request) {
        return retry(()->partitionService.listPartitions(this.blockingStub, request));
    }

    /**
     * Loads partitions in a collection in Milvus.
     *
     * @param request load partitions request
     */
    public void loadPartitions(LoadPartitionsReq request) {
        retry(()->partitionService.loadPartitions(this.blockingStub, request));
    }
    /**
     * Releases partitions in a collection in Milvus.
     *
     * @param request release partitions request
     */
    public void releasePartitions(ReleasePartitionsReq request) {
        retry(()->partitionService.releasePartitions(this.blockingStub, request));
    }
    // rbac operations
    // user operations
    /**
     * list users
     *
     * @return List of String usernames
     */
    public List<String> listUsers() {
        return retry(()->userService.listUsers(this.blockingStub));
    }
    /**
     * describe user
     *
     * @param request describe user request
     * @return DescribeUserResp
     */
    public DescribeUserResp describeUser(DescribeUserReq request) {
        return retry(()->userService.describeUser(this.blockingStub, request));
    }
    /**
     * create user
     *
     * @param request create user request
     */
    public void createUser(CreateUserReq request) {
        retry(()->userService.createUser(this.blockingStub, request));
    }
    /**
     * change password
     *
     * @param request change password request
     */
    public void updatePassword(UpdatePasswordReq request) {
        retry(()->userService.updatePassword(this.blockingStub, request));
    }
    /**
     * drop user
     *
     * @param request drop user request
     */
    public void dropUser(DropUserReq request) {
        retry(()->userService.dropUser(this.blockingStub, request));
    }
    // role operations
    /**
     * list roles
     *
     * @return List of String role names
     */
    public List<String> listRoles() {
        return retry(()->roleService.listRoles(this.blockingStub));
    }
    /**
     * describe role
     *
     * @param request describe role request
     * @return DescribeRoleResp
     */
    public DescribeRoleResp describeRole(DescribeRoleReq request) {
        return retry(()->roleService.describeRole(this.blockingStub, request));
    }
    /**
     * create role
     *
     * @param request create role request
     */
    public void createRole(CreateRoleReq request) {
        retry(()->roleService.createRole(this.blockingStub, request));
    }
    /**
     * drop role
     *
     * @param request drop role request
     */
    public void dropRole(DropRoleReq request) {
        retry(()->roleService.dropRole(this.blockingStub, request));
    }
    /**
     * grant privilege
     *
     * @param request grant privilege request
     */
    public void grantPrivilege(GrantPrivilegeReq request) {
        retry(()->roleService.grantPrivilege(this.blockingStub, request));
    }
    /**
     * revoke privilege
     *
     * @param request revoke privilege request
     */
    public void revokePrivilege(RevokePrivilegeReq request) {
        retry(()->roleService.revokePrivilege(this.blockingStub, request));
    }
    /**
     * grant role
     *
     * @param request grant role request
     */
    public void grantRole(GrantRoleReq request) {
        retry(()->roleService.grantRole(this.blockingStub, request));
    }
    /**
     * revoke role
     *
     * @param request revoke role request
     */
    public void revokeRole(RevokeRoleReq request) {
        retry(()->roleService.revokeRole(this.blockingStub, request));
    }

    // Utility Operations

    /**
     * create aliases
     *
     * @param request create alias request
     */
    public void createAlias(CreateAliasReq request) {
        retry(()->utilityService.createAlias(this.blockingStub, request));
    }
    /**
     * drop aliases
     *
     * @param request drop alias request
     */
    public void dropAlias(DropAliasReq request) {
        retry(()->utilityService.dropAlias(this.blockingStub, request));
    }
    /**
     * alter aliases
     *
     * @param request alter alias request
     */
    public void alterAlias(AlterAliasReq request) {
        retry(()->utilityService.alterAlias(this.blockingStub, request));
    }
    /**
     * list aliases
     *
     * @param request list aliases request
     * @return List of String alias names
     */
    public ListAliasResp listAliases(ListAliasesReq request) {
        return retry(()->utilityService.listAliases(this.blockingStub, request));
    }
    /**
     * describe aliases
     *
     * @param request describe alias request
     * @return DescribeAliasResp
     */
    public DescribeAliasResp describeAlias(DescribeAliasReq request) {
        return retry(()->utilityService.describeAlias(this.blockingStub, request));
    }

    /**
     * Get server version
     *
     * @return String
     */
    public String getServerVersion() {
        return retry(()->clientUtils.getServerVersion(this.blockingStub));
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

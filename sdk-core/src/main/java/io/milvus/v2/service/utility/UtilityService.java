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

package io.milvus.v2.service.utility;

import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.*;
import io.milvus.v2.common.CompactionPlan;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service for utility operations, such as flushing, compacting, aliases, health checks,
 * and segment information queries.
 */
public class UtilityService extends BaseService {
    /**
     * Returns the Milvus server version, optionally including build details.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get server version request
     * @return the get server version response
     */
    public GetServerVersionResp getServerVersion(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                 GetServerVersionReq request) {
        if (Boolean.TRUE.equals(request.getDetail())) {
            ConnectResponse response = blockingStub.connect(ConnectRequest.newBuilder().build());
            rpcUtils.handleResponse("Get server version", response.getStatus());
            ServerInfo info = response.getServerInfo();
            return GetServerVersionResp.builder()
                    .version(info.getBuildTags())
                    .buildTime(info.getBuildTime())
                    .gitCommit(info.getGitCommit())
                    .goVersion(info.getGoVersion())
                    .deployMode(info.getDeployMode())
                    .build();
        }

        GetVersionResponse response = blockingStub.getVersion(GetVersionRequest.newBuilder().build());
        rpcUtils.handleResponse("Get server version", response.getStatus());
        return GetServerVersionResp.builder()
                .version(response.getVersion())
                .build();
    }

    /**
     * Flushes the specified collections, returning the flushed segment IDs and flush timestamps.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the flush request
     * @return the flush response
     */
    public FlushResp flush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushReq request) {
        String dbName = request.getDatabaseName();
        List<String> collectionNames = request.getCollectionNames();
        String title = String.format("Flush collections: '%s' in database: '%s'", collectionNames, dbName);
        if (collectionNames.isEmpty()) {
            // consistent with python sdk behavior, throw an error if collection names list is null or empty
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Collection name list can not be null or empty");
        }

        FlushRequest.Builder builder = FlushRequest.newBuilder()
                .addAllCollectionNames(collectionNames);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        FlushResponse response = blockingStub.flush(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        Map<String, LongArray> rpcCollSegIDs = response.getCollSegIDsMap();
        Map<String, List<Long>> collectionSegmentIDs = new HashMap<>();
        rpcCollSegIDs.forEach((key, value) -> {
            collectionSegmentIDs.put(key, value.getDataList());
        });
        Map<String, Long> collectionFlushTs = response.getCollFlushTsMap();
        return FlushResp.builder()
                .databaseName(response.getDbName())
                .collectionSegmentIDs(collectionSegmentIDs)
                .collectionFlushTs(collectionFlushTs)
                .build();
    }

    /**
     * Flushes all collections in the given database.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the flush all request
     * @return the flush all response
     */
    public FlushAllResp flushAll(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushAllReq request) {
        String dbName = request.getDatabaseName();
        String title = String.format("Flush all in database: '%s'", dbName);

        FlushAllRequest.Builder builder = FlushAllRequest.newBuilder();
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        FlushAllResponse response = blockingStub.flushAll(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        return FlushAllResp.builder()
                .flushAllTs(response.getFlushAllTs())
                .build();
    }

    /**
     * Returns whether the flush-all operation referenced by the given timestamp has completed.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get flush all state request
     * @return the get flush all state response
     */
    public GetFlushAllStateResp getFlushAllState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                 GetFlushAllStateReq request) {
        String dbName = request.getDatabaseName();
        String title = String.format("Get flush all state in database: '%s'", dbName);

        GetFlushAllStateRequest.Builder builder = GetFlushAllStateRequest.newBuilder()
                .setFlushAllTs(request.getFlushAllTs());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        GetFlushAllStateResponse response = blockingStub.getFlushAllState(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        return GetFlushAllStateResp.builder()
                .flushed(response.getFlushed())
                .build();
    }

    /**
     * Waits until the flushed segments referenced by the given flush response are flushed.
     * This method is for internal use and is not exposed to users.
     *
     * @param blockingStub the gRPC blocking stub
     * @param flushResp the flush response returned by {@link #flush(MilvusServiceGrpc.MilvusServiceBlockingStub, FlushReq)}
     * @return {@code null}
     */
    public Void waitFlush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushResp flushResp) {
        Map<String, List<Long>> collectionSegmentIDs = flushResp.getCollectionSegmentIDs();
        Map<String, Long> collectionFlushTs = flushResp.getCollectionFlushTs();
        collectionSegmentIDs.forEach((collectionName, segmentIDs) -> {
            if (collectionFlushTs.containsKey(collectionName)) {
                Long flushTs = collectionFlushTs.get(collectionName);
                boolean flushed = false;
                while (!flushed) {
                    logger.debug("Flush wait check: collection={}, segment_count={}, flush_ts={}",
                            collectionName, segmentIDs.size(), flushTs);
                    GetFlushStateResponse flushResponse = blockingStub.getFlushState(GetFlushStateRequest.newBuilder()
                            .setDbName(flushResp.getDatabaseName())
                            .addAllSegmentIDs(segmentIDs)
                            .setFlushTs(flushTs)
                            .build());

                    flushed = flushResponse.getFlushed();
                    logger.debug("Flush wait result: collection={}, flushed={}", collectionName, flushed);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException t) {
                        Thread.currentThread().interrupt();
                        logger.warn("Interrupted while waiting for flush", t);
                        break;
                    }
                }
            }
        });

        return null;
    }

    /**
     * Waits until the flush-all operation referenced by the given response completes.
     * This method is for internal use and is not exposed to users.
     *
     * @param blockingStub the gRPC blocking stub
     * @param flushAllResp the flush all response returned by {@link #flushAll(MilvusServiceGrpc.MilvusServiceBlockingStub, FlushAllReq)}
     * @param request the original flush all request providing the wait timeout
     * @return {@code null}
     */
    public Void waitFlushAll(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushAllResp flushAllResp,
                             FlushAllReq request) {
        boolean flushed = false;
        long start = System.currentTimeMillis();
        while (!flushed) {
            GetFlushAllStateResp flushAllStateResp = getFlushAllState(blockingStub, GetFlushAllStateReq.builder()
                    .databaseName(request.getDatabaseName())
                    .flushAllTs(flushAllResp.getFlushAllTs())
                    .build());

            flushed = Boolean.TRUE.equals(flushAllStateResp.getFlushed());
            if (!flushed) {
                Long timeout = request.getWaitFlushedTimeoutMs();
                if (timeout != null && timeout > 0L && System.currentTimeMillis() - start > timeout) {
                    throw new MilvusClientException(ErrorCode.CLIENT_ERROR,
                            "wait for flush all timeout, flush_all_ts: " + flushAllResp.getFlushAllTs());
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException t) {
                    Thread.currentThread().interrupt();
                    throw new MilvusClientException(ErrorCode.CLIENT_ERROR, "Interrupted while waiting for flush all");
                }
            }
        }

        return null;
    }

    /**
     * Compacts the specified collection and returns the compaction ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the compact request
     * @return the compact response
     */
    public CompactResp compact(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CompactReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Compact collection: '%s' in database: '%s'", collectionName, dbName);

        DescribeCollectionRequest.Builder descBuilder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(dbName)) {
            descBuilder.setDbName(dbName);
        }
        DescribeCollectionResponse descResponse = blockingStub.describeCollection(descBuilder.build());
        rpcUtils.handleResponse(title, descResponse.getStatus());

        ManualCompactionRequest.Builder builder = ManualCompactionRequest.newBuilder()
                .setCollectionID(descResponse.getCollectionID())
                .setMajorCompaction(request.getIsClustering())
                .setL0Compaction(request.getIsL0());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (request.getTargetSizeInMB() != null) {
            builder.setTargetSize(request.getTargetSizeInMB());
        }
        ManualCompactionResponse response = blockingStub.manualCompaction(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        return CompactResp.builder()
                .compactionID(response.getCompactionID())
                .build();
    }

    /**
     * Returns the state of the compaction identified by the given compaction ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get compaction state request
     * @return the get compaction state response
     */
    public GetCompactionStateResp getCompactionState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                     GetCompactionStateReq request) {
        String title = "Get compaction state";
        GetCompactionStateRequest getRequest = GetCompactionStateRequest.newBuilder()
                .setCompactionID(request.getCompactionID())
                .build();
        GetCompactionStateResponse response = blockingStub.getCompactionState(getRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        return GetCompactionStateResp.builder()
                .state(CompactionState.valueOf(response.getState().name()))
                .executingPlanNo(response.getExecutingPlanNo())
                .timeoutPlanNo(response.getTimeoutPlanNo())
                .completedPlanNo(response.getCompletedPlanNo())
                .build();
    }

    /**
     * Returns the merge plans of the compaction identified by the given compaction ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get compaction plans request
     * @return the get compaction plans response
     */
    public GetCompactionPlansResp getCompactionPlans(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                     GetCompactionPlansReq request) {
        String title = "Get compaction plans";
        GetCompactionPlansRequest getRequest = GetCompactionPlansRequest.newBuilder()
                .setCompactionID(request.getCompactionID())
                .build();
        GetCompactionPlansResponse response = blockingStub.getCompactionStateWithPlans(getRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        List<CompactionPlan> plans = new ArrayList<>();
        List<CompactionMergeInfo> infos = response.getMergeInfosList();
        infos.forEach(info -> {
            plans.add(CompactionPlan.builder()
                    .target(info.getTarget())
                    .sources(info.getSourcesList())
                    .build());
        });

        return GetCompactionPlansResp.builder()
                .compactionId(request.getCompactionID())
                .state(CompactionState.valueOf(response.getState().name()))
                .plans(plans)
                .build();
    }

    /**
     * Creates an alias for the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the create alias request
     * @return {@code null}
     */
    public Void createAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateAliasReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String alias = request.getAlias();
        String title = String.format("Create alias '%s' of collection: '%s' in database: '%s' ", alias, collectionName, dbName);
        CreateAliasRequest.Builder createAliasRequestBuilder = CreateAliasRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAlias(alias);
        if (StringUtils.isNotEmpty(dbName)) {
            createAliasRequestBuilder.setDbName(dbName);
        }

        Status status = blockingStub.createAlias(createAliasRequestBuilder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, alias);
        CollectionTsCache.getInstance().copy(
                getEndpoint(), actualDbName(dbName), collectionName,
                actualDbName(dbName), alias);

        return null;
    }

    /**
     * Drops the specified alias.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop alias request
     * @return {@code null}
     */
    public Void dropAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropAliasReq request) {
        String dbName = request.getDatabaseName();
        String alias = request.getAlias();
        String title = String.format("Drop aliases '%s' in database: '%s'", alias, dbName);
        DropAliasRequest.Builder dropAliasRequestBuilder = DropAliasRequest.newBuilder()
                .setAlias(alias);
        if (StringUtils.isNotEmpty(dbName)) {
            dropAliasRequestBuilder.setDbName(dbName);
        }
        Status status = blockingStub.dropAlias(dropAliasRequestBuilder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, alias);
        invalidateTsCache(dbName, alias);

        return null;
    }

    /**
     * Alters an alias so that it points to another collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the alter alias request
     * @return {@code null}
     */
    public Void alterAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterAliasReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String alias = request.getAlias();
        String title = String.format("Alter alias '%s' of collection: '%s' in database: '%s'", alias, collectionName, dbName);
        AlterAliasRequest.Builder alterAliasRequestBuilder = AlterAliasRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAlias(alias);
        if (StringUtils.isNotEmpty(dbName)) {
            alterAliasRequestBuilder.setDbName(dbName);
        }

        Status status = blockingStub.alterAlias(alterAliasRequestBuilder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, alias);
        CollectionTsCache.getInstance().copy(
                getEndpoint(), actualDbName(dbName), collectionName,
                actualDbName(dbName), alias);

        return null;
    }

    /**
     * Describes the specified alias, returning the collection it points to.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the describe alias request
     * @return the describe alias response
     */
    public DescribeAliasResp describeAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeAliasReq request) {
        String dbName = request.getDatabaseName();
        String alias = request.getAlias();
        String title = String.format("Describe alias '%s' in database: '%s'", alias, dbName);
        DescribeAliasRequest.Builder builder = DescribeAliasRequest.newBuilder()
                .setAlias(alias);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        DescribeAliasResponse response = blockingStub.describeAlias(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return DescribeAliasResp.builder()
                .databaseName(response.getDbName())
                .collectionName(response.getCollection())
                .alias(response.getAlias())
                .build();
    }

    /**
     * Lists the aliases of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the list aliases request
     * @return the list aliases response
     */
    public ListAliasResp listAliases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListAliasesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("List alias of collection: '%s' in database: '%s'", collectionName, dbName);
        ListAliasesRequest.Builder builder = ListAliasesRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        ListAliasesResponse response = blockingStub.listAliases(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return ListAliasResp.builder()
                .collectionName(response.getCollectionName())
                .alias(response.getAliasesList())
                .dbName(response.getDbName())
                .build();
    }

    /**
     * Checks the health of the Milvus server, returning any quota states or reasons of unhealthiness.
     *
     * @param blockingStub the gRPC blocking stub
     * @return the check health response
     */
    public CheckHealthResp checkHealth(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "Check health";
        CheckHealthResponse response = blockingStub.checkHealth(CheckHealthRequest.newBuilder().build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<String> states = new ArrayList<>();
        response.getQuotaStatesList().forEach(s -> states.add(s.name()));
        return CheckHealthResp.builder()
                .isHealthy(response.getIsHealthy())
                .reasons(response.getReasonsList().stream().collect(Collectors.toList()))
                .quotaStates(states)
                .build();
    }

    /**
     * Returns the persistent segment information of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get persistent segment info request
     * @return the get persistent segment info response
     */
    public GetPersistentSegmentInfoResp getPersistentSegmentInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                                 GetPersistentSegmentInfoReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Get persistent segment info in collection: '%s' in database: '%s'", collectionName, dbName);
        GetPersistentSegmentInfoRequest.Builder builder = GetPersistentSegmentInfoRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        GetPersistentSegmentInfoResponse response = blockingStub.getPersistentSegmentInfo(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<GetPersistentSegmentInfoResp.PersistentSegmentInfo> segmentInfos = new ArrayList<>();
        response.getInfosList().forEach(info -> {
            segmentInfos.add(GetPersistentSegmentInfoResp.PersistentSegmentInfo.builder()
                    .segmentID(info.getSegmentID())
                    .collectionID(info.getCollectionID())
                    .partitionID(info.getPartitionID())
                    .collectionName(collectionName)
                    .numOfRows(info.getNumRows())
                    .state(info.getState().name())
                    .level(info.getLevel().name())
                    .storageVersion(info.getStorageVersion())
                    .isSorted(info.getIsSorted())
                    .build());
        });
        return GetPersistentSegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }

    /**
     * Returns the query segment information of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get query segment info request
     * @return the get query segment info response
     */
    public GetQuerySegmentInfoResp getQuerySegmentInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                       GetQuerySegmentInfoReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Get query segment info in collection: '%s' in database: '%s'", collectionName, dbName);
        GetQuerySegmentInfoRequest.Builder builder = GetQuerySegmentInfoRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        GetQuerySegmentInfoResponse response = blockingStub.getQuerySegmentInfo(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<GetQuerySegmentInfoResp.QuerySegmentInfo> segmentInfos = new ArrayList<>();
        response.getInfosList().forEach(info -> {
            segmentInfos.add(GetQuerySegmentInfoResp.QuerySegmentInfo.builder()
                    .collectionName(request.getCollectionName())
                    .segmentID(info.getSegmentID())
                    .collectionID(info.getCollectionID())
                    .partitionID(info.getPartitionID())
                    .memSize(info.getMemSize())
                    .numOfRows(info.getNumRows())
                    .indexName(info.getIndexName())
                    .indexID(info.getIndexID())
                    .state(info.getState().name())
                    .level(info.getLevel().name())
                    .nodeIDs(info.getNodeIdsList())
                    .storageVersion(info.getStorageVersion())
                    .isSorted(info.getIsSorted())
                    .build());
        });
        return GetQuerySegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }

    /**
     * Refreshes an external collection and returns the refresh job ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the refresh external collection request
     * @return the refresh external collection response
     */
    public RefreshExternalCollectionResp refreshExternalCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                                    RefreshExternalCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("RefreshExternalCollection '%s' in database: '%s'", collectionName, dbName);

        RefreshExternalCollectionRequest.Builder builder = RefreshExternalCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setExternalSource(request.getExternalSource())
                .setExternalSpec(JsonUtils.toJsonString(request.getExternalSpec()));
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        RefreshExternalCollectionResponse response = blockingStub.refreshExternalCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return RefreshExternalCollectionResp.builder()
                .jobId(response.getJobId())
                .build();
    }

    /**
     * Returns the progress of an external collection refresh job.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get refresh external collection progress request
     * @return the get refresh external collection progress response
     */
    public GetRefreshExternalCollectionProgressResp getRefreshExternalCollectionProgress(
            MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
            GetRefreshExternalCollectionProgressReq request) {
        String title = String.format("GetRefreshExternalCollectionProgress jobId: %d", request.getJobId());

        GetRefreshExternalCollectionProgressRequest grpcRequest = GetRefreshExternalCollectionProgressRequest.newBuilder()
                .setJobId(request.getJobId())
                .build();

        GetRefreshExternalCollectionProgressResponse response = blockingStub.getRefreshExternalCollectionProgress(grpcRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        return GetRefreshExternalCollectionProgressResp.builder()
                .jobInfo(convertJobInfo(response.getJobInfo()))
                .build();
    }

    /**
     * Lists the external collection refresh jobs of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the list refresh external collection jobs request
     * @return the list refresh external collection jobs response
     */
    public ListRefreshExternalCollectionJobsResp listRefreshExternalCollectionJobs(
            MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
            ListRefreshExternalCollectionJobsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("ListRefreshExternalCollectionJobs '%s' in database: '%s'", collectionName, dbName);

        ListRefreshExternalCollectionJobsRequest.Builder builder = ListRefreshExternalCollectionJobsRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        ListRefreshExternalCollectionJobsResponse response = blockingStub.listRefreshExternalCollectionJobs(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo> jobs = new ArrayList<>();
        for (io.milvus.grpc.RefreshExternalCollectionJobInfo job : response.getJobsList()) {
            jobs.add(convertJobInfo(job));
        }
        return ListRefreshExternalCollectionJobsResp.builder()
                .jobs(jobs)
                .build();
    }

    private io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo convertJobInfo(io.milvus.grpc.RefreshExternalCollectionJobInfo info) {
        return io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo.builder()
                .jobId(info.getJobId())
                .collectionName(info.getCollectionName())
                .state(info.getState().name())
                .progress((int) info.getProgress())
                .reason(info.getReason())
                .externalSource(info.getExternalSource())
                .externalSpec(info.getExternalSpec())
                .startTime(info.getStartTime())
                .endTime(info.getEndTime())
                .build();
    }

    /**
     * Adds a file resource with the given name and path to the server.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the add file resource request
     * @return {@code null}
     */
    public Void addFileResource(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                AddFileResourceReq request) {
        if (StringUtils.isEmpty(request.getName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "File resource name cannot be null or empty");
        }
        if (StringUtils.isEmpty(request.getPath())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "File resource path cannot be null or empty");
        }
        String title = String.format("AddFileResource name: '%s', path: '%s'", request.getName(), request.getPath());

        AddFileResourceRequest grpcRequest = AddFileResourceRequest.newBuilder()
                .setName(request.getName())
                .setPath(request.getPath())
                .build();

        Status status = blockingStub.addFileResource(grpcRequest);
        rpcUtils.handleResponse(title, status);
        return null;
    }

    /**
     * Removes the file resource with the given name from the server.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the remove file resource request
     * @return {@code null}
     */
    public Void removeFileResource(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                   RemoveFileResourceReq request) {
        if (StringUtils.isEmpty(request.getName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "File resource name cannot be null or empty");
        }
        String title = String.format("RemoveFileResource name: '%s'", request.getName());

        RemoveFileResourceRequest grpcRequest = RemoveFileResourceRequest.newBuilder()
                .setName(request.getName())
                .build();

        Status status = blockingStub.removeFileResource(grpcRequest);
        rpcUtils.handleResponse(title, status);
        return null;
    }

    /**
     * Lists all file resources on the server.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the list file resources request
     * @return the list file resources response
     */
    public ListFileResourcesResp listFileResources(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                   ListFileResourcesReq request) {
        String title = "ListFileResources";

        ListFileResourcesRequest grpcRequest = ListFileResourcesRequest.newBuilder().build();

        ListFileResourcesResponse response = blockingStub.listFileResources(grpcRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        List<io.milvus.v2.service.utility.response.FileResourceInfo> resources = new ArrayList<>();
        for (io.milvus.grpc.FileResourceInfo info : response.getResourcesList()) {
            resources.add(io.milvus.v2.service.utility.response.FileResourceInfo.builder()
                    .name(info.getName())
                    .path(info.getPath())
                    .build());
        }
        return ListFileResourcesResp.builder()
                .resources(resources)
                .build();
    }
}

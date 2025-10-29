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

import io.milvus.grpc.*;
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
import java.util.stream.Collectors;

public class UtilityService extends BaseService {
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

    // this method is internal use, not expose to user
    public Void waitFlush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushResp flushResp) {
        Map<String, List<Long>> collectionSegmentIDs = flushResp.getCollectionSegmentIDs();
        Map<String, Long> collectionFlushTs = flushResp.getCollectionFlushTs();
        collectionSegmentIDs.forEach((collectionName, segmentIDs) -> {
            if (collectionFlushTs.containsKey(collectionName)) {
                Long flushTs = collectionFlushTs.get(collectionName);
                boolean flushed = false;
                while (!flushed) {
                    GetFlushStateResponse flushResponse = blockingStub.getFlushState(GetFlushStateRequest.newBuilder()
                            .setDbName(flushResp.getDatabaseName())
                            .addAllSegmentIDs(segmentIDs)
                            .setFlushTs(flushTs)
                            .build());

                    flushed = flushResponse.getFlushed();
                }
            }
        });

        return null;
    }

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
                .setMajorCompaction(request.getIsClustering());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        ManualCompactionResponse response = blockingStub.manualCompaction(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        return CompactResp.builder()
                .compactionID(response.getCompactionID())
                .build();
    }

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

        return null;
    }

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

        return null;
    }

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

        return null;
    }

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
                .build();
    }

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
                    .numOfRows(info.getNumRows())
                    .state(info.getState().name())
                    .level(info.getLevel().name())
                    .isSorted(info.getIsSorted())
                    .build());
        });
        return GetPersistentSegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }

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
                    .isSorted(info.getIsSorted())
                    .build());
        });
        return GetQuerySegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }
}

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

import java.util.*;
import java.util.stream.Collectors;

public class UtilityService extends BaseService {
    public FlushResp flush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushReq request) {
        List<String> collectionNames = request.getCollectionNames();
        String title = String.format("Flush collections %s", collectionNames);
        if (collectionNames.isEmpty()) {
            // consistent with python sdk behavior, throw an error if collection names list is null or empty
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Collection name list can not be null or empty");
        }

        FlushRequest flushRequest = io.milvus.grpc.FlushRequest.newBuilder()
                .addAllCollectionNames(collectionNames)
                .build();
        FlushResponse response = blockingStub.flush(flushRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        Map<String, io.milvus.grpc.LongArray> rpcCollSegIDs = response.getCollSegIDsMap();
        Map<String, List<Long>> collectionSegmentIDs = new HashMap<>();
        rpcCollSegIDs.forEach((key, value)->{
            collectionSegmentIDs.put(key, value.getDataList());
        });
        Map<String, Long> collectionFlushTs = response.getCollFlushTsMap();
        return FlushResp.builder()
                .collectionSegmentIDs(collectionSegmentIDs)
                .collectionFlushTs(collectionFlushTs)
                .build();
    }

    // this method is internal use, not expose to user
    public Void waitFlush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                          Map<String, List<Long>> collectionSegmentIDs,
                          Map<String, Long> collectionFlushTs) {
        collectionSegmentIDs.forEach((collectionName, segmentIDs)->{
            if (collectionFlushTs.containsKey(collectionName)) {
                Long flushTs = collectionFlushTs.get(collectionName);
                boolean flushed = false;
                while (!flushed) {
                    GetFlushStateResponse flushResponse = blockingStub.getFlushState(GetFlushStateRequest.newBuilder()
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
        String title = String.format("Compact collection %s", request.getCollectionName());

        DescribeCollectionResponse descResponse = blockingStub.describeCollection(DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build());
        rpcUtils.handleResponse(title, descResponse.getStatus());

        io.milvus.grpc.ManualCompactionRequest compactRequest = io.milvus.grpc.ManualCompactionRequest.newBuilder()
                .setCollectionID(descResponse.getCollectionID())
                .setMajorCompaction(request.getIsClustering())
                .build();
        io.milvus.grpc.ManualCompactionResponse response = blockingStub.manualCompaction(compactRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        return CompactResp.builder()
                .compactionID(response.getCompactionID())
                .build();
    }

    public GetCompactionStateResp getCompactionState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                     GetCompactionStateReq request) {
        String title = "GetCompactionState";
        io.milvus.grpc.GetCompactionStateRequest getRequest = io.milvus.grpc.GetCompactionStateRequest.newBuilder()
                .setCompactionID(request.getCompactionID())
                .build();
        io.milvus.grpc.GetCompactionStateResponse response = blockingStub.getCompactionState(getRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        return GetCompactionStateResp.builder()
                .state(CompactionState.valueOf(response.getState().name()))
                .executingPlanNo(response.getExecutingPlanNo())
                .timeoutPlanNo(response.getTimeoutPlanNo())
                .completedPlanNo(response.getCompletedPlanNo())
                .build();
    }

    public Void createAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateAliasReq request) {
        String title = String.format("CreateAlias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.CreateAliasRequest createAliasRequest = io.milvus.grpc.CreateAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.createAlias(createAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void dropAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropAliasReq request) {
        String title = String.format("DropAlias %s", request.getAlias());
        io.milvus.grpc.DropAliasRequest dropAliasRequest = io.milvus.grpc.DropAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.dropAlias(dropAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void alterAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterAliasReq request) {
        String title = String.format("AlterAlias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.AlterAliasRequest alterAliasRequest = io.milvus.grpc.AlterAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.alterAlias(alterAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public DescribeAliasResp describeAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeAliasReq request) {
        String title = String.format("DescribeAlias %s", request.getAlias());
        io.milvus.grpc.DescribeAliasRequest describeAliasRequest = io.milvus.grpc.DescribeAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.DescribeAliasResponse response = blockingStub.describeAlias(describeAliasRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return DescribeAliasResp.builder()
                .collectionName(response.getCollection())
                .alias(response.getAlias())
                .build();
    }

    public ListAliasResp listAliases(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListAliasesReq request) {
        String title = "ListAliases";
        io.milvus.grpc.ListAliasesRequest listAliasesRequest = io.milvus.grpc.ListAliasesRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        io.milvus.grpc.ListAliasesResponse response = blockingStub.listAliases(listAliasesRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return ListAliasResp.builder()
                .collectionName(response.getCollectionName())
                .alias(response.getAliasesList())
                .build();
    }

    public CheckHealthResp checkHealth(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        String title = "CheckHealth";
        CheckHealthResponse response = blockingStub.checkHealth(CheckHealthRequest.newBuilder().build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<String> states = new ArrayList<>();
        response.getQuotaStatesList().forEach(s->states.add(s.name()));
        return CheckHealthResp.builder()
                .isHealthy(response.getIsHealthy())
                .reasons(response.getReasonsList().stream().collect(Collectors.toList()))
                .quotaStates(states)
                .build();
    }

    public GetPersistentSegmentInfoResp getPersistentSegmentInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                    GetPersistentSegmentInfoReq request) {
        String title = String.format("GetPersistentSegmentInfo collectionName %s", request.getCollectionName());
        GetPersistentSegmentInfoResponse response = blockingStub.getPersistentSegmentInfo(GetPersistentSegmentInfoRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<GetPersistentSegmentInfoResp.PersistentSegmentInfo> segmentInfos = new ArrayList<>();
        response.getInfosList().forEach(info->{segmentInfos.add(GetPersistentSegmentInfoResp.PersistentSegmentInfo.builder()
                .segmentID(info.getSegmentID())
                .collectionID(info.getCollectionID())
                .partitionID(info.getPartitionID())
                .numOfRows(info.getNumRows())
                .state(info.getState().name())
                .level(info.getLevel().name())
                .isSorted(info.getIsSorted())
                .build());});
        return GetPersistentSegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }

    public GetQuerySegmentInfoResp getQuerySegmentInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                            GetQuerySegmentInfoReq request) {
        String title = String.format("GetQuerySegmentInfo collectionName %s", request.getCollectionName());
        GetQuerySegmentInfoResponse response = blockingStub.getQuerySegmentInfo(GetQuerySegmentInfoRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<GetQuerySegmentInfoResp.QuerySegmentInfo> segmentInfos = new ArrayList<>();
        response.getInfosList().forEach(info->{segmentInfos.add(GetQuerySegmentInfoResp.QuerySegmentInfo.builder()
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
                .build());});
        return GetQuerySegmentInfoResp.builder()
                .segmentInfos(segmentInfos)
                .build();
    }
}

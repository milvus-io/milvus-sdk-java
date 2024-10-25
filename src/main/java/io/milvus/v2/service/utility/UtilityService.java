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

import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;

import java.util.*;

public class UtilityService extends BaseService {
    public FlushResp flush(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, FlushReq request) {
        String title = String.format("Flush collection %s", request.getCollectionName());
        io.milvus.grpc.FlushRequest flushRequest = io.milvus.grpc.FlushRequest.newBuilder()
                .addCollectionNames(request.getCollectionName())
                .build();
        FlushResponse response = blockingStub.flush(flushRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        Map<String, List<Long>> collectionSegmentIDs = new HashMap<>();
        Map<String, io.milvus.grpc.LongArray> rpcCollSegIDs = response.getCollSegIDsMap();
        rpcCollSegIDs.forEach((key, value)->{
            collectionSegmentIDs.put(key, value.getDataList());
        });

        Map<String, List<Long>> flushedCollectionSegmentIDs = new HashMap<>();
        Map<String, io.milvus.grpc.LongArray> rpcFlushCollSegIDs = response.getFlushCollSegIDsMap();
        rpcFlushCollSegIDs.forEach((key, value)->{
            flushedCollectionSegmentIDs.put(key, value.getDataList());
        });

        return FlushResp.builder()
                .collectionSegmentIDs(collectionSegmentIDs)
                .flushedCollectionSegmentIDs(flushedCollectionSegmentIDs)
                .build();
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
        String title = "Get compaction state";
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
        String title = String.format("Create alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.CreateAliasRequest createAliasRequest = io.milvus.grpc.CreateAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.createAlias(createAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void dropAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropAliasReq request) {
        String title = String.format("Drop alias %s", request.getAlias());
        io.milvus.grpc.DropAliasRequest dropAliasRequest = io.milvus.grpc.DropAliasRequest.newBuilder()
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.dropAlias(dropAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void alterAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterAliasReq request) {
        String title = String.format("Alter alias %s for collection %s", request.getAlias(), request.getCollectionName());
        io.milvus.grpc.AlterAliasRequest alterAliasRequest = io.milvus.grpc.AlterAliasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setAlias(request.getAlias())
                .build();
        io.milvus.grpc.Status status = blockingStub.alterAlias(alterAliasRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public DescribeAliasResp describeAlias(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeAliasReq request) {
        String title = String.format("Describe alias %s", request.getAlias());
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
        String title = "List aliases";
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
}

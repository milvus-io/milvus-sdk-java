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

package io.milvus.v2.service.partition;

import io.milvus.grpc.*;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.partition.request.*;
import io.milvus.v2.service.partition.response.*;

import java.util.List;

public class PartitionService extends BaseService {
    public Void createPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreatePartitionReq request) {
        String title = String.format("Create partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        CreatePartitionRequest createPartitionRequest = io.milvus.grpc.CreatePartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        Status status = blockingStub.createPartition(createPartitionRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void dropPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropPartitionReq request) {
        String title = String.format("Drop partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        io.milvus.grpc.DropPartitionRequest dropPartitionRequest = io.milvus.grpc.DropPartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        Status status = blockingStub.dropPartition(dropPartitionRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Boolean hasPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HasPartitionReq request) {
        String title = String.format("Has partition %s in collection %s", request.getPartitionName(), request.getCollectionName());

        io.milvus.grpc.HasPartitionRequest hasPartitionRequest = io.milvus.grpc.HasPartitionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName()).build();

        io.milvus.grpc.BoolResponse boolResponse = blockingStub.hasPartition(hasPartitionRequest);
        rpcUtils.handleResponse(title, boolResponse.getStatus());

        return boolResponse.getValue();
    }

    public List<String> listPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListPartitionsReq request) {
        String title = String.format("List partitions in collection %s", request.getCollectionName());

        io.milvus.grpc.ShowPartitionsRequest showPartitionsRequest = io.milvus.grpc.ShowPartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName()).build();

        io.milvus.grpc.ShowPartitionsResponse showPartitionsResponse = blockingStub.showPartitions(showPartitionsRequest);
        rpcUtils.handleResponse(title, showPartitionsResponse.getStatus());

        return showPartitionsResponse.getPartitionNamesList();
    }

    public GetPartitionStatsResp getPartitionStats(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetPartitionStatsReq request) {
        String title = String.format("GetCollectionStatisticsRequest collectionName:%s", request.getCollectionName());
        GetPartitionStatisticsRequest getPartitionStatisticsRequest = GetPartitionStatisticsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName())
                .build();
        GetPartitionStatisticsResponse response = blockingStub.getPartitionStatistics(getPartitionStatisticsRequest);

        rpcUtils.handleResponse(title, response.getStatus());
        GetPartitionStatsResp getPartitionStatsResp = GetPartitionStatsResp.builder()
                .numOfEntities(response.getStatsList().stream().filter(stat -> stat.getKey().equals("row_count")).map(stat -> Long.parseLong(stat.getValue())).findFirst().get())
                .build();
        return getPartitionStatsResp;
    }

    public Void loadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadPartitionsReq request) {
        String title = String.format("Load partitions %s in collection %s", request.getPartitionNames(), request.getCollectionName());

        io.milvus.grpc.LoadPartitionsRequest loadPartitionsRequest = io.milvus.grpc.LoadPartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames())
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(request.getRefresh())
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(request.getSkipLoadDynamicField())
                .addAllResourceGroups(request.getResourceGroups())
                .build();
        Status status = blockingStub.loadPartitions(loadPartitionsRequest);
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadPartitions(blockingStub, request.getCollectionName(), request.getPartitionNames(), request.getTimeout());
        }

        return null;
    }

    public Void releasePartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ReleasePartitionsReq request) {
        String title = String.format("Release partitions %s in collection %s", request.getPartitionNames(), request.getCollectionName());

        io.milvus.grpc.ReleasePartitionsRequest releasePartitionsRequest = io.milvus.grpc.ReleasePartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames()).build();
        Status status = blockingStub.releasePartitions(releasePartitionsRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    private void WaitForLoadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                       String collectionName, List<String> partitions, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (true) {
            GetLoadingProgressResponse response = blockingStub.getLoadingProgress(GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitions)
                    .build());
            String title = String.format("GetLoadingProgressRequest collectionName:%s", collectionName);
            rpcUtils.handleResponse(title, response.getStatus());
            if (response.getProgress() >= 100) {
                return;
            }

            // Check if timeout is exceeded
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load partitions timeout");
            }
            // Wait for a certain period before checking again
            try {
                Thread.sleep(500); // Sleep for 0.5 second. Adjust this value as needed.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread was interrupted, failed to complete operation");
                return; // or handle interruption appropriately
            }
        }
    }
}

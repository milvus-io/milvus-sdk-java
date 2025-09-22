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
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class PartitionService extends BaseService {
    public Void createPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreatePartitionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        String title = String.format("Create partition: '%s' in collection: '%s' in database: '%s'",
                partitionName, collectionName, dbName);

        CreatePartitionRequest.Builder builder = CreatePartitionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.createPartition(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void dropPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropPartitionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        String title = String.format("Drop partition: '%s' in collection: '%s' in database: '%s'",
                partitionName, collectionName, dbName);

        DropPartitionRequest.Builder builder = DropPartitionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.dropPartition(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Boolean hasPartition(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HasPartitionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        String title = String.format("Has partition: '%s' in collection: '%s' in database: '%s'",
                partitionName, collectionName, dbName);

        HasPartitionRequest.Builder builder = HasPartitionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        BoolResponse boolResponse = blockingStub.hasPartition(builder.build());
        rpcUtils.handleResponse(title, boolResponse.getStatus());

        return boolResponse.getValue();
    }

    public List<String> listPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListPartitionsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("List partitions in collection: '%s' in database: '%s'", collectionName, dbName);

        ShowPartitionsRequest.Builder builder = ShowPartitionsRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        ShowPartitionsResponse showPartitionsResponse = blockingStub.showPartitions(builder.build());
        rpcUtils.handleResponse(title, showPartitionsResponse.getStatus());

        return showPartitionsResponse.getPartitionNamesList();
    }

    public GetPartitionStatsResp getPartitionStats(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                   GetPartitionStatsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        String title = String.format("Get statistics of partition: '%s' in collection: '%s' in database: '%s'",
                partitionName, collectionName, dbName);

        GetPartitionStatisticsRequest.Builder builder = GetPartitionStatisticsRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        GetPartitionStatisticsResponse response = blockingStub.getPartitionStatistics(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        GetPartitionStatsResp getPartitionStatsResp = GetPartitionStatsResp.builder()
                .numOfEntities(response.getStatsList().stream().filter(stat -> stat.getKey().equals("row_count"))
                        .map(stat -> Long.parseLong(stat.getValue())).findFirst().get())
                .build();
        return getPartitionStatsResp;
    }

    public Void loadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadPartitionsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        List<String> partitionNames = request.getPartitionNames();
        String title = String.format("Load partitions: %s in collection: '%s' in database: '%s'",
                partitionNames, collectionName, dbName);

        LoadPartitionsRequest.Builder builder = LoadPartitionsRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllPartitionNames(partitionNames)
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(request.getRefresh())
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(request.getSkipLoadDynamicField())
                .addAllResourceGroups(request.getResourceGroups());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.loadPartitions(builder.build());
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadPartitions(blockingStub, dbName, collectionName, partitionNames, request.getTimeout());
        }

        return null;
    }

    public Void releasePartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ReleasePartitionsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        List<String> partitionNames = request.getPartitionNames();
        String title = String.format("Release partitions: %s in collection: '%s' in database: '%s'",
                partitionNames, collectionName, dbName);

        ReleasePartitionsRequest.Builder builder = ReleasePartitionsRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllPartitionNames(partitionNames);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.releasePartitions(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    private void WaitForLoadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName,
                                       String collectionName, List<String> partitions, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (true) {
            GetLoadingProgressRequest.Builder builder = GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitions);
            if (StringUtils.isNotEmpty(dbName)) {
                builder.setDbName(dbName);
            }
            GetLoadingProgressResponse response = blockingStub.getLoadingProgress(builder.build());
            String title = String.format("Get loading progress of collection: '%s' in database: '%s'", collectionName, dbName);
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

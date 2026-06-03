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
import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
        boolean sync = Boolean.TRUE.equals(request.getSync());
        boolean refresh = Boolean.TRUE.equals(request.getRefresh());
        boolean skipLoadDynamicField = Boolean.TRUE.equals(request.getSkipLoadDynamicField());
        String title = String.format("Load partitions: %s in collection: '%s' in database: '%s'",
                partitionNames, collectionName, dbName);

        LoadPartitionsRequest.Builder builder = LoadPartitionsRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllPartitionNames(partitionNames)
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(refresh)
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(skipLoadDynamicField)
                .addAllResourceGroups(request.getResourceGroups());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub = blockingStub;
        if (request.getTimeout() != null && request.getTimeout() > 0) {
            tempBlockingStub = tempBlockingStub.withDeadlineAfter(request.getTimeout(), TimeUnit.MILLISECONDS);
        }
        Status status = tempBlockingStub.loadPartitions(builder.build());
        rpcUtils.handleResponse(title, status);
        if (sync) {
            waitForLoadPartitions(blockingStub, dbName, collectionName, partitionNames, request.getTimeout(), refresh);
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

    private void waitForLoadPartitions(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName,
                                       String collectionName, List<String> partitions, Long timeoutMs, boolean refreshLoad) {
        long startTime = System.currentTimeMillis();

        while (true) {
            GetLoadingProgressRequest.Builder builder = GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitions);
            if (StringUtils.isNotEmpty(dbName)) {
                builder.setDbName(dbName);
            }
            MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub = blockingStub;
            if (timeoutMs != null && timeoutMs > 0) {
                tempBlockingStub = tempBlockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
            }
            GetLoadingProgressResponse response = tempBlockingStub.getLoadingProgress(builder.build());
            String title = String.format("Get loading progress of collection: '%s' in database: '%s'", collectionName, dbName);
            rpcUtils.handleResponse(title, response.getStatus());
            long progress = refreshLoad ? response.getRefreshProgress() : response.getProgress();
            if (progress >= 100L) {
                return;
            }

            if (timeoutMs != null && timeoutMs > 0 && System.currentTimeMillis() - startTime > timeoutMs) {
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load partitions timeout");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread was interrupted, failed to complete operation");
                return;
            }
        }
    }
}

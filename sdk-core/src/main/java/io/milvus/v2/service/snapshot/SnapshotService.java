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

package io.milvus.v2.service.snapshot;

import io.milvus.grpc.*;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.snapshot.request.*;
import io.milvus.v2.service.snapshot.response.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class SnapshotService extends BaseService {
    public Void createSnapshot(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateSnapshotReq request) {
        requireNotEmpty(request.getSnapshotName(), "Snapshot name cannot be null or empty");
        requireNotEmpty(request.getCollectionName(), "Collection name cannot be null or empty");
        requireNonNegative(request.getCompactionProtectionSeconds(), "Compaction protection seconds cannot be negative");

        String dbName = request.getDatabaseName();
        String title = String.format("CreateSnapshot name: '%s', collection: '%s', database: '%s'",
                request.getSnapshotName(), request.getCollectionName(), dbName);
        CreateSnapshotRequest.Builder builder = CreateSnapshotRequest.newBuilder()
                .setName(request.getSnapshotName())
                .setDescription(StringUtils.defaultString(request.getDescription()))
                .setCollectionName(request.getCollectionName())
                .setCompactionProtectionSeconds(request.getCompactionProtectionSeconds());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status status = blockingStub.createSnapshot(builder.build());
        rpcUtils.handleResponse(title, status);
        return null;
    }

    public Void dropSnapshot(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropSnapshotReq request) {
        requireNotEmpty(request.getSnapshotName(), "Snapshot name cannot be null or empty");
        requireNotEmpty(request.getCollectionName(), "Collection name cannot be null or empty");

        String dbName = request.getDatabaseName();
        String title = String.format("DropSnapshot name: '%s', collection: '%s', database: '%s'",
                request.getSnapshotName(), request.getCollectionName(), dbName);
        DropSnapshotRequest.Builder builder = DropSnapshotRequest.newBuilder()
                .setName(request.getSnapshotName())
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status status = blockingStub.dropSnapshot(builder.build());
        rpcUtils.handleResponse(title, status);
        return null;
    }

    public ListSnapshotsResp listSnapshots(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ListSnapshotsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("ListSnapshots collection: '%s', database: '%s'", collectionName, dbName);
        ListSnapshotsRequest.Builder builder = ListSnapshotsRequest.newBuilder();
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (StringUtils.isNotEmpty(collectionName)) {
            builder.setCollectionName(collectionName);
        }

        ListSnapshotsResponse response = blockingStub.listSnapshots(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return ListSnapshotsResp.builder()
                .snapshots(new ArrayList<>(response.getSnapshotsList()))
                .build();
    }

    public DescribeSnapshotResp describeSnapshot(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeSnapshotReq request) {
        requireNotEmpty(request.getSnapshotName(), "Snapshot name cannot be null or empty");
        requireNotEmpty(request.getCollectionName(), "Collection name cannot be null or empty");

        String dbName = request.getDatabaseName();
        String title = String.format("DescribeSnapshot name: '%s', collection: '%s', database: '%s'",
                request.getSnapshotName(), request.getCollectionName(), dbName);
        DescribeSnapshotRequest.Builder builder = DescribeSnapshotRequest.newBuilder()
                .setName(request.getSnapshotName())
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        DescribeSnapshotResponse response = blockingStub.describeSnapshot(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return DescribeSnapshotResp.builder()
                .name(response.getName())
                .description(response.getDescription())
                .collectionName(response.getCollectionName())
                .partitionNames(new ArrayList<>(response.getPartitionNamesList()))
                .createTs(response.getCreateTs())
                .s3Location(response.getS3Location())
                .build();
    }

    public RestoreSnapshotResp restoreSnapshot(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RestoreSnapshotReq request) {
        requireNotEmpty(request.getSnapshotName(), "Snapshot name cannot be null or empty");
        requireNotEmpty(request.getSourceCollectionName(), "Source collection name cannot be null or empty");
        requireNotEmpty(request.getTargetCollectionName(), "Target collection name cannot be null or empty");

        String title = String.format("RestoreSnapshot name: '%s', source collection: '%s', target collection: '%s'",
                request.getSnapshotName(), request.getSourceCollectionName(), request.getTargetCollectionName());
        RestoreSnapshotRequest.Builder builder = RestoreSnapshotRequest.newBuilder()
                .setName(request.getSnapshotName())
                .setCollectionName(request.getSourceCollectionName())
                .setTargetCollectionName(request.getTargetCollectionName());
        if (StringUtils.isNotEmpty(request.getSourceDbName())) {
            builder.setDbName(request.getSourceDbName());
        }
        if (StringUtils.isNotEmpty(request.getTargetDbName())) {
            builder.setTargetDbName(request.getTargetDbName());
        }

        RestoreSnapshotResponse response = blockingStub.restoreSnapshot(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return RestoreSnapshotResp.builder()
                .jobId(response.getJobId())
                .build();
    }

    public GetRestoreSnapshotStateResp getRestoreSnapshotState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                               GetRestoreSnapshotStateReq request) {
        requirePositive(request.getJobId(), "Restore snapshot job ID must be positive");
        String title = String.format("GetRestoreSnapshotState jobId: %d", request.getJobId());
        GetRestoreSnapshotStateRequest grpcRequest = GetRestoreSnapshotStateRequest.newBuilder()
                .setJobId(request.getJobId())
                .build();

        GetRestoreSnapshotStateResponse response = blockingStub.getRestoreSnapshotState(grpcRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        return GetRestoreSnapshotStateResp.builder()
                .jobInfo(convertRestoreSnapshotJobInfo(response.getInfo()))
                .build();
    }

    public ListRestoreSnapshotJobsResp listRestoreSnapshotJobs(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                               ListRestoreSnapshotJobsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("ListRestoreSnapshotJobs collection: '%s', database: '%s'", collectionName, dbName);
        ListRestoreSnapshotJobsRequest.Builder builder = ListRestoreSnapshotJobsRequest.newBuilder();
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (StringUtils.isNotEmpty(collectionName)) {
            builder.setCollectionName(collectionName);
        }

        ListRestoreSnapshotJobsResponse response = blockingStub.listRestoreSnapshotJobs(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        List<RestoreSnapshotJobInfo> jobs = new ArrayList<>();
        for (io.milvus.grpc.RestoreSnapshotInfo job : response.getJobsList()) {
            jobs.add(convertRestoreSnapshotJobInfo(job));
        }
        return ListRestoreSnapshotJobsResp.builder()
                .jobs(jobs)
                .build();
    }

    public PinSnapshotDataResp pinSnapshotData(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, PinSnapshotDataReq request) {
        requireNotEmpty(request.getSnapshotName(), "Snapshot name cannot be null or empty");
        requireNotEmpty(request.getCollectionName(), "Collection name cannot be null or empty");
        requireNonNegative(request.getTtlSeconds(), "TTL seconds cannot be negative");

        String dbName = request.getDatabaseName();
        String title = String.format("PinSnapshotData name: '%s', collection: '%s', database: '%s'",
                request.getSnapshotName(), request.getCollectionName(), dbName);
        PinSnapshotDataRequest.Builder builder = PinSnapshotDataRequest.newBuilder()
                .setName(request.getSnapshotName())
                .setCollectionName(request.getCollectionName())
                .setTtlSeconds(request.getTtlSeconds());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        PinSnapshotDataResponse response = blockingStub.pinSnapshotData(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return PinSnapshotDataResp.builder()
                .pinId(response.getPinId())
                .build();
    }

    public Void unpinSnapshotData(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UnpinSnapshotDataReq request) {
        requirePositive(request.getPinId(), "Snapshot pin ID must be positive");
        String title = String.format("UnpinSnapshotData pinId: %d", request.getPinId());
        UnpinSnapshotDataRequest grpcRequest = UnpinSnapshotDataRequest.newBuilder()
                .setPinId(request.getPinId())
                .build();

        Status status = blockingStub.unpinSnapshotData(grpcRequest);
        rpcUtils.handleResponse(title, status);
        return null;
    }

    private RestoreSnapshotJobInfo convertRestoreSnapshotJobInfo(io.milvus.grpc.RestoreSnapshotInfo info) {
        return RestoreSnapshotJobInfo.builder()
                .jobId(info.getJobId())
                .snapshotName(info.getSnapshotName())
                .dbName(info.getDbName())
                .collectionName(info.getCollectionName())
                .state(info.getState().name())
                .progress(info.getProgress())
                .reason(info.getReason())
                .startTime(info.getStartTime())
                .timeCost(info.getTimeCost())
                .build();
    }

    private void requireNotEmpty(String value, String message) {
        if (StringUtils.isEmpty(value)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, message);
        }
    }

    private void requirePositive(Long value, String message) {
        if (value == null || value <= 0L) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, message);
        }
    }

    private void requireNonNegative(Long value, String message) {
        if (value == null || value < 0L) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, message);
        }
    }
}

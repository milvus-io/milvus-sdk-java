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

package io.milvus.client;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.StatusRuntimeException;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.common.utils.VectorUtils;
import io.milvus.exception.*;
import io.milvus.grpc.*;
import io.milvus.grpc.ObjectEntity;
import io.milvus.param.*;
import io.milvus.param.alias.*;
import io.milvus.param.bulkinsert.*;
import io.milvus.param.collection.*;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.control.*;
import io.milvus.param.credential.*;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.dml.*;
import io.milvus.param.highlevel.dml.response.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import io.milvus.param.resourcegroup.*;
import io.milvus.param.role.*;
import io.milvus.response.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);
    protected LogLevel logLevel = LogLevel.Info;

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    protected abstract boolean clientIsReady();

    private void waitForLoadingCollection(String databaseName, String collectionName, List<String> partitionNames,
                                          long waitingInterval, long timeout) throws IllegalResponseException {
        long tsBegin = System.currentTimeMillis();
        if (partitionNames == null || partitionNames.isEmpty()) {
            ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder()
                    .addCollectionNames(collectionName)
                    .setType(ShowType.InMemory);
            if (StringUtils.isNotEmpty(databaseName)) {
                builder.setDbName(databaseName);
            }
            ShowCollectionsRequest showCollectionRequest = builder.build();

            // Use showCollection() to check loading percentages of the collection.
            // If the inMemory percentage is 100, that means the collection has finished loading.
            // Otherwise, this thread will sleep a small interval and check again.
            // If waiting time exceed timeout, exist the circle
            while (true) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting load thread is timeout, loading process may not be finished");
                    break;
                }

                ShowCollectionsResponse response = blockingStub().showCollections(showCollectionRequest);
                int namesCount = response.getCollectionNamesCount();
                int percentagesCount = response.getInMemoryPercentagesCount();
                if (namesCount != 1) {
                    throw new IllegalResponseException("ShowCollectionsResponse is illegal. Collection count: "
                            + namesCount);
                }

                if (namesCount != percentagesCount) {
                    String msg = "ShowCollectionsResponse is illegal. Collection count: " + namesCount
                            + " memory percentages count: " + percentagesCount;
                    throw new IllegalResponseException(msg);
                }

                long percentage = response.getInMemoryPercentages(0);
                String responseCollection = response.getCollectionNames(0);
                if (responseCollection.compareTo(collectionName) == 0 && percentage >= 100) {
                    break;
                }

                try {
                    logDebug("Waiting load, interval: {} ms, percentage: {}%", waitingInterval, percentage);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting load thread is interrupted, loading process may not be finished");
                    break;
                }
            }

        } else {
            ShowPartitionsRequest.Builder builder = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitionNames);
            if (StringUtils.isNotEmpty(databaseName)) {
                builder.setDbName(databaseName);
            }
            ShowPartitionsRequest showPartitionsRequest = builder.setType(ShowType.InMemory).build();

            // Use showPartitions() to check loading percentages of all the partitions.
            // If each partition's  inMemory percentage is 100, that means all the partitions have finished loading.
            // Otherwise, this thread will sleep a small interval and check again.
            // If waiting time exceed timeout, exist the circle
            while (true) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting load thread is timeout, loading process may not be finished");
                    break;
                }

                ShowPartitionsResponse response = blockingStub().showPartitions(showPartitionsRequest);
                int namesCount = response.getPartitionNamesCount();
                int percentagesCount = response.getInMemoryPercentagesCount();
                if (namesCount != percentagesCount) {
                    String msg = "ShowPartitionsResponse is illegal. Partition count: " + namesCount
                            + " memory percentages count: " + percentagesCount;
                    throw new IllegalResponseException(msg);
                }

                // construct a hash map to check each partition's inMemory percentage by name
                Map<String, Long> percentages = new HashMap<>();
                for (int i = 0; i < response.getInMemoryPercentagesCount(); ++i) {
                    percentages.put(response.getPartitionNames(i), response.getInMemoryPercentages(i));
                }

                String partitionNoMemState = "";
                String partitionNotFullyLoad = "";
                boolean allLoaded = true;
                for (String name : partitionNames) {
                    if (!percentages.containsKey(name)) {
                        allLoaded = false;
                        partitionNoMemState = name;
                        break;
                    }
                    if (percentages.get(name) < 100L) {
                        allLoaded = false;
                        partitionNotFullyLoad = name;
                        break;
                    }
                }

                if (allLoaded) {
                    break;
                }

                try {
                    String msg = "Waiting load, interval: " + waitingInterval + "ms";
                    if (!partitionNoMemState.isEmpty()) {
                        msg += ("Partition " + partitionNoMemState + " has no memory state");
                    }
                    if (!partitionNotFullyLoad.isEmpty()) {
                        msg += ("Partition " + partitionNotFullyLoad + " has not fully loaded");
                    }
                    logDebug(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting load thread is interrupted, load process may not be finished");
                    break;
                }
            }
        }
    }

    private void waitForFlush(FlushResponse flushResponse, long waitingInterval, long timeout) {
        // The rpc api flush() return FlushResponse, but the returned segment ids maybe not yet persisted.
        // This method use getFlushState() to check segment state.
        // If all segments state become Flushed, then we say the sync flush action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        Map<String, LongArray> collectionSegIDs = flushResponse.getCollSegIDsMap();
        collectionSegIDs.forEach((collectionName, segmentIDs) -> {
            while (segmentIDs.getDataCount() > 0) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout * 1000) {
                    logWarning("Waiting flush thread is timeout, flush process may not be finished");
                    break;
                }

                GetFlushStateRequest getFlushStateRequest = GetFlushStateRequest.newBuilder()
                        .addAllSegmentIDs(segmentIDs.getDataList())
                        .build();
                GetFlushStateResponse response = blockingStub().getFlushState(getFlushStateRequest);
                if (response.getFlushed()) {
                    // if all segment of this collection has been flushed, break this circle and check next collection
                    String msg = segmentIDs.getDataCount() + " segments of " + collectionName + " has been flushed";
                    logDebug(msg);
                    break;
                }

                try {
                    String msg = "Waiting flush for " + collectionName + ", interval: " + waitingInterval + "ms";
                    logDebug(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting flush thread is interrupted, flush process may not be finished");
                    break;
                }
            }
        });
    }

    private void waitForFlushAll(FlushAllResponse flushAllResponse, long waitingInterval, long timeout) {
        // The rpc api flushAll() returns a FlushAllResponse, but the returned flushAllTs may not yet be persisted.
        // This method uses getFlushAllState() to check the flushAll state.
        // If getFlushAllState() returns Flushed, then we can say that the flushAll action is finished.
        // If the waiting time exceeds the timeout, exit the loop.
        long tsBegin = System.currentTimeMillis();
        long flushAllTs = flushAllResponse.getFlushAllTs();
        while (true) {
            long tsNow = System.currentTimeMillis();
            if ((tsNow - tsBegin) >= timeout * 1000) {
                logWarning("waitForFlushAll timeout");
                break;
            }

            GetFlushAllStateRequest getFlushAllStateRequest = GetFlushAllStateRequest.newBuilder()
                    .setFlushAllTs(flushAllTs)
                    .build();
            GetFlushAllStateResponse response = blockingStub().getFlushAllState(getFlushAllStateRequest);
            if (response.getFlushed()) {
                logDebug("waitForFlushAll done, all flushed!");
                break;
            }

            try {
                String msg = "waitForFlushAll, interval: " + waitingInterval + "ms";
                logDebug(msg);
                TimeUnit.MILLISECONDS.sleep(waitingInterval);
            } catch (InterruptedException e) {
                logWarning("waitForFlushAll interrupted");
                break;
            }
        }
    }

    private R<Boolean> waitForIndex(String databaseName, String collectionName, String indexName, String fieldName,
                                    long waitingInterval, long timeout) {
        // This method use getIndexState() to check index state.
        // If all index state become Finished, then we say the sync index action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        while (true) {
            long tsNow = System.currentTimeMillis();
            if ((tsNow - tsBegin) >= timeout * 1000) {
                String msg = "Waiting index thread is timeout, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setIndexName(indexName);
            if (StringUtils.isNotEmpty(databaseName)) {
                builder.setDbName(databaseName);
            }
            DescribeIndexRequest request = builder.build();

            DescribeIndexResponse response = blockingStub().describeIndex(request);

            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return R.failed(response.getStatus().getErrorCode(), response.getStatus().getReason());
            }

            if (response.getIndexDescriptionsList().size() == 0) {
                return R.failed(R.Status.UnexpectedError, response.getStatus().getReason());
            }
            IndexDescription index = response.getIndexDescriptionsList().stream()
                    .filter(x -> x.getFieldName().equals(fieldName))
                    .findFirst()
                    .orElse(response.getIndexDescriptions(0));

            if (index.getState() == IndexState.Finished) {
                return R.success(true);
            } else if (index.getState() == IndexState.Failed) {
                String msg = "Get index state failed: " + index.getState().toString();
                logError(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            try {
                String msg = "Waiting index, interval: " + waitingInterval + "ms";
                logDebug(msg);
                TimeUnit.MILLISECONDS.sleep(waitingInterval);
            } catch (InterruptedException e) {
                String msg = "Waiting index thread is interrupted, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.Success, msg);
            }
        }
    }

    private <T> R<T> failedStatus(String requestName, io.milvus.grpc.Status status) {
        String reason = status.getReason();
        if (StringUtils.isEmpty(reason)) {
            reason = "error code: " + status.getErrorCode().toString();
        }
        logError(requestName + " failed:{}", reason);
        return R.failed(R.Status.valueOf(status.getErrorCode().getNumber()), reason);
    }

    ///////////////////// API implementation //////////////////////
    @Override
    public R<Boolean> hasCollection(@NonNull HasCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            HasCollectionRequest.Builder builder = HasCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            HasCollectionRequest hasCollectionRequest = builder
                    .build();

            BoolResponse response = blockingStub().hasCollection(hasCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("HasCollectionRequest successfully!");
                Boolean value = Optional.of(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);
                return R.success(value);
            } else {
                return failedStatus("HasCollectionRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("HasCollectionRequest RPC failed:{}", requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("HasCollectionRequest failed:{}", requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createDatabase(CreateDatabaseParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // Construct CreateDatabaseRequest
            CreateDatabaseRequest createDatabaseRequest = CreateDatabaseRequest.newBuilder()
                    .setDbName(requestParam.getDatabaseName())
                    .build();

            Status response = blockingStub().createDatabase(createDatabaseRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreateDatabaseRequest successfully! Database name:{}",
                        requestParam.getDatabaseName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreateDatabaseRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("CreateDatabaseRequest RPC failed! Database name:{}",
                    requestParam.getDatabaseName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateDatabaseRequest failed! Database name:{}",
                    requestParam.getDatabaseName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListDatabasesResponse> listDatabases() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        try {
            // Construct ListDatabasesRequest
            ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder()
                    .build();

            ListDatabasesResponse response = blockingStub().listDatabases(listDatabasesRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ListDatabasesRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ListDatabasesRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("ListDatabasesRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ListDatabasesRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropDatabase(DropDatabaseParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // Construct DropDatabaseRequest
            DropDatabaseRequest dropDatabaseRequest = DropDatabaseRequest.newBuilder()
                    .setDbName(requestParam.getDatabaseName())
                    .build();

            Status response = blockingStub().dropDatabase(dropDatabaseRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropDatabaseRequest successfully! Database name:{}",
                        requestParam.getDatabaseName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropDatabaseRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropDatabaseRequest RPC failed! Database name:{}",
                    requestParam.getDatabaseName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropDatabaseRequest failed! Database name:{}",
                    requestParam.getDatabaseName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createCollection(@NonNull CreateCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // Construct CollectionSchema Params
            CollectionSchema.Builder collectionSchemaBuilder = CollectionSchema.newBuilder();
            collectionSchemaBuilder.setName(requestParam.getCollectionName())
                    .setDescription(requestParam.getDescription())
                    .setEnableDynamicField(requestParam.isEnableDynamicField());

            for (FieldType fieldType : requestParam.getFieldTypes()) {
                collectionSchemaBuilder.addFields(ParamUtils.ConvertField(fieldType));
            }

            // Construct CreateCollectionRequest
            CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setShardsNum(requestParam.getShardsNum())
                    .setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode())
                    .setSchema(collectionSchemaBuilder.build().toByteString());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            if (requestParam.getPartitionsNum() > 0) {
                builder.setNumPartitions(requestParam.getPartitionsNum());
            }

            CreateCollectionRequest createCollectionRequest = builder.build();

            Status response = blockingStub().createCollection(createCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreateCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreateCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("CreateCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropCollection(@NonNull DropCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropCollectionRequest.Builder builder = DropCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            DropCollectionRequest dropCollectionRequest = builder.build();

            Status response = blockingStub().dropCollection(dropCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadCollection(@NonNull LoadCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .addAllResourceGroups(requestParam.getResourceGroups())
                    .setRefresh(requestParam.isRefresh());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            LoadCollectionRequest loadCollectionRequest = builder
                    .build();

            Status response = blockingStub().loadCollection(loadCollectionRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            // sync load, wait until collection finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), null,
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadCollectionRequest successfully! Collection name:{}",
                    requestParam.getCollectionName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) { // gRPC could throw this exception
            logError("LoadCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (IllegalResponseException e) { // milvus exception for illegal response
            logError("LoadCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releaseCollection(@NonNull ReleaseCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            Status response = blockingStub().releaseCollection(releaseCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("ReleaseCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("ReleaseCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("ReleaseCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleaseCollectionRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> renameCollection(RenameCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            RenameCollectionRequest renameCollectionRequest = RenameCollectionRequest.newBuilder()
                    .setOldName(requestParam.getOldCollectionName())
                    .setNewName(requestParam.getNewCollectionName())
                    .build();

            Status response = blockingStub().renameCollection(renameCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("RenameCollectionRequest successfully! Collection name:{}",
                        requestParam.getOldCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("RenameCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("RenameCollectionRequest RPC failed! Collection name:{}",
                    requestParam.getOldCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("RenameCollectionRequest failed! Collection name:{}",
                    requestParam.getOldCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(@NonNull DescribeCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            DescribeCollectionRequest describeCollectionRequest = builder.build();

            DescribeCollectionResponse response = blockingStub().describeCollection(describeCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeCollectionRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeCollectionRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("DescribeCollectionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeCollectionRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(@NonNull GetCollectionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // flush collection if client command to do it(some times user may want to know the newest row count)
            if (requestParam.isFlushCollection()) {
                R<FlushResponse> response = flush(FlushParam.newBuilder()
                        .withDatabaseName(requestParam.getDatabaseName())
                        .addCollectionName(requestParam.getCollectionName())
                        .withSyncFlush(Boolean.TRUE)
                        .build());
                if (response.getStatus() != R.Status.Success.getCode()) {
                    return R.failed(R.Status.valueOf(response.getStatus()), response.getMessage());
                }
            }

            GetCollectionStatisticsRequest.Builder builder = GetCollectionStatisticsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            GetCollectionStatisticsRequest getCollectionStatisticsRequest = builder.build();

            GetCollectionStatisticsResponse response = blockingStub().getCollectionStatistics(getCollectionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetCollectionStatisticsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetCollectionStatisticsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetCollectionStatisticsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCollectionStatisticsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(@NonNull ShowCollectionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder()
                    .addAllCollectionNames(requestParam.getCollectionNames())
                    .setType(requestParam.getShowType());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            ShowCollectionsRequest showCollectionsRequest = builder.build();

            ShowCollectionsResponse response = blockingStub().showCollections(showCollectionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ShowCollectionsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ShowCollectionsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("ShowCollectionsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowCollectionsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterCollection(AlterCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            AlterCollectionRequest.Builder alterCollRequestBuilder = AlterCollectionRequest.newBuilder();
            List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(requestParam.getProperties());
            if (CollectionUtils.isNotEmpty(propertiesList)) {
                propertiesList.forEach(alterCollRequestBuilder::addProperties);
            }

            AlterCollectionRequest alterCollectionRequest = alterCollRequestBuilder
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            Status response = blockingStub().alterCollection(alterCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("AlterCollectionRequest successfully!");
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("AlterCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("AlterCollectionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("AlterCollectionRequest failed!", e);
            return R.failed(e);
        }
    }

    /**
     * Flush insert buffer into storage. To make sure the buffer persisted successfully, it calls
     * GetFlushState() to check related segments state.
     */
    @Override
    public R<FlushResponse> flush(@NonNull FlushParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
            FlushRequest.Builder builder = FlushRequest.newBuilder()
                    .setBase(msgBase)
                    .addAllCollectionNames(requestParam.getCollectionNames());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            FlushRequest flushRequest = builder.build();
            FlushResponse response = blockingStub().flush(flushRequest);

            if (Objects.equals(requestParam.getSyncFlush(), Boolean.TRUE)) {
                waitForFlush(response, requestParam.getSyncFlushWaitingInterval(),
                        requestParam.getSyncFlushWaitingTimeout());
            }

            logDebug("FlushRequest successfully! Collection names:{}", requestParam.getCollectionNames());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("FlushRequest RPC failed! Collection names:{}",
                    requestParam.getCollectionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("FlushRequest failed! Collection names:{}",
                    requestParam.getCollectionNames(), e);
            return R.failed(e);
        }
    }

    /**
     * Flush all collections. All insertions, deletions, and upserts before `flushAll` will be synced.
     */
    @Override
    public R<FlushAllResponse> flushAll(boolean syncFlushAll, long syncFlushAllWaitingInterval, long syncFlushAllTimeout) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo("start flushAll...");
        try {
            FlushAllRequest flushAllRequest = FlushAllRequest.newBuilder().build();
            FlushAllResponse response = blockingStub().flushAll(flushAllRequest);

            if (syncFlushAll) {
                waitForFlushAll(response, syncFlushAllWaitingInterval, syncFlushAllTimeout);
            }

            logDebug("flushAll successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("flushAll RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("flushAll failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createPartition(@NonNull CreatePartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            CreatePartitionRequest createPartitionRequest = CreatePartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            Status response = blockingStub().createPartition(createPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreatePartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreatePartitionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("CreatePartitionRequest RPC failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreatePartitionRequest failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropPartition(@NonNull DropPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropPartitionRequest dropPartitionRequest = DropPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            Status response = blockingStub().dropPartition(dropPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropPartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropPartitionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropPartitionRequest RPC failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropPartitionRequest failed! Collection name:{}, partition name:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasPartition(@NonNull HasPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            HasPartitionRequest hasPartitionRequest = HasPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            BoolResponse response = blockingStub().hasPartition(hasPartitionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("HasPartitionRequest successfully!");
                Boolean result = response.getValue();
                return R.success(result);
            } else {
                return failedStatus("HasPartitionRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("HasPartitionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("HasPartitionRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadPartitions(@NonNull LoadPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            LoadPartitionsRequest.Builder builder = LoadPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .addAllResourceGroups(requestParam.getResourceGroups())
                    .setRefresh(requestParam.isRefresh());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            LoadPartitionsRequest loadPartitionsRequest = builder.build();

            Status response = blockingStub().loadPartitions(loadPartitionsRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            // sync load, wait until all partitions finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getPartitionNames(),
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadPartitionsRequest successfully! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) { // gRPC could throw this exception
            logError("LoadPartitionsRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (IllegalResponseException e) { // milvus exception for illegal response
            logError("LoadPartitionsRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadPartitionsRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releasePartitions(@NonNull ReleasePartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ReleasePartitionsRequest releasePartitionsRequest = ReleasePartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            Status response = blockingStub().releasePartitions(releasePartitionsRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("ReleasePartitionsRequest successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("ReleasePartitionsRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("ReleasePartitionsRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleasePartitionsRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(@NonNull GetPartitionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // flush collection if client command to do it(some times user may want to know the newest row count)
            if (requestParam.isFlushCollection()) {
                R<FlushResponse> response = flush(FlushParam.newBuilder()
                        .addCollectionName(requestParam.getCollectionName())
                        .withSyncFlush(Boolean.TRUE)
                        .build());
                if (response.getStatus() != R.Status.Success.getCode()) {
                    return R.failed(R.Status.valueOf(response.getStatus()), response.getMessage());
                }
            }

            GetPartitionStatisticsRequest getPartitionStatisticsRequest = GetPartitionStatisticsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            GetPartitionStatisticsResponse response =
                    blockingStub().getPartitionStatistics(getPartitionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetPartitionStatisticsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("getPartitionStatistics", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetPartitionStatisticsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetPartitionStatisticsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(@NonNull ShowPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ShowPartitionsRequest showPartitionsRequest = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            ShowPartitionsResponse response = blockingStub().showPartitions(showPartitionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ShowPartitionsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ShowPartitionsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("ShowPartitionsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowPartitionsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createAlias(@NonNull CreateAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            CreateAliasRequest createAliasRequest = CreateAliasRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setAlias(requestParam.getAlias())
                    .build();

            Status response = blockingStub().createAlias(createAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreateAliasRequest successfully! Collection name:{}, alias name:{}",
                        requestParam.getCollectionName(), requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreateAliasRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("CreateAliasRequest RPC failed! Collection name:{}, alias name:{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateAliasRequest failed! Collection name:{}, alias name:{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropAlias(@NonNull DropAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropAliasRequest dropAliasRequest = DropAliasRequest.newBuilder()
                    .setAlias(requestParam.getAlias())
                    .build();

            Status response = blockingStub().dropAlias(dropAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropAliasRequest successfully! Alias name:{}", requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropAliasRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropAliasRequest RPC failed! Alias name:{}",
                    requestParam.getAlias(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropAliasRequest failed! Alias name:{}",
                    requestParam.getAlias(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterAlias(@NonNull AlterAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            AlterAliasRequest alterAliasRequest = AlterAliasRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setAlias(requestParam.getAlias())
                    .build();

            Status response = blockingStub().alterAlias(alterAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("AlterAliasRequest successfully! Collection name:{}, alias name:{}",
                        requestParam.getCollectionName(), requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("AlterAliasRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("AlterAliasRequest RPC failed! Collection name:{}, alias name:{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("AlterAliasRequest failed! Collection name:{}, alias name:{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createIndex(@NonNull CreateIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            // get collection schema to check input
            DescribeCollectionParam.Builder descBuilder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(descBuilder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            List<FieldType> fields = wrapper.getFields();
            // check field existence and index_type/field_type must be matched
            boolean fieldExists = false;
            boolean validType = false;
            for (FieldType field : fields) {
                if (requestParam.getFieldName().equals(field.getName())) {
                    fieldExists = true;
                    if (ParamUtils.VerifyIndexType(requestParam.getIndexType(), field.getDataType())) {
                        validType = true;
                    }
                    break;
                }
            }

            if (!fieldExists) {
                String msg = String.format("Field '%s' doesn't exist in the collection", requestParam.getFieldName());
                logError("CreateIndexRequest failed! {}\n", msg);
                return R.failed(R.Status.IllegalArgument, msg);
            }
            if (!validType) {
                String msg = String.format("Index type '%s' doesn't match with data type of field '%s'",
                        requestParam.getIndexType().name(), requestParam.getFieldName());
                logError("CreateIndexRequest failed! {}\n", msg);
                return R.failed(R.Status.IllegalArgument, msg);
            }

            // prepare index parameters
            CreateIndexRequest.Builder createIndexRequestBuilder = CreateIndexRequest.newBuilder();
            List<KeyValuePair> extraParamList = ParamUtils.AssembleKvPair(requestParam.getExtraParam());
            if (CollectionUtils.isNotEmpty(extraParamList)) {
                extraParamList.forEach(createIndexRequestBuilder::addExtraParams);
            }

            CreateIndexRequest.Builder builder = createIndexRequestBuilder
                    .setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName())
                    .setIndexName(requestParam.getIndexName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            CreateIndexRequest createIndexRequest = builder.build();

            Status response = blockingStub().createIndex(createIndexRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateIndexRequest", response);
            }

            if (requestParam.isSyncMode()) {
                R<Boolean> res = waitForIndex(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getIndexName(),
                        requestParam.getFieldName(),
                        requestParam.getSyncWaitingInterval(), requestParam.getSyncWaitingTimeout());
                if (res.getStatus() != R.Status.Success.getCode()) {
                    logError("CreateIndexRequest in sync mode" + " failed:{}", res.getMessage());
                    return R.failed(R.Status.valueOf(res.getStatus()), res.getMessage());
                }
            }
            logDebug("CreateIndexRequest successfully! Collection name:{}， Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateIndexRequest RPC failed! Collection name:{}， Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateIndexRequest failed! Collection name:{} ，Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropIndex(@NonNull DropIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            Status response = blockingStub().dropIndex(dropIndexRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropIndexRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropIndexRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropIndexRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropIndexRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(@NonNull DescribeIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            DescribeIndexRequest describeIndexRequest = builder.build();

            DescribeIndexResponse response = blockingStub().describeIndex(describeIndexRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeIndexRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeIndexRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("DescribeIndexRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeIndexRequest failed!", e);
            return R.failed(e);
        }
    }

    @Deprecated
    // use DescribeIndex instead
    @Override
    public R<GetIndexStateResponse> getIndexState(@NonNull GetIndexStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetIndexStateRequest getIndexStateRequest = GetIndexStateRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            GetIndexStateResponse response = blockingStub().getIndexState(getIndexStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetIndexStateRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetIndexStateRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetIndexStateRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexStateRequest failed!", e);
            return R.failed(e);
        }
    }

    @Deprecated
    // use DescribeIndex instead
    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(@NonNull GetIndexBuildProgressParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetIndexBuildProgressRequest getIndexBuildProgressRequest = GetIndexBuildProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            GetIndexBuildProgressResponse response = blockingStub().getIndexBuildProgress(getIndexBuildProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetIndexBuildProgressRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetIndexBuildProgressRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetIndexBuildProgressRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexBuildProgressRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> delete(@NonNull DeleteParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                    .setBase(MsgBase.newBuilder().setMsgType(MsgType.Delete).build())
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .setExpr(requestParam.getExpr())
                    .build();

            MutationResult response = blockingStub().delete(deleteRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DeleteRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                return failedStatus("DeleteRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("DeleteRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> insert(@NonNull InsertParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            InsertRequest insertRequest = ParamUtils.convertInsertParam(requestParam, wrapper);
            MutationResult response = blockingStub().insert(insertRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("InsertRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                return failedStatus("InsertRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("InsertRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("InsertRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public ListenableFuture<R<MutationResult>> insertAsync(InsertParam requestParam) {
        if (!clientIsReady()) {
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Client rpc channel is not ready")));
        }

        logInfo(requestParam.toString());

        DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                .withCollectionName(requestParam.getCollectionName());
        if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
            builder.withDatabaseName(requestParam.getDatabaseName());
        }
        R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

        if (descResp.getStatus() != R.Status.Success.getCode()) {
            logDebug("Failed to describe collection: {}", requestParam.getCollectionName());
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Failed to describe collection")));
        }

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
        InsertRequest insertRequest = ParamUtils.convertInsertParam(requestParam, wrapper);
        ListenableFuture<MutationResult> response = futureStub().insert(insertRequest);

        Futures.addCallback(
                response,
                new FutureCallback<MutationResult>() {
                    @Override
                    public void onSuccess(MutationResult result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("insertAsync successfully! Collection name:{}",
                                    requestParam.getCollectionName());
                        } else {
                            logError("insertAsync failed! Collection name:{}\n{}",
                                    requestParam.getCollectionName(), result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("insertAsync failed:\n{}", t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<MutationResult, R<MutationResult>> transformFunc =
                results -> {
                    if (results.getStatus().getErrorCode() == ErrorCode.Success) {
                        return R.success(results);
                    } else {
                        return R.failed(R.Status.valueOf(results.getStatus().getErrorCode().getNumber()),
                                results.getStatus().getReason());
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<SearchResults> search(@NonNull SearchParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
            SearchResults response = this.blockingStub().search(searchRequest);

            //TODO: truncate distance value by round decimal

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("SearchRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("SearchRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("SearchRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (ParamException e) {
            logError("SearchRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public ListenableFuture<R<SearchResults>> searchAsync(SearchParam requestParam) {
        if (!clientIsReady()) {
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Client rpc channel is not ready")));
        }

        logInfo(requestParam.toString());

        SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
        ListenableFuture<SearchResults> response = this.futureStub().search(searchRequest);

        Futures.addCallback(
                response,
                new FutureCallback<SearchResults>() {
                    @Override
                    public void onSuccess(SearchResults result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("searchAsync successfully! Collection name:{}",
                                    requestParam.getCollectionName());
                        } else {
                            logError("searchAsync failed! Collection name:{}\n{}",
                                    requestParam.getCollectionName(), result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("searchAsync failed:\n{}", t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<SearchResults, R<SearchResults>> transformFunc =
                results -> {
                    if (results.getStatus().getErrorCode() == ErrorCode.Success) {
                        return R.success(results);
                    } else {
                        return R.failed(R.Status.valueOf(results.getStatus().getErrorCode().getNumber()),
                                results.getStatus().getReason());
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<QueryResults> query(@NonNull QueryParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            QueryRequest queryRequest = ParamUtils.convertQueryParam(requestParam);
            QueryResults response = this.blockingStub().query(queryRequest);
            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("QueryRequest successfully!");
                return R.success(response);
            } else {
                // Server side behavior: if a query expression could not filter out any result,
                // or collection is empty, the server return ErrorCode.EmptyCollection.
                // Here we give a general message for this case.
                if (response.getStatus().getErrorCode() == ErrorCode.EmptyCollection) {
                    logWarning("QueryRequest returns nothing: empty collection or improper expression");
                    return R.failed(ErrorCode.EmptyCollection, "empty collection or improper expression");
                }
                return failedStatus("QueryRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
//            e.printStackTrace();
            logError("QueryRequest RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("QueryRequest failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public ListenableFuture<R<QueryResults>> queryAsync(QueryParam requestParam) {
        if (!clientIsReady()) {
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Client rpc channel is not ready")));
        }

        logInfo(requestParam.toString());

        QueryRequest queryRequest = ParamUtils.convertQueryParam(requestParam);
        ListenableFuture<QueryResults> response = this.futureStub().query(queryRequest);

        Futures.addCallback(
                response,
                new FutureCallback<QueryResults>() {
                    @Override
                    public void onSuccess(QueryResults result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("queryAsync successfully! Collection name:{}",
                                    requestParam.getCollectionName());
                        } else {
                            logError("queryAsync failed! Collection name:{}\n{}",
                                    requestParam.getCollectionName(), result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("queryAsync failed:\n{}", t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<QueryResults, R<QueryResults>> transformFunc =
                results -> {
                    if (results.getStatus().getErrorCode() == ErrorCode.Success) {
                        return R.success(results);
                    } else {
                        return R.failed(R.Status.valueOf(results.getStatus().getErrorCode().getNumber()),
                                results.getStatus().getReason());
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<GetMetricsResponse> getMetrics(@NonNull GetMetricsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetMetricsRequest getMetricsRequest = GetMetricsRequest.newBuilder()
                    .setRequest(requestParam.getRequest())
                    .build();

            GetMetricsResponse response = blockingStub().getMetrics(getMetricsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetMetricsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetMetricsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetMetricsRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetMetricsRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetFlushStateResponse> getFlushState(@NonNull GetFlushStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetFlushStateRequest getFlushStateRequest = GetFlushStateRequest.newBuilder()
                    .addAllSegmentIDs(requestParam.getSegmentIDs())
                    .build();

            GetFlushStateResponse response = blockingStub().getFlushState(getFlushStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetFlushState successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetFlushState", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetFlushState RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetFlushState failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetFlushAllStateResponse> getFlushAllState(GetFlushAllStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
            GetFlushAllStateRequest getFlushStateRequest = GetFlushAllStateRequest.newBuilder()
                    .setBase(msgBase)
                    .setFlushAllTs(requestParam.getFlushAllTs())
                    .build();

            GetFlushAllStateResponse response = blockingStub().getFlushAllState(getFlushStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("getFlushAllState successfully!");
                return R.success(response);
            } else {
                return failedStatus("getFlushAllState", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("getFlushAllState RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("getFlushAllState failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(@NonNull GetPersistentSegmentInfoParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetPersistentSegmentInfoRequest getSegmentInfoRequest = GetPersistentSegmentInfoRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            GetPersistentSegmentInfoResponse response = blockingStub().getPersistentSegmentInfo(getSegmentInfoRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetPersistentSegmentInfoRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetPersistentSegmentInfoRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetPersistentSegmentInfoRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetPersistentSegmentInfoRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(@NonNull GetQuerySegmentInfoParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetQuerySegmentInfoRequest getSegmentInfoRequest = GetQuerySegmentInfoRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            GetQuerySegmentInfoResponse response = blockingStub().getQuerySegmentInfo(getSegmentInfoRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetQuerySegmentInfoRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetQuerySegmentInfoRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetQuerySegmentInfoRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetQuerySegmentInfoRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetReplicasResponse> getReplicas(GetReplicasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            R<DescribeCollectionResponse> descResp = describeCollection(DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            GetReplicasRequest getReplicasRequest = GetReplicasRequest.newBuilder()
                    .setCollectionID(descResp.getData().getCollectionID())
                    .setWithShardNodes(requestParam.isWithShardNodes())
                    .build();

            GetReplicasResponse response = blockingStub().getReplicas(getReplicasRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetReplicasRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetReplicasRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetReplicasRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetReplicasRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadBalance(LoadBalanceParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            LoadBalanceRequest loadBalanceRequest = LoadBalanceRequest.newBuilder()
                    .setSrcNodeID(requestParam.getSrcNodeID())
                    .addAllDstNodeIDs(requestParam.getDestNodeIDs())
                    .addAllSealedSegmentIDs(requestParam.getSegmentIDs())
                    .build();

            Status response = blockingStub().loadBalance(loadBalanceRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("LoadBalanceRequest successfully!");
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("LoadBalanceRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("LoadBalanceRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadBalanceRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetCompactionStateRequest getCompactionStateRequest = GetCompactionStateRequest.newBuilder()
                    .setCompactionID(requestParam.getCompactionID())
                    .build();

            GetCompactionStateResponse response = blockingStub().getCompactionState(getCompactionStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetCompactionStateRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetCompactionStateRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetCompactionStateRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCompactionStateRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<ManualCompactionResponse> manualCompact(ManualCompactParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            R<DescribeCollectionResponse> descResp = describeCollection(DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            ManualCompactionRequest manualCompactionRequest = ManualCompactionRequest.newBuilder()
                    .setCollectionID(descResp.getData().getCollectionID())
                    .build();

            ManualCompactionResponse response = blockingStub().manualCompaction(manualCompactionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ManualCompactionRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ManualCompactionRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("ManualCompactionRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ManualCompactionRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetCompactionPlansRequest getCompactionPlansRequest = GetCompactionPlansRequest.newBuilder()
                    .setCompactionID(requestParam.getCompactionID())
                    .build();

            GetCompactionPlansResponse response = blockingStub().getCompactionStateWithPlans(getCompactionPlansRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetCompactionPlansRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetCompactionPlansRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetCompactionPlansRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCompactionPlansRequest failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createCredential(CreateCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            CreateCredentialRequest createCredentialRequest = CreateCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .setPassword(getBase64EncodeString(requestParam.getPassword()))
                    .build();

            Status response = blockingStub().createCredential(createCredentialRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateCredentialRequest", response);
            }

            logDebug("CreateCredentialRequest successfully! User name:{}",
                    requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateCredentialRequest RPC failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCredentialRequest failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> updateCredential(UpdateCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .setOldPassword(getBase64EncodeString(requestParam.getOldPassword()))
                    .setNewPassword(getBase64EncodeString(requestParam.getNewPassword()))
                    .build();

            Status response = blockingStub().updateCredential(updateCredentialRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("UpdateCredentialRequest", response);
            }

            logDebug("UpdateCredentialRequest successfully! User name:{}", requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("UpdateCredentialRequest RPC failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("UpdateCredentialRequest failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> deleteCredential(DeleteCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DeleteCredentialRequest deleteCredentialRequest = DeleteCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .build();

            Status response = blockingStub().deleteCredential(deleteCredentialRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("DeleteCredentialRequest", response);
            }

            logDebug("DeleteCredentialRequest successfully! User name:{}", requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("DeleteCredentialRequest RPC failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteCredentialRequest failed! User name:{}",
                    requestParam.getUsername(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListCredUsersResponse> listCredUsers(ListCredUsersParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ListCredUsersRequest listCredUsersRequest = ListCredUsersRequest.newBuilder()
                    .build();

            ListCredUsersResponse response = blockingStub().listCredUsers(listCredUsersRequest);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("ListCredUsersRequest", response.getStatus());
            }

            logDebug("ListCredUsersRequest successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("ListCredUsersRequest RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ListCredUsersRequest failed!", e);
            return R.failed(e);
        }
    }

    private String getBase64EncodeString(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public R<RpcStatus> addUserToRole(AddUserToRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            OperateUserRoleRequest request = OperateUserRoleRequest.newBuilder()
                    .setUsername(requestParam.getUserName())
                    .setRoleName(requestParam.getRoleName())
                    .setType(OperateUserRoleType.AddUserToRole)
                    .build();

            Status response = blockingStub().operateUserRole(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("AddUserToRole", response);
            }

            logDebug("AddUserToRole successfully! Username:{}, Role name:{}", requestParam.getUserName(), request.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("AddUserToRole RPC failed! Username:{} Role name:{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("AddUserToRole failed! Username:{} Role name:{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> removeUserFromRole(RemoveUserFromRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            OperateUserRoleRequest request = OperateUserRoleRequest.newBuilder()
                    .setUsername(requestParam.getUserName())
                    .setRoleName(requestParam.getRoleName())
                    .setType(OperateUserRoleType.RemoveUserFromRole)
                    .build();

            Status response = blockingStub().operateUserRole(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("RemoveUserFromRole", response);
            }

            logDebug("RemoveUserFromRole successfully! User name:{}, Role name:{}", requestParam.getUserName(), request.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("RemoveUserFromRole RPC failed! User name:{} Role name:{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("RemoveUserFromRole failed! User name:{} Role name:{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> createRole(CreateRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            CreateRoleRequest request = CreateRoleRequest.newBuilder()
                    .setEntity(RoleEntity.newBuilder()
                            .setName(requestParam.getRoleName())
                            .build())
                    .build();

            Status response = blockingStub().createRole(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateRoleRequest", response);
            }
            logDebug("CreateRoleRequest successfully! Role name:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateRoleRequest RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateRoleRequest failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> dropRole(DropRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropRoleRequest request = DropRoleRequest.newBuilder()
                    .setRoleName(requestParam.getRoleName())
                    .build();

            Status response = blockingStub().dropRole(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("DropRoleRequest", response);
            }
            logDebug("DropRoleRequest successfully! Role name:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("DropRoleRequest RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropRoleRequest failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }


    public R<SelectRoleResponse> selectRole(SelectRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            SelectRoleRequest request = SelectRoleRequest.newBuilder()
                    .setRole(RoleEntity.newBuilder()
                            .setName(requestParam.getRoleName())
                            .build())
                    .setIncludeUserInfo(requestParam.isIncludeUserInfo())
                    .build();

            SelectRoleResponse response = blockingStub().selectRole(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectRoleRequest", response.getStatus());
            }
            logDebug("SelectRoleRequest successfully! Role name:{}", requestParam.getRoleName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectRoleRequest RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectRoleRequest failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }


    public R<SelectUserResponse> selectUser(SelectUserParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());
        try {
            SelectUserRequest request = SelectUserRequest.newBuilder()
                    .setUser(UserEntity.newBuilder().setName(requestParam.getUserName()).build())
                    .setIncludeRoleInfo(requestParam.isIncludeRoleInfo())
                    .build();

            SelectUserResponse response = blockingStub().selectUser(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectUserRequest", response.getStatus());
            }
            logDebug("SelectUserRequest successfully! User name:{}", requestParam.getUserName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectUserRequest RPC failed! User name:{}",
                    requestParam.getUserName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectUserRequest failed! User name:{}",
                    requestParam.getUserName(), e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        GrantEntity.Builder builder = GrantEntity.newBuilder()
                .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build())
                .setObjectName(requestParam.getObjectName())
                .setObject(ObjectEntity.newBuilder().setName(requestParam.getObject()).build())
                .setGrantor(GrantorEntity.newBuilder()
                        .setPrivilege(PrivilegeEntity.newBuilder().setName(requestParam.getPrivilege()).build()).build());
        if (StringUtils.isNotBlank(requestParam.getDatabaseName())) {
            builder.setDbName(requestParam.getDatabaseName());
        }

        try {
            OperatePrivilegeRequest request = OperatePrivilegeRequest.newBuilder()
                    .setType(OperatePrivilegeType.Grant)
                    .setEntity(builder.build())
                    .build();

            Status response = blockingStub().operatePrivilege(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("GrantRolePrivilege", response);
            }
            logDebug("GrantRolePrivilege successfully! Role name:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("GrantRolePrivilege RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GrantRolePrivilege failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }

    public R<RpcStatus> revokeRolePrivilege(RevokeRolePrivilegeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());
        try {
            OperatePrivilegeRequest request = OperatePrivilegeRequest.newBuilder()
                    .setType(OperatePrivilegeType.Revoke)
                    .setEntity(GrantEntity.newBuilder()
                            .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build())
                            .setObjectName(requestParam.getObjectName())
                            .setObject(ObjectEntity.newBuilder().setName(requestParam.getObject()).build())
                            .setGrantor(GrantorEntity.newBuilder()
                                    .setPrivilege(PrivilegeEntity.newBuilder().setName(requestParam.getPrivilege()).build()).build())
                            .build())
                    .build();

            Status response = blockingStub().operatePrivilege(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("RevokeRolePrivilege", response);
            }
            logDebug("RevokeRolePrivilege successfully! Role name:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("RevokeRolePrivilege RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("RevokeRolePrivilege failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }


    public R<SelectGrantResponse> selectGrantForRole(SelectGrantForRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            SelectGrantRequest request = SelectGrantRequest.newBuilder()
                    .setEntity(GrantEntity.newBuilder()
                            .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build())
                            .build())
                    .build();

            SelectGrantResponse response = blockingStub().selectGrant(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectGrantForRole", response.getStatus());
            }
            logDebug("SelectGrantForRole successfully! Role name:{},", requestParam.getRoleName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectGrantForRole RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectGrantForRole failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }

    public R<SelectGrantResponse> selectGrantForRoleAndObject(SelectGrantForRoleAndObjectParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());
        try {
            SelectGrantRequest request = SelectGrantRequest.newBuilder()
                    .setEntity(GrantEntity.newBuilder()
                            .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build())
                            .setObjectName(requestParam.getObjectName())
                            .setObject(ObjectEntity.newBuilder().setName(requestParam.getObject()).build())
                            .build())
                    .build();

            SelectGrantResponse response = blockingStub().selectGrant(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectGrantForRoleAndObject", response.getStatus());
            }
            logDebug("SelectGrantForRoleAndObject successfully! Role name:{},", requestParam.getRoleName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectGrantForRoleAndObject RPC failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectGrantForRoleAndObject failed! Role name:{}",
                    requestParam.getRoleName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<ImportResponse> bulkInsert(BulkInsertParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ImportRequest.Builder importRequest = ImportRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllFiles(requestParam.getFiles());

            if (StringUtils.isNotEmpty(requestParam.getPartitionName())) {
                importRequest.setPartitionName(requestParam.getPartitionName());
            }

            List<KeyValuePair> options = ParamUtils.AssembleKvPair(requestParam.getOptions());
            if (CollectionUtils.isNotEmpty(options)) {
                options.forEach(importRequest::addOptions);
            }

            ImportResponse response = blockingStub().import_(importRequest.build());
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("BulkInsert", response.getStatus());
            }

            logDebug("BulkInsert successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("BulkInsert RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("BulkInsert failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetImportStateResponse> getBulkInsertState(GetBulkInsertStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetImportStateRequest getImportStateRequest = GetImportStateRequest.newBuilder()
                    .setTask(requestParam.getTask())
                    .build();

            GetImportStateResponse response = blockingStub().getImportState(getImportStateRequest);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("GetBulkInsertState", response.getStatus());
            }

            logDebug("GetBulkInsertState successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("GetBulkInsertState RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetBulkInsertState failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListImportTasksResponse> listBulkInsertTasks(ListBulkInsertTasksParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ListImportTasksRequest listImportTasksRequest = ListImportTasksRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setLimit(requestParam.getLimit())
                    .build();

            ListImportTasksResponse response = blockingStub().listImportTasks(listImportTasksRequest);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("ListBulkInsertTasks", response.getStatus());
            }

            logDebug("ListBulkInsertTasks successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("ListBulkInsertTasks RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ListBulkInsertTasks failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetLoadingProgressResponse> getLoadingProgress(GetLoadingProgressParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetLoadingProgressRequest getLoadingProgressRequest = GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            GetLoadingProgressResponse response = blockingStub().getLoadingProgress(getLoadingProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetLoadingProgressRequest successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(response);
            } else {
                return failedStatus("GetLoadingProgressRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetLoadingProgressRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetLoadingProgressRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetLoadStateResponse> getLoadState(GetLoadStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            GetLoadStateRequest.Builder builder = GetLoadStateRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetLoadStateRequest loadStateRequest = builder
                    .build();

            GetLoadStateResponse response = blockingStub().getLoadState(loadStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetLoadStateRequest successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(response);
            } else {
                return failedStatus("GetLoadStateRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetLoadStateRequest RPC failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetLoadStateRequest failed! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<CheckHealthResponse> checkHealth() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        try {
            CheckHealthResponse response = blockingStub().checkHealth(CheckHealthRequest.newBuilder().build());
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("CheckHealth", response.getStatus());
            }
            logDebug("CheckHealth successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("CheckHealth RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CheckHealth failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetVersionResponse> getVersion() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        try {
            GetVersionResponse response = blockingStub().getVersion(GetVersionRequest.newBuilder().build());
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("GetVersion", response.getStatus());
            }
            logDebug("GetVersion successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("GetVersion RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("GetVersion failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createResourceGroup(CreateResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            CreateResourceGroupRequest request = CreateResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .build();

            Status response = blockingStub().createResourceGroup(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateResourceGroup", response);
            }
            logDebug("CreateResourceGroup successfully! Resource group name:{}",
                    requestParam.getGroupName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateResourceGroup RPC failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateResourceGroup failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropResourceGroup(DropResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DropResourceGroupRequest request = DropResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .build();

            Status response = blockingStub().dropResourceGroup(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("DropResourceGroup", response);
            }
            logDebug("DropResourceGroup successfully! Resource group name:{}",
                    requestParam.getGroupName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("DropResourceGroup RPC failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DropResourceGroup failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListResourceGroupsResponse> listResourceGroups(ListResourceGroupsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ListResourceGroupsRequest request = ListResourceGroupsRequest.newBuilder()
                    .build();

            ListResourceGroupsResponse response = blockingStub().listResourceGroups(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("ListResourceGroups", response.getStatus());
            }
            logDebug("ListResourceGroups successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("ListResourceGroups RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ListResourceGroups failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeResourceGroupResponse> describeResourceGroup(DescribeResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            DescribeResourceGroupRequest request = DescribeResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .build();

            DescribeResourceGroupResponse response = blockingStub().describeResourceGroup(request);
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("DescribeResourceGroup", response.getStatus());
            }
            logDebug("DescribeResourceGroup successfully! Resource group name:{}",
                    requestParam.getGroupName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("DescribeResourceGroup RPC failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeResourceGroup failed! Resource group name:{}",
                    requestParam.getGroupName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> transferNode(TransferNodeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            TransferNodeRequest request = TransferNodeRequest.newBuilder()
                    .setSourceResourceGroup(requestParam.getSourceGroupName())
                    .setTargetResourceGroup(requestParam.getTargetGroupName())
                    .setNumNode(requestParam.getNodeNumber())
                    .build();

            Status response = blockingStub().transferNode(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("TransferNode", response);
            }
            logDebug("TransferNode successfully! Source group:{}, target group:{}, nodes number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(), requestParam.getNodeNumber());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("TransferNode RPC failed! Source group:{}, target group:{}, nodes number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(), requestParam.getNodeNumber(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("TransferNode failed! Source group:{}, target group:{}, nodes number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(), requestParam.getNodeNumber(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> transferReplica(TransferReplicaParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            TransferReplicaRequest request = TransferReplicaRequest.newBuilder()
                    .setSourceResourceGroup(requestParam.getSourceGroupName())
                    .setTargetResourceGroup(requestParam.getTargetGroupName())
                    .setCollectionName(requestParam.getCollectionName())
                    .setDbName(requestParam.getDatabaseName())
                    .setNumReplica(requestParam.getReplicaNumber())
                    .build();

            Status response = blockingStub().transferReplica(request);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("TransferReplica", response);
            }
            logDebug("TransferReplica successfully! Source group:{}, target group:{}, replica number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(),
                    requestParam.getReplicaNumber());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("TransferReplica RPC failed! Source group:{}, target group:{}, replica number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(),
                    requestParam.getReplicaNumber(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("TransferReplica failed! Source group:{}, target group:{}, replica number:{}",
                    requestParam.getSourceGroupName(), requestParam.getTargetGroupName(),
                    requestParam.getReplicaNumber(), e);
            return R.failed(e);
        }
    }


    ///////////////////// High Level API//////////////////////
    @Override
    public R<RpcStatus> createCollection(CreateSimpleCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());
        
        try {
            // step1: create collection
            R<RpcStatus> createCollectionStatus = createCollection(requestParam.getCreateCollectionParam());
            if(!Objects.equals(createCollectionStatus.getStatus(), R.success().getStatus())){
                logError("CreateCollection failed: {}", createCollectionStatus.getException().getMessage());
                return R.failed(createCollectionStatus.getException());
            }

            // step2: create index
            R<RpcStatus> createIndexStatus = createIndex(requestParam.getCreateIndexParam());
            if(!Objects.equals(createIndexStatus.getStatus(), R.success().getStatus())){
                logError("CreateIndex failed: {}", createIndexStatus.getException().getMessage());
                return R.failed(createIndexStatus.getException());
            }

            // step3: load collection
            R<RpcStatus> loadCollectionStatus = loadCollection(requestParam.getLoadCollectionParam());
            if(!Objects.equals(loadCollectionStatus.getStatus(), R.success().getStatus())){
                logError("LoadCollection failed: {}", loadCollectionStatus.getException().getMessage());
                return R.failed(loadCollectionStatus.getException());
            }

            logDebug("CreateCollection successfully!");
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateCollection RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCollection failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListCollectionsResponse> listCollections(ListCollectionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());
        
        try {
            R<ShowCollectionsResponse> response = showCollections(requestParam.getShowCollectionsParam());
            if(!Objects.equals(response.getStatus(), R.success().getStatus())){
                logError("ListCollections failed: {}", response.getException().getMessage());
                return R.failed(response.getException());
            }

            ShowCollResponseWrapper showCollResponseWrapper = new ShowCollResponseWrapper(response.getData());
            return R.success(ListCollectionsResponse.builder()
                    .collectionNames(showCollResponseWrapper.getCollectionNames()).build());
        } catch (StatusRuntimeException e) {
            logError("ListCollections RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("ListCollections failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<InsertResponse> insert(InsertRowsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());
        
        try {
            R<MutationResult> response = insert(requestParam.getInsertParam());
            if(!Objects.equals(response.getStatus(), R.success().getStatus())){
                logError("Insert failed: {}", response.getException().getMessage());
                return R.failed(response.getException());
            }

            logDebug("Insert successfully!");
            MutationResultWrapper wrapper = new MutationResultWrapper(response.getData());
            return R.success(InsertResponse.builder().insertIds(wrapper.getInsertIDs()).insertCount(wrapper.getInsertCount()).build());
        } catch (StatusRuntimeException e) {
            logError("Insert RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("Insert failed!", e);
            return R.failed(e);
        }
    }

    @Override
    public R<DeleteResponse> delete(DeleteIdsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }
            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());

            String expr = VectorUtils.convertPksExpr(requestParam.getPrimaryIds(), wrapper);
            DeleteParam deleteParam = DeleteParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .withExpr(expr)
                    .build();
            R<MutationResult> resultR = delete(deleteParam);
            MutationResultWrapper resultWrapper = new MutationResultWrapper(resultR.getData());
            return R.success(DeleteResponse.builder().deleteIds(resultWrapper.getDeleteIDs()).build());
        } catch (StatusRuntimeException e) {
            logError("Delete RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("Delete failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetResponse> get(GetIdsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            if (CollectionUtils.isEmpty(requestParam.getOutputFields())) {
                FieldType vectorField = wrapper.getVectorField();
                requestParam.getOutputFields().addAll(Lists.newArrayList(Constant.ALL_OUTPUT_FIELDS, vectorField.getName()));
            }

            String expr = VectorUtils.convertPksExpr(requestParam.getPrimaryIds(), wrapper);
            QueryParam queryParam = QueryParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .withExpr(expr)
                    .withOutFields(requestParam.getOutputFields())
                    .withConsistencyLevel(requestParam.getConsistencyLevel())
                    .build();
            R<QueryResults> queryResp = query(queryParam);
            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryResp.getData());
            return R.success(GetResponse.builder().rowRecords(queryResultsWrapper.getRowRecords()).build());
        } catch (StatusRuntimeException e) {
            logError("Get RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("Get failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    @Override
    public R<QueryResponse> query(QuerySimpleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper descCollWrapper = new DescCollResponseWrapper(descResp.getData());
            if (CollectionUtils.isEmpty(requestParam.getOutputFields())) {
                FieldType vectorField = descCollWrapper.getVectorField();
                requestParam.getOutputFields().addAll(Lists.newArrayList(Constant.ALL_OUTPUT_FIELDS, vectorField.getName()));
            }

            QueryParam queryParam = QueryParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .withExpr(requestParam.getFilter())
                    .withOutFields(requestParam.getOutputFields())
                    .withOffset(requestParam.getOffset())
                    .withLimit(requestParam.getLimit())
                    .withConsistencyLevel(requestParam.getConsistencyLevel())
                    .build();
            R<QueryResults> response = query(queryParam);
            if(!Objects.equals(response.getStatus(), R.success().getStatus())){
                logError("Query failed: {}", response.getException().getMessage());
                return R.failed(response.getException());
            }

            QueryResultsWrapper queryWrapper = new QueryResultsWrapper(response.getData());
            return R.success(QueryResponse.builder().rowRecords(queryWrapper.getRowRecords()).build());
        } catch (StatusRuntimeException e) {
            logError("Query RPC failed!", e);
            return R.failed(e);
        } catch (Exception e) {
            logError("Query failed!", e);
            return R.failed(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public R<SearchResponse> search(SearchSimpleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logInfo(requestParam.toString());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());

            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            FieldType vectorField = wrapper.getVectorField();

            // fill in vectorData
            List<List<?>> vectors = new ArrayList<>();
            if (requestParam.getVectors().get(0) instanceof List) {
                vectors = (List<List<?>>) requestParam.getVectors();
            } else {
                vectors.add(requestParam.getVectors());
            }

            SearchParam searchParam = SearchParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .withVectors(vectors)
                    .withVectorFieldName(vectorField.getName())
                    .withOutFields(requestParam.getOutputFields())
                    .withExpr(requestParam.getFilter())
                    .withTopK(requestParam.getLimit())
                    .withParams(JacksonUtils.toJsonString(requestParam.getParams()))
                    .withConsistencyLevel(requestParam.getConsistencyLevel())
                    .build();

            // search
            R<SearchResults> response = search(searchParam);
            if(!Objects.equals(response.getStatus(), R.success().getStatus())){
                logError("Search failed: {}", response.getException().getMessage());
                return R.failed(response.getException());
            }

            SearchResultsWrapper searchResultsWrapper = new SearchResultsWrapper(response.getData().getResults());
            List<List<QueryResultsWrapper.RowRecord>> records = new ArrayList<>();
            for (int i = 0; i < vectors.size(); ++i) {
                records.add(searchResultsWrapper.getRowRecords(i));
            }
            return R.success(SearchResponse.builder().rowRecords(records).build());
        } catch (StatusRuntimeException e) {
            logError("Search RPC failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("Search failed! Collection name:{}",
                    requestParam.getCollectionName(), e);
            return R.failed(e);
        }
    }

    ///////////////////// Log Functions//////////////////////
    protected void logDebug(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Debug.ordinal()) {
            logger.debug(msg, params);
        }
    }

    protected void logInfo(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Info.ordinal()) {
            logger.info(msg, params);
        }
    }

    protected void logWarning(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Warning.ordinal()) {
            logger.warn(msg, params);
        }
    }

    protected void logError(String msg, Object... params) {
        if (logLevel.ordinal() <= LogLevel.Error.ordinal()) {
            logger.error(msg, params);
        }
    }
}

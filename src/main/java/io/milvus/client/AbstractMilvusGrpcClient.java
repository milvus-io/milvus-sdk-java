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
import com.google.common.util.concurrent.*;
import io.grpc.StatusRuntimeException;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.common.utils.VectorUtils;
import io.milvus.exception.*;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.*;
import io.milvus.param.alias.*;
import io.milvus.param.bulkinsert.*;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.credential.*;
import io.milvus.param.dml.*;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
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

    private void handleResponse(String requestInfo, io.milvus.grpc.Status status) {
        // the server made a change for error code:
        // for 2.2.x, error code is status.getErrorCode()
        // for 2.3.x, error code is status.getCode(), and the status.getErrorCode()
        // is also assigned according to status.getCode()
        //
        // For error cases:
        // if we use 2.3.4 sdk to interact with 2.3.x server, getCode() is non-zero, getErrorCode() is non-zero
        // if we use 2.3.4 sdk to interact with 2.2.x server, getCode() is zero, getErrorCode() is non-zero
        // if we use <=2.3.3 sdk to interact with 2.2.x/2.3.x server, getCode() is not available, getErrorCode() is non-zero

        if (status.getCode() != 0 || !status.getErrorCode().equals(ErrorCode.Success)) {
            logError("{} failed, error code: {}, reason: {}", requestInfo,
                    status.getCode() > 0 ? status.getCode() : status.getErrorCode().getNumber(),
                    status.getReason());

            // 2.3.4 sdk to interact with 2.2.x server, the getCode() is zero, here we reset its value to getErrorCode()
            int code = status.getCode();
            if (code == 0) {
                code = status.getErrorCode().getNumber();
            }
            throw new ServerException(status.getReason(), code, status.getErrorCode());
        }

        logDebug("{} successfully!", requestInfo);
    }

    ///////////////////// API implementation //////////////////////
    @Override
    public R<Boolean> hasCollection(@NonNull HasCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("HasCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            HasCollectionRequest.Builder builder = HasCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            HasCollectionRequest hasCollectionRequest = builder
                    .build();

            BoolResponse response = blockingStub().hasCollection(hasCollectionRequest);
            handleResponse(title, response.getStatus());
            Boolean value = Optional.of(response)
                    .map(BoolResponse::getValue)
                    .orElse(false);
            return R.success(value);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createDatabase(CreateDatabaseParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateDatabaseRequest databaseName:%s", requestParam.getDatabaseName());

        try {
            // Construct CreateDatabaseRequest
            CreateDatabaseRequest createDatabaseRequest = CreateDatabaseRequest.newBuilder()
                    .setDbName(requestParam.getDatabaseName())
                    .build();

            Status response = blockingStub().createDatabase(createDatabaseRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListDatabasesResponse> listDatabases() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug("ListDatabasesRequest");
        String title = "ListDatabasesRequest";

        try {
            // Construct ListDatabasesRequest
            ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder()
                    .build();

            ListDatabasesResponse response = blockingStub().listDatabases(listDatabasesRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropDatabase(DropDatabaseParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropDatabaseRequest databaseName:%s", requestParam.getDatabaseName());

        try {
            // Construct DropDatabaseRequest
            DropDatabaseRequest dropDatabaseRequest = DropDatabaseRequest.newBuilder()
                    .setDbName(requestParam.getDatabaseName())
                    .build();

            Status response = blockingStub().dropDatabase(dropDatabaseRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createCollection(@NonNull CreateCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateCollectionRequest collectionName:%s", requestParam.getCollectionName());

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
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropCollection(@NonNull DropCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DropCollectionRequest.Builder builder = DropCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            DropCollectionRequest dropCollectionRequest = builder.build();

            Status response = blockingStub().dropCollection(dropCollectionRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadCollection(@NonNull LoadCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("LoadCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .addAllResourceGroups(requestParam.getResourceGroups())
                    .setRefresh(requestParam.isRefresh());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().loadCollection(builder.build());
            handleResponse(title, response);

            // sync load, wait until collection finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), null,
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) { // gRPC could throw this exception
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) { // milvus exception for illegal response
            logError("{} failed! Exceptione:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releaseCollection(@NonNull ReleaseCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ReleaseCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            ReleaseCollectionRequest.Builder builder = ReleaseCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().releaseCollection(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> renameCollection(RenameCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("RenameCollectionRequest collectionName:%s", requestParam.getOldCollectionName());

        try {
            RenameCollectionRequest.Builder builder = RenameCollectionRequest.newBuilder()
                    .setOldName(requestParam.getOldCollectionName())
                    .setNewName(requestParam.getNewCollectionName());

            if (StringUtils.isNotEmpty(requestParam.getOldDatabaseName())) {
                builder.setDbName(requestParam.getOldDatabaseName());
            }
            if (StringUtils.isNotEmpty(requestParam.getNewDatabaseName())) {
                builder.setNewDBName(requestParam.getNewDatabaseName());
            }

            Status response = blockingStub().renameCollection(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(@NonNull DescribeCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DescribeCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            DescribeCollectionRequest describeCollectionRequest = builder.build();

            DescribeCollectionResponse response = blockingStub().describeCollection(describeCollectionRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(@NonNull GetCollectionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetCollectionStatisticsRequest collectionName:%s",
                requestParam.getCollectionName());

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
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(@NonNull ShowCollectionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = "ShowCollectionsRequest";

        try {
            ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder()
                    .addAllCollectionNames(requestParam.getCollectionNames())
                    .setType(requestParam.getShowType());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            ShowCollectionsRequest showCollectionsRequest = builder.build();

            ShowCollectionsResponse response = blockingStub().showCollections(showCollectionsRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterCollection(AlterCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("AlterCollectionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder();
            List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(requestParam.getProperties());
            if (CollectionUtils.isNotEmpty(propertiesList)) {
                propertiesList.forEach(builder::addProperties);
            }
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            AlterCollectionRequest alterCollectionRequest = builder
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            Status response = blockingStub().alterCollection(alterCollectionRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = "FlushRequest";

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

            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug("FlushAllRequest");
        String title = "FlushAllRequest";

        try {
            FlushAllRequest flushAllRequest = FlushAllRequest.newBuilder().build();
            FlushAllResponse response = blockingStub().flushAll(flushAllRequest);

            if (syncFlushAll) {
                waitForFlushAll(response, syncFlushAllWaitingInterval, syncFlushAllTimeout);
            }

            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createPartition(@NonNull CreatePartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreatePartitionRequest collectionName:%s, partitionName:%s",
                requestParam.getCollectionName(), requestParam.getPartitionName());

        try {
            CreatePartitionRequest.Builder builder = CreatePartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().createPartition(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropPartition(@NonNull DropPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropPartitionRequest collectionName:%s, partitionName:%s",
                requestParam.getCollectionName(), requestParam.getPartitionName());

        try {
            DropPartitionRequest.Builder builder = DropPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().dropPartition(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasPartition(@NonNull HasPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("HasPartitionRequest collectionName:%s, partitionName:%s",
                requestParam.getCollectionName(), requestParam.getPartitionName());

        try {
            HasPartitionRequest.Builder builder = HasPartitionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            BoolResponse response = blockingStub().hasPartition(builder.build());
            handleResponse(title, response.getStatus());
            Boolean result = response.getValue();
            return R.success(result);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadPartitions(@NonNull LoadPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("LoadPartitionsRequest collectionName:%s", requestParam.getCollectionName());

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

            Status response = blockingStub().loadPartitions(builder.build());
            handleResponse(title, response);

            // sync load, wait until all partitions finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getPartitionNames(),
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releasePartitions(@NonNull ReleasePartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ReleasePartitionsRequest collectionName:%s", requestParam.getCollectionName());

        try {
            ReleasePartitionsRequest.Builder builder = ReleasePartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().releasePartitions(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(@NonNull GetPartitionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetPartitionStatisticsRequest collectionName:%s, partitionName:%s",
                requestParam.getCollectionName(), requestParam.getPartitionName());

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

            GetPartitionStatisticsRequest.Builder builder = GetPartitionStatisticsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetPartitionStatisticsResponse response = blockingStub().getPartitionStatistics(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(@NonNull ShowPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ShowPartitionsRequest collectionName:%s", requestParam.getCollectionName());

        try {
            ShowPartitionsRequest.Builder builder = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            ShowPartitionsResponse response = blockingStub().showPartitions(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createAlias(@NonNull CreateAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateAliasRequest collectionName:%s, alias:%s",
                requestParam.getCollectionName(), requestParam.getAlias());

        try {
            CreateAliasRequest.Builder builder = CreateAliasRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setAlias(requestParam.getAlias());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().createAlias(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropAlias(@NonNull DropAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropAliasRequest alias:%s", requestParam.getAlias());

        try {
            DropAliasRequest.Builder builder = DropAliasRequest.newBuilder()
                    .setAlias(requestParam.getAlias());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().dropAlias(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterAlias(@NonNull AlterAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("AlterAliasRequest collectionName:%s, alias:%s",
                requestParam.getCollectionName(), requestParam.getAlias());

        try {
            AlterAliasRequest.Builder builder = AlterAliasRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setAlias(requestParam.getAlias());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().alterAlias(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListAliasesResponse> listAliases(ListAliasesParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ListAliasesRequest collectionName:%s", requestParam.getCollectionName());

        try {
            ListAliasesRequest.Builder builder = ListAliasesRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            ListAliasesResponse response = blockingStub().listAliases(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createIndex(@NonNull CreateIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateIndexRequest collectionName:%s, fieldName:%s",
                requestParam.getCollectionName(), requestParam.getFieldName());

        try {
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

            Status response = blockingStub().createIndex(builder.build());
            handleResponse(title, response);

            if (requestParam.isSyncMode()) {
                R<Boolean> res = waitForIndex(requestParam.getDatabaseName(), requestParam.getCollectionName(), requestParam.getIndexName(),
                        requestParam.getFieldName(),
                        requestParam.getSyncWaitingInterval(), requestParam.getSyncWaitingTimeout());
                if (res.getStatus() != R.Status.Success.getCode()) {
                    logError("CreateIndexRequest in sync mode" + " failed:{}", res.getMessage());
                    return R.failed(R.Status.valueOf(res.getStatus()), res.getMessage());
                }
            }
            logDebug("{} in sync mode successfully!", title);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropIndex(@NonNull DropIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropIndexRequest collectionName:%s, indexName:%s",
                requestParam.getCollectionName(), requestParam.getIndexName());

        try {
            DropIndexRequest.Builder builder = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().dropIndex(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(@NonNull DescribeIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DescribeIndexRequest collectionName:%s, indexName:%s",
                requestParam.getCollectionName(), requestParam.getIndexName());

        try {
            DescribeIndexRequest.Builder builder = DescribeIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            DescribeIndexResponse response = blockingStub().describeIndex(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("GetIndexStateRequest collectionName:%s, indexName:%s",
                requestParam.getCollectionName(), requestParam.getIndexName());

        try {
            GetIndexStateRequest.Builder builder = GetIndexStateRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetIndexStateResponse response = blockingStub().getIndexState(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("GetIndexBuildProgressRequest collectionName:%s, indexName:%s",
                requestParam.getCollectionName(), requestParam.getIndexName());

        try {
            GetIndexBuildProgressRequest.Builder builder = GetIndexBuildProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetIndexBuildProgressResponse response = blockingStub().getIndexBuildProgress(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterIndex(AlterIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("AlterIndexRequest indexName:%s", requestParam.getIndexName());

        try {
            AlterIndexRequest.Builder builder = AlterIndexRequest.newBuilder();
            List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(requestParam.getProperties());
            if (CollectionUtils.isNotEmpty(propertiesList)) {
                propertiesList.forEach(builder::addExtraParams);
            }
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            AlterIndexRequest alterIndexRequest = builder
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            Status response = blockingStub().alterIndex(alterIndexRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> delete(@NonNull DeleteParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DeleteRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DeleteRequest.Builder builder = DeleteRequest.newBuilder()
                    .setBase(MsgBase.newBuilder().setMsgType(MsgType.Delete).build())
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .setExpr(requestParam.getExpr());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            MutationResult response = blockingStub().delete(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<MutationResult> insert(@NonNull InsertParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("InsertRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                return R.failed(descResp.getException());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
            MutationResult response = blockingStub().insert(builderWraper.buildInsertRequest());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("InsertAsyncRequest collectionName:%s", requestParam.getCollectionName());

        DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                .withDatabaseName(requestParam.getDatabaseName())
                .withCollectionName(requestParam.getCollectionName());
        R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
        if (descResp.getStatus() != R.Status.Success.getCode()) {
            return Futures.immediateFuture(R.failed(descResp.getException()));
        }

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
        ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
        ListenableFuture<MutationResult> response = futureStub().insert(builderWraper.buildInsertRequest());

        Futures.addCallback(
                response,
                new FutureCallback<MutationResult>() {
                    @Override
                    public void onSuccess(MutationResult result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("{} successfully!", title);
                        } else {
                            logError("{} failed:\n{}", title, result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("{} failed:\n{}", title, t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<MutationResult, R<MutationResult>> transformFunc =
                results -> {
                    Status status = results.getStatus();
                    if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
                        return R.failed(new ServerException(status.getReason(), status.getCode(), status.getErrorCode()));
                    } else {
                        return R.success(results);
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<MutationResult> upsert(UpsertParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("UpsertRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withDatabaseName(requestParam.getDatabaseName())
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                return R.failed(descResp.getException());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
            MutationResult response = blockingStub().upsert(builderWraper.buildUpsertRequest());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public ListenableFuture<R<MutationResult>> upsertAsync(UpsertParam requestParam) {
        if (!clientIsReady()) {
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Client rpc channel is not ready")));
        }

        logDebug(requestParam.toString());
        String title = String.format("UpsertAsyncRequest collectionName:%s", requestParam.getCollectionName());

        DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                .withDatabaseName(requestParam.getDatabaseName())
                .withCollectionName(requestParam.getCollectionName());
        R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
        if (descResp.getStatus() != R.Status.Success.getCode()) {
            return Futures.immediateFuture(R.failed(descResp.getException()));
        }

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
        ParamUtils.InsertBuilderWrapper builderWraper = new ParamUtils.InsertBuilderWrapper(requestParam, wrapper);
        ListenableFuture<MutationResult> response = futureStub().upsert(builderWraper.buildUpsertRequest());

        Futures.addCallback(
                response,
                new FutureCallback<MutationResult>() {
                    @Override
                    public void onSuccess(MutationResult result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("{} successfully!", title);
                        } else {
                            logError("{} failed:\n{}", title, result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("{} failed:\n{}", title, t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<MutationResult, R<MutationResult>> transformFunc =
                results -> {
                    Status status = results.getStatus();
                    if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
                        return R.failed(new ServerException(status.getReason(), status.getCode(), status.getErrorCode()));
                    } else {
                        return R.success(results);
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<SearchResults> search(@NonNull SearchParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("SearchRequest collectionName:%s", requestParam.getCollectionName());

        try {
            SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
            SearchResults response = this.blockingStub().search(searchRequest);

            //TODO: truncate distance value by round decimal

            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("SearchAsyncRequest collectionName:%s", requestParam.getCollectionName());

        SearchRequest searchRequest = ParamUtils.convertSearchParam(requestParam);
        ListenableFuture<SearchResults> response = this.futureStub().search(searchRequest);

        Futures.addCallback(
                response,
                new FutureCallback<SearchResults>() {
                    @Override
                    public void onSuccess(SearchResults result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("{} successfully!", title);
                        } else {
                            logError("{} failed:\n{}", title, result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("{} failed:\n{}", title, t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<SearchResults, R<SearchResults>> transformFunc =
                results -> {
                    Status status = results.getStatus();
                    if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
                        return R.failed(new ServerException(status.getReason(), status.getCode(), status.getErrorCode()));
                    } else {
                        return R.success(results);
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<SearchResults> hybridSearch(HybridSearchParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("HybridSearchRequest collectionName:%s", requestParam.getCollectionName());

        try {
            HybridSearchRequest searchRequest = ParamUtils.convertHybridSearchParam(requestParam);
            SearchResults response = this.blockingStub().hybridSearch(searchRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public ListenableFuture<R<SearchResults>> hybridSearchAsync(HybridSearchParam requestParam) {
        if (!clientIsReady()) {
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Client rpc channel is not ready")));
        }

        logDebug(requestParam.toString());
        String title = String.format("HybridSearchAsyncRequest collectionName:%s", requestParam.getCollectionName());

        HybridSearchRequest searchRequest = ParamUtils.convertHybridSearchParam(requestParam);
        ListenableFuture<SearchResults> response = this.futureStub().hybridSearch(searchRequest);

        Futures.addCallback(
                response,
                new FutureCallback<SearchResults>() {
                    @Override
                    public void onSuccess(SearchResults result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("{} successfully!", title);
                        } else {
                            logError("{} failed:\n{}", title, result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("{} failed:\n{}", title, t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<SearchResults, R<SearchResults>> transformFunc =
                results -> {
                    Status status = results.getStatus();
                    if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
                        return R.failed(new ServerException(status.getReason(), status.getCode(), status.getErrorCode()));
                    } else {
                        return R.success(results);
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<QueryResults> query(@NonNull QueryParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("QueryRequest collectionName:%s, expr:%s",
                requestParam.getCollectionName(), requestParam.getExpr());

        try {
            QueryRequest queryRequest = ParamUtils.convertQueryParam(requestParam);
            QueryResults response = this.blockingStub().query(queryRequest);

            // Keep this section to compatible with old v2.2.x versions
            // Server side behavior: if a query expression could not filter out any result,
            // or collection is empty, the server return ErrorCode.EmptyCollection.
            // Here we give a general message for this case.
            if (response.getStatus().getErrorCode() == ErrorCode.EmptyCollection) {
                logWarning("QueryRequest returns nothing: empty collection or improper expression");
                return R.failed(ErrorCode.EmptyCollection, "empty collection or improper expression");
            }

            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("QueryAsyncRequest collectionName:%s, expr:%s",
                requestParam.getCollectionName(), requestParam.getExpr());

        QueryRequest queryRequest = ParamUtils.convertQueryParam(requestParam);
        ListenableFuture<QueryResults> response = this.futureStub().query(queryRequest);

        Futures.addCallback(
                response,
                new FutureCallback<QueryResults>() {
                    @Override
                    public void onSuccess(QueryResults result) {
                        if (result.getStatus().getErrorCode() == ErrorCode.Success) {
                            logDebug("{} successfully!", title);
                        } else {
                            logError("{} failed:\n{}", title, result.getStatus().getReason());
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        logError("{} failed:\n{}", title, t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());

        Function<QueryResults, R<QueryResults>> transformFunc =
                results -> {
                    Status status = results.getStatus();
                    if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
                        return R.failed(new ServerException(status.getReason(), status.getCode(), status.getErrorCode()));
                    } else {
                        return R.success(results);
                    }
                };

        return Futures.transform(response, transformFunc::apply, MoreExecutors.directExecutor());
    }

    @Override
    public R<GetMetricsResponse> getMetrics(@NonNull GetMetricsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("QueryAsyncRequest request:%s", requestParam.getRequest());

        try {
            GetMetricsRequest getMetricsRequest = GetMetricsRequest.newBuilder()
                    .setRequest(requestParam.getRequest())
                    .build();

            GetMetricsResponse response = blockingStub().getMetrics(getMetricsRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetFlushStateResponse> getFlushState(@NonNull GetFlushStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetFlushState collectionName:%s", requestParam.getCollectionName());

        try {
            GetFlushStateRequest.Builder builder = GetFlushStateRequest.newBuilder()
                    .addAllSegmentIDs(requestParam.getSegmentIDs())
                    .setCollectionName(requestParam.getCollectionName())
                    .setFlushTs(requestParam.getFlushTs());

            if (StringUtils.isNotBlank(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetFlushStateResponse response = blockingStub().getFlushState(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetFlushAllStateResponse> getFlushAllState(GetFlushAllStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = "GetFlushAllState";

        try {
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
            GetFlushAllStateRequest getFlushStateRequest = GetFlushAllStateRequest.newBuilder()
                    .setBase(msgBase)
                    .setFlushAllTs(requestParam.getFlushAllTs())
                    .build();

            GetFlushAllStateResponse response = blockingStub().getFlushAllState(getFlushStateRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfo(@NonNull GetPersistentSegmentInfoParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetPersistentSegmentInfoRequest collectionName:%s",
                requestParam.getCollectionName());

        try {
            GetPersistentSegmentInfoRequest getSegmentInfoRequest = GetPersistentSegmentInfoRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            GetPersistentSegmentInfoResponse response = blockingStub().getPersistentSegmentInfo(getSegmentInfoRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetQuerySegmentInfoResponse> getQuerySegmentInfo(@NonNull GetQuerySegmentInfoParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetQuerySegmentInfoRequest collectionName:%s",
                requestParam.getCollectionName());

        try {
            GetQuerySegmentInfoRequest getSegmentInfoRequest = GetQuerySegmentInfoRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            GetQuerySegmentInfoResponse response = blockingStub().getQuerySegmentInfo(getSegmentInfoRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetReplicasResponse> getReplicas(GetReplicasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetReplicasRequest collectionName:%s", requestParam.getCollectionName());

        try {
            GetReplicasRequest.Builder builder = GetReplicasRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setWithShardNodes(requestParam.isWithShardNodes());

            if (StringUtils.isNotBlank(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }
            GetReplicasRequest getReplicasRequest = builder.build();
            GetReplicasResponse response = blockingStub().getReplicas(getReplicasRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadBalance(LoadBalanceParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = "LoadBalanceRequest";

        try {
            LoadBalanceRequest.Builder builder = LoadBalanceRequest.newBuilder()
                    .setSrcNodeID(requestParam.getSrcNodeID())
                    .addAllDstNodeIDs(requestParam.getDestNodeIDs())
                    .addAllSealedSegmentIDs(requestParam.getSegmentIDs());

            if (StringUtils.isNotBlank(requestParam.getCollectionName())) {
                builder.setCollectionName(requestParam.getCollectionName());
            }

            if (StringUtils.isNotBlank(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            Status response = blockingStub().loadBalance(builder.build());
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCompactionStateResponse> getCompactionState(GetCompactionStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetCompactionStateRequest compactionID:%d", requestParam.getCompactionID());

        try {
            GetCompactionStateRequest getCompactionStateRequest = GetCompactionStateRequest.newBuilder()
                    .setCompactionID(requestParam.getCompactionID())
                    .build();

            GetCompactionStateResponse response = blockingStub().getCompactionState(getCompactionStateRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ManualCompactionResponse> manualCompact(ManualCompactParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ManualCompactionRequest collectionName:%s", requestParam.getCollectionName());

        try {
            R<DescribeCollectionResponse> descResp = describeCollection(DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                return R.failed(descResp.getException());
            }

            ManualCompactionRequest manualCompactionRequest = ManualCompactionRequest.newBuilder()
                    .setCollectionID(descResp.getData().getCollectionID())
                    .build();

            ManualCompactionResponse response = blockingStub().manualCompaction(manualCompactionRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetCompactionPlansResponse> getCompactionStateWithPlans(GetCompactionPlansParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetCompactionPlansRequest compactionID:%d", requestParam.getCompactionID());

        try {
            GetCompactionPlansRequest getCompactionPlansRequest = GetCompactionPlansRequest.newBuilder()
                    .setCompactionID(requestParam.getCompactionID())
                    .build();

            GetCompactionPlansResponse response = blockingStub().getCompactionStateWithPlans(getCompactionPlansRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createCredential(CreateCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateCredentialRequest userName:%s", requestParam.getUsername());

        try {
            CreateCredentialRequest createCredentialRequest = CreateCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .setPassword(getBase64EncodeString(requestParam.getPassword()))
                    .build();

            Status response = blockingStub().createCredential(createCredentialRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> updateCredential(UpdateCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("UpdateCredentialRequest userName:%s", requestParam.getUsername());

        try {
            UpdateCredentialRequest updateCredentialRequest = UpdateCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .setOldPassword(getBase64EncodeString(requestParam.getOldPassword()))
                    .setNewPassword(getBase64EncodeString(requestParam.getNewPassword()))
                    .build();

            Status response = blockingStub().updateCredential(updateCredentialRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> deleteCredential(DeleteCredentialParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DeleteCredentialRequest userName:%s", requestParam.getUsername());

        try {
            DeleteCredentialRequest deleteCredentialRequest = DeleteCredentialRequest.newBuilder()
                    .setUsername(requestParam.getUsername())
                    .build();

            Status response = blockingStub().deleteCredential(deleteCredentialRequest);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListCredUsersResponse> listCredUsers(ListCredUsersParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = "ListCredUsersRequest";

        try {
            ListCredUsersRequest listCredUsersRequest = ListCredUsersRequest.newBuilder()
                    .build();

            ListCredUsersResponse response = blockingStub().listCredUsers(listCredUsersRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
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

        logDebug(requestParam.toString());
        String title = String.format("AddUserToRoleRequest userName:%s, roleName:%s",
                requestParam.getUserName(), requestParam.getRoleName());

        try {
            OperateUserRoleRequest request = OperateUserRoleRequest.newBuilder()
                    .setUsername(requestParam.getUserName())
                    .setRoleName(requestParam.getRoleName())
                    .setType(OperateUserRoleType.AddUserToRole)
                    .build();

            Status response = blockingStub().operateUserRole(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> removeUserFromRole(RemoveUserFromRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("RemoveUserFromRoleRequest userName:%s, roleName:%s",
                requestParam.getUserName(), requestParam.getRoleName());

        try {
            OperateUserRoleRequest request = OperateUserRoleRequest.newBuilder()
                    .setUsername(requestParam.getUserName())
                    .setRoleName(requestParam.getRoleName())
                    .setType(OperateUserRoleType.RemoveUserFromRole)
                    .build();

            Status response = blockingStub().operateUserRole(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> createRole(CreateRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("RemoveUserFromRoleRequest roleName:%s", requestParam.getRoleName());

        try {
            CreateRoleRequest request = CreateRoleRequest.newBuilder()
                    .setEntity(RoleEntity.newBuilder()
                            .setName(requestParam.getRoleName())
                            .build())
                    .build();

            Status response = blockingStub().createRole(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> dropRole(DropRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropRoleRequest roleName:%s", requestParam.getRoleName());

        try {
            DropRoleRequest request = DropRoleRequest.newBuilder()
                    .setRoleName(requestParam.getRoleName())
                    .build();

            Status response = blockingStub().dropRole(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<SelectRoleResponse> selectRole(SelectRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("SelectRoleRequest roleName:%s", requestParam.getRoleName());

        try {
            SelectRoleRequest request = SelectRoleRequest.newBuilder()
                    .setRole(RoleEntity.newBuilder()
                            .setName(requestParam.getRoleName())
                            .build())
                    .setIncludeUserInfo(requestParam.isIncludeUserInfo())
                    .build();

            SelectRoleResponse response = blockingStub().selectRole(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<SelectUserResponse> selectUser(SelectUserParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("SelectUserRequest userName:%s", requestParam.getUserName());

        try {
            SelectUserRequest request = SelectUserRequest.newBuilder()
                    .setUser(UserEntity.newBuilder().setName(requestParam.getUserName()).build())
                    .setIncludeRoleInfo(requestParam.isIncludeRoleInfo())
                    .build();

            SelectUserResponse response = blockingStub().selectUser(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GrantRolePrivilegeRequest roleName:%s", requestParam.getRoleName());

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
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    public R<RpcStatus> revokeRolePrivilege(RevokeRolePrivilegeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("RevokeRolePrivilegeRequest roleName:%s", requestParam.getRoleName());

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
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }


    public R<SelectGrantResponse> selectGrantForRole(SelectGrantForRoleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("SelectGrantForRoleRequest roleName:%s", requestParam.getRoleName());

        try {
            GrantEntity.Builder builder = GrantEntity.newBuilder()
                    .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            SelectGrantRequest request = SelectGrantRequest.newBuilder()
                    .setEntity(builder.build())
                    .build();

            SelectGrantResponse response = blockingStub().selectGrant(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    public R<SelectGrantResponse> selectGrantForRoleAndObject(SelectGrantForRoleAndObjectParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("SelectGrantForRoleAndObjectRequest roleName:%s", requestParam.getRoleName());

        try {
            SelectGrantRequest request = SelectGrantRequest.newBuilder()
                    .setEntity(GrantEntity.newBuilder()
                            .setRole(RoleEntity.newBuilder().setName(requestParam.getRoleName()).build())
                            .setObjectName(requestParam.getObjectName())
                            .setObject(ObjectEntity.newBuilder().setName(requestParam.getObject()).build())
                            .build())
                    .build();

            SelectGrantResponse response = blockingStub().selectGrant(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ImportResponse> bulkInsert(BulkInsertParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("BulkInsertRequest collectionName:%s", requestParam.getCollectionName());

        try {
            ImportRequest.Builder builder = ImportRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllFiles(requestParam.getFiles());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            if (StringUtils.isNotEmpty(requestParam.getPartitionName())) {
                builder.setPartitionName(requestParam.getPartitionName());
            }

            List<KeyValuePair> options = ParamUtils.AssembleKvPair(requestParam.getOptions());
            if (CollectionUtils.isNotEmpty(options)) {
                options.forEach(builder::addOptions);
            }

            ImportResponse response = blockingStub().import_(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetImportStateResponse> getBulkInsertState(GetBulkInsertStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetBulkInsertStateRequest taskID:%d", requestParam.getTask());

        try {
            GetImportStateRequest getImportStateRequest = GetImportStateRequest.newBuilder()
                    .setTask(requestParam.getTask())
                    .build();

            GetImportStateResponse response = blockingStub().getImportState(getImportStateRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListImportTasksResponse> listBulkInsertTasks(ListBulkInsertTasksParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("ListBulkInsertTasksRequest collectionName:%s",
                requestParam.getCollectionName());

        try {
            ListImportTasksRequest.Builder builder = ListImportTasksRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setLimit(requestParam.getLimit());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            ListImportTasksResponse response = blockingStub().listImportTasks(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetLoadingProgressResponse> getLoadingProgress(GetLoadingProgressParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetLoadingProgressRequest collectionName:%s",
                requestParam.getCollectionName());

        try {
            GetLoadingProgressRequest.Builder builder = GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetLoadingProgressResponse response = blockingStub().getLoadingProgress(builder.build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetLoadStateResponse> getLoadState(GetLoadStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("GetLoadStateRequest collectionName:%s",
                requestParam.getCollectionName());

        try {
            GetLoadStateRequest.Builder builder = GetLoadStateRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames());

            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                builder.setDbName(requestParam.getDatabaseName());
            }

            GetLoadStateRequest loadStateRequest = builder.build();

            GetLoadStateResponse response = blockingStub().getLoadState(loadStateRequest);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<CheckHealthResponse> checkHealth() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        String title = "CheckHealthRequest";

        try {
            CheckHealthResponse response = blockingStub().checkHealth(CheckHealthRequest.newBuilder().build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetVersionResponse> getVersion() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        String title = "GetVersionRequest";

        try {
            GetVersionResponse response = blockingStub().getVersion(GetVersionRequest.newBuilder().build());
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createResourceGroup(CreateResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("CreateResourceGroupRequest groupName:%s",
                requestParam.getGroupName());

        try {
            CreateResourceGroupRequest request = CreateResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .setConfig(requestParam.getConfig().toGRPC())
                    .build();

            Status response = blockingStub().createResourceGroup(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropResourceGroup(DropResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DropResourceGroupRequest groupName:%s",
                requestParam.getGroupName());

        try {
            DropResourceGroupRequest request = DropResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .build();

            Status response = blockingStub().dropResourceGroup(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListResourceGroupsResponse> listResourceGroups(ListResourceGroupsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = "ListResourceGroupsRequest";

        try {
            ListResourceGroupsRequest request = ListResourceGroupsRequest.newBuilder()
                    .build();

            ListResourceGroupsResponse response = blockingStub().listResourceGroups(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeResourceGroupResponse> describeResourceGroup(DescribeResourceGroupParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("DescribeResourceGroupRequest groupName:%s",
                requestParam.getGroupName());

        try {
            DescribeResourceGroupRequest request = DescribeResourceGroupRequest.newBuilder()
                    .setResourceGroup(requestParam.getGroupName())
                    .build();

            DescribeResourceGroupResponse response = blockingStub().describeResourceGroup(request);
            handleResponse(title, response.getStatus());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> transferNode(TransferNodeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("TransferNodeRequest nodeNumber:%d, sourceGroup:%s, targetGroup:%s",
                requestParam.getNodeNumber(), requestParam.getSourceGroupName(), requestParam.getTargetGroupName());

        try {
            TransferNodeRequest request = TransferNodeRequest.newBuilder()
                    .setSourceResourceGroup(requestParam.getSourceGroupName())
                    .setTargetResourceGroup(requestParam.getTargetGroupName())
                    .setNumNode(requestParam.getNodeNumber())
                    .build();

            Status response = blockingStub().transferNode(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> transferReplica(TransferReplicaParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());
        String title = String.format("TransferReplicaRequest replicaNumber:%d, sourceGroup:%s, targetGroup:%s",
                requestParam.getReplicaNumber(), requestParam.getSourceGroupName(), requestParam.getTargetGroupName());

        try {
            TransferReplicaRequest request = TransferReplicaRequest.newBuilder()
                    .setSourceResourceGroup(requestParam.getSourceGroupName())
                    .setTargetResourceGroup(requestParam.getTargetGroupName())
                    .setCollectionName(requestParam.getCollectionName())
                    .setDbName(requestParam.getDatabaseName())
                    .setNumReplica(requestParam.getReplicaNumber())
                    .build();

            Status response = blockingStub().transferReplica(request);
            handleResponse(title, response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> updateResourceGroups(UpdateResourceGroupsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logDebug(requestParam.toString());

        try {
            UpdateResourceGroupsRequest request = requestParam.toGRPC();

            Status response = blockingStub().updateResourceGroups(request);
            handleResponse(requestParam.toString(), response);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", requestParam.toString(), e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", requestParam.toString(), e);
            return R.failed(e);
        }
    }

    ///////////////////// High Level API//////////////////////
    @Override
    public R<RpcStatus> createCollection(CreateSimpleCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = "CreateSimpleCollectionRequest";

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

            logDebug("{} successfully!", title);
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<ListCollectionsResponse> listCollections(ListCollectionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = "ListCollectionsRequest";
        
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
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<InsertResponse> insert(InsertRowsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = "InsertRowsRequest";
        
        try {
            R<MutationResult> response = insert(requestParam.getInsertParam());
            if(!Objects.equals(response.getStatus(), R.success().getStatus())){
                logError("Insert failed: {}", response.getException().getMessage());
                return R.failed(response.getException());
            }

            logDebug("{} successfully!", title);
            MutationResultWrapper wrapper = new MutationResultWrapper(response.getData());
            return R.success(InsertResponse.builder().insertIds(wrapper.getInsertIDs()).insertCount(wrapper.getInsertCount()).build());
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<DeleteResponse> delete(DeleteIdsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = String.format("DeleteIdsRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(descResp.getException());
            }
            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());

            String expr = VectorUtils.convertPksExpr(requestParam.getPrimaryIds(), wrapper);
            DeleteParam deleteParam = DeleteParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .withPartitionName(requestParam.getPartitionName())
                    .withExpr(expr)
                    .build();
            R<MutationResult> resultR = delete(deleteParam);
            if (resultR.getStatus() != R.Status.Success.getCode()) {
                return R.failed(resultR.getException());
            }

            MutationResultWrapper resultWrapper = new MutationResultWrapper(resultR.getData());
            return R.success(DeleteResponse.builder().deleteIds(resultWrapper.getDeleteIDs()).build());
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<GetResponse> get(GetIdsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = "GetIdsRequest";

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(descResp.getException());
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
            if (queryResp.getStatus() != R.Status.Success.getCode()) {
                return R.failed(queryResp.getException());
            }

            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryResp.getData());
            return R.success(GetResponse.builder().rowRecords(queryResultsWrapper.getRowRecords()).build());
        } catch (StatusRuntimeException e) {
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<QueryResponse> query(QuerySimpleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = "QuerySimpleRequest";

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(descResp.getException());
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
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public R<SearchResponse> search(SearchSimpleParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        logDebug(requestParam.toString());
        String title = String.format("SearchSimpleRequest collectionName:%s", requestParam.getCollectionName());

        try {
            DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName());
            R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(descResp.getException());
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
            logError("{} RPC failed! Exception:{}", title, e);
            return R.failed(e);
        } catch (Exception e) {
            logError("{} failed! Exception:{}", title, e);
            return R.failed(e);
        }
    }

    @Override
    public R<QueryIterator> queryIterator(QueryIteratorParam requestParam) {
        DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                .withDatabaseName(requestParam.getDatabaseName())
                .withCollectionName(requestParam.getCollectionName());
        R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
        if (descResp.getStatus() != R.Status.Success.getCode()) {
            logError("Failed to describe collection: {}", requestParam.getCollectionName());
            return R.failed(descResp.getException());
        }
        DescCollResponseWrapper descCollResponseWrapper = new DescCollResponseWrapper(descResp.getData());
        QueryIterator queryIterator = new QueryIterator(requestParam, this.blockingStub(), descCollResponseWrapper.getPrimaryField());
        return R.success(queryIterator);
    }

    @Override
    public R<SearchIterator> searchIterator(SearchIteratorParam requestParam) {
        DescribeCollectionParam.Builder builder = DescribeCollectionParam.newBuilder()
                .withDatabaseName(requestParam.getDatabaseName())
                .withCollectionName(requestParam.getCollectionName());
        R<DescribeCollectionResponse> descResp = describeCollection(builder.build());
        if (descResp.getStatus() != R.Status.Success.getCode()) {
            logError("Failed to describe collection: {}", requestParam.getCollectionName());
            return R.failed(descResp.getException());
        }
        DescCollResponseWrapper descCollResponseWrapper = new DescCollResponseWrapper(descResp.getData());
        SearchIterator searchIterator = new SearchIterator(requestParam, this.blockingStub(), descCollResponseWrapper.getPrimaryField());
        return R.success(searchIterator);
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

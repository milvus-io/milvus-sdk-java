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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.StatusRuntimeException;
import io.milvus.exception.ClientNotConnectedException;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;
import io.milvus.param.bulkinsert.ListBulkInsertTasksParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.GetLoadingProgressParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.control.GetCompactionPlansParam;
import io.milvus.param.control.GetCompactionStateParam;
import io.milvus.param.control.GetFlushStateParam;
import io.milvus.param.control.GetMetricsParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.milvus.param.control.GetReplicasParam;
import io.milvus.param.control.LoadBalanceParam;
import io.milvus.param.control.ManualCompactParam;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.credential.ListCredUsersParam;
import io.milvus.param.credential.UpdateCredentialParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexBuildProgressParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.param.partition.DropPartitionParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.HasPartitionParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.param.role.AddUserToRoleParam;
import io.milvus.param.role.CreateRoleParam;
import io.milvus.param.role.DropRoleParam;
import io.milvus.param.role.GrantRolePrivilegeParam;
import io.milvus.param.role.RemoveUserFromRoleParam;
import io.milvus.param.role.RevokeRolePrivilegeParam;
import io.milvus.param.role.SelectGrantForRoleAndObjectParam;
import io.milvus.param.role.SelectGrantForRoleParam;
import io.milvus.param.role.SelectRoleParam;
import io.milvus.param.role.SelectUserParam;
import io.milvus.response.DescCollResponseWrapper;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    protected abstract boolean clientIsReady();

    ///////////////////// Internal Functions//////////////////////
    private List<KeyValuePair> assembleKvPair(Map<String, String> sourceMap) {
        List<KeyValuePair> result = new ArrayList<>();

        if (MapUtils.isNotEmpty(sourceMap)) {
            sourceMap.forEach((key, value) -> {
                KeyValuePair kv = KeyValuePair.newBuilder()
                        .setKey(key)
                        .setValue(value).build();
                result.add(kv);
            });
        }
        return result;
    }

    private void waitForLoadingCollection(String collectionName, List<String> partitionNames,
                                          long waitingInterval, long timeout) throws IllegalResponseException {
        long tsBegin = System.currentTimeMillis();
        if (partitionNames == null || partitionNames.isEmpty()) {
            ShowCollectionsRequest showCollectionRequest = ShowCollectionsRequest.newBuilder()
                    .addCollectionNames(collectionName)
                    .setType(ShowType.InMemory)
                    .build();

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
            ShowPartitionsRequest showPartitionsRequest = ShowPartitionsRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .addAllPartitionNames(partitionNames)
                    .setType(ShowType.InMemory).build();

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
                    String msg = "Waiting load, interval: " + waitingInterval + "ms.";
                    if (!partitionNoMemState.isEmpty()) {
                        msg += ("Partition " + partitionNoMemState + " has no memory state.");
                    }
                    if (!partitionNotFullyLoad.isEmpty()) {
                        msg += ("Partition " + partitionNotFullyLoad + " has not fully loaded.");
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
                    String msg = segmentIDs.getDataCount() + " segments of " + collectionName + " has been flushed.";
                    logDebug(msg);
                    break;
                }

                try {
                    String msg = "Waiting flush for " + collectionName + ", interval: " + waitingInterval + "ms. ";
                    logDebug(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting flush thread is interrupted, flush process may not be finished");
                    break;
                }
            }
        });
    }

    private R<Boolean> waitForIndex(String collectionName, String indexName,
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
                return R.failed(R.Status.Success, msg);
            }

            GetIndexStateRequest request = GetIndexStateRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setIndexName(indexName)
                    .build();

            GetIndexStateResponse response = blockingStub().getIndexState(request);
            if (response.getState() == IndexState.Finished) {
                break;
            } else if (response.getState() == IndexState.Failed) {
                String msg = "Get index state failed: " + response.toString();
                logError(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            try {
                String msg = "Waiting index, interval: " + waitingInterval + "ms. ";
                logDebug(msg);
                TimeUnit.MILLISECONDS.sleep(waitingInterval);
            } catch (InterruptedException e) {
                String msg = "Waiting index thread is interrupted, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.Success, msg);
            }
        }

        return R.failed(R.Status.Success, "Waiting index thread exist");
    }

    private <T> R<T> failedStatus(String requestName, io.milvus.grpc.Status status) {
        String reason = status.getReason();
        if (reason == null || reason.isEmpty()) {
            reason = "error code: " + status.getErrorCode().toString();
        }
        logError(requestName + " failed:\n{}", reason);
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
            HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
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
            logError("HasCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("HasCollectionRequest failed:\n{}", e.getMessage());
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
                    .setDescription(requestParam.getDescription());

            long fieldID = 0;
            for (FieldType fieldType : requestParam.getFieldTypes()) {
                FieldSchema.Builder fieldSchemaBuilder = FieldSchema.newBuilder()
                        .setFieldID(fieldID)
                        .setName(fieldType.getName())
                        .setIsPrimaryKey(fieldType.isPrimaryKey())
                        .setDescription(fieldType.getDescription())
                        .setDataType(fieldType.getDataType())
                        .setAutoID(fieldType.isAutoID());

                // assemble typeParams for CollectionSchema
                List<KeyValuePair> typeParamsList = assembleKvPair(fieldType.getTypeParams());
                if (CollectionUtils.isNotEmpty(typeParamsList)) {
                    typeParamsList.forEach(fieldSchemaBuilder::addTypeParams);
                }

                collectionSchemaBuilder.addFields(fieldSchemaBuilder.build());
                fieldID++;
            }

            // Construct CreateCollectionRequest
            CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setShardsNum(requestParam.getShardsNum())
                    .setSchema(collectionSchemaBuilder.build().toByteString())
                    .build();

            Status response = blockingStub().createCollection(createCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("CreateCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("CreateCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("CreateCollectionRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCollectionRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            DropCollectionRequest dropCollectionRequest = DropCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            Status response = blockingStub().dropCollection(dropCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logDebug("DropCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return failedStatus("DropCollectionRequest", response);
            }
        } catch (StatusRuntimeException e) {
            logError("DropCollectionRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DropCollectionRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .build();

            Status response = blockingStub().loadCollection(loadCollectionRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            // sync load, wait until collection finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getCollectionName(), null,
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadCollectionRequest successfully! Collection name:{}",
                    requestParam.getCollectionName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) { // gRPC could throw this exception
            logError("LoadCollectionRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (IllegalResponseException e) { // milvus exception for illegal response
            logError("LoadCollectionRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadCollectionRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            logError("ReleaseCollectionRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleaseCollectionRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            DescribeCollectionRequest describeCollectionRequest = DescribeCollectionRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            DescribeCollectionResponse response = blockingStub().describeCollection(describeCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeCollectionRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeCollectionRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("DescribeCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeCollectionRequest failed:\n{}", e.getMessage());
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
                        .addCollectionName(requestParam.getCollectionName())
                        .withSyncFlush(Boolean.TRUE)
                        .build());
                if (response.getStatus() != R.Status.Success.getCode()) {
                    return R.failed(R.Status.valueOf(response.getStatus()), response.getMessage());
                }
            }

            GetCollectionStatisticsRequest getCollectionStatisticsRequest = GetCollectionStatisticsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .build();

            GetCollectionStatisticsResponse response = blockingStub().getCollectionStatistics(getCollectionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetCollectionStatisticsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("GetCollectionStatisticsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetCollectionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCollectionStatisticsRequest failed:\n{}", e.getMessage());
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
            ShowCollectionsRequest showCollectionsRequest = ShowCollectionsRequest.newBuilder()
                    .addAllCollectionNames(requestParam.getCollectionNames())
                    .setType(requestParam.getShowType()).build();

            ShowCollectionsResponse response = blockingStub().showCollections(showCollectionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("ShowCollectionsRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("ShowCollectionsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("ShowCollectionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowCollectionsRequest failed:\n{}", e.getMessage());
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
            FlushRequest flushRequest = FlushRequest.newBuilder()
                    .setBase(msgBase)
                    .addAllCollectionNames(requestParam.getCollectionNames())
                    .build();
            FlushResponse response = blockingStub().flush(flushRequest);

            if (Objects.equals(requestParam.getSyncFlush(), Boolean.TRUE)) {
                waitForFlush(response, requestParam.getSyncFlushWaitingInterval(),
                        requestParam.getSyncFlushWaitingTimeout());
            }

            logDebug("FlushRequest successfully! Collection names:{}", requestParam.getCollectionNames());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("FlushRequest RPC failed! Collection names:{}\n{}",
                    requestParam.getCollectionNames(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("FlushRequest failed! Collection names:{}\n{}",
                    requestParam.getCollectionNames(), e.getMessage());
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
            logError("CreatePartitionRequest RPC failed! Collection name:{}, partition name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreatePartitionRequest failed! Collection name:{}, partition name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e.getMessage());
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
            logError("DropPartitionRequest RPC failed! Collection name:{}, partition name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DropPartitionRequest failed! Collection name:{}, partition name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionName(), e.getMessage());
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
            logError("HasPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("HasPartitionRequest failed:\n{}", e.getMessage());
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
            LoadPartitionsRequest loadPartitionsRequest = LoadPartitionsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setReplicaNumber(requestParam.getReplicaNumber())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            Status response = blockingStub().loadPartitions(loadPartitionsRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            // sync load, wait until all partitions finish loading
            if (requestParam.isSyncLoad()) {
                waitForLoadingCollection(requestParam.getCollectionName(), requestParam.getPartitionNames(),
                        requestParam.getSyncLoadWaitingInterval(), requestParam.getSyncLoadWaitingTimeout());
            }

            logDebug("LoadPartitionsRequest successfully! Collection name:{}, partition names:{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) { // gRPC could throw this exception
            logError("LoadPartitionsRequest RPC failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getStatus().toString());
            return R.failed(e);
        } catch (IllegalResponseException e) { // milvus exception for illegal response
            logError("LoadPartitionsRequest failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadPartitionsRequest failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getMessage());
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
            logError("ReleasePartitionsRequest RPC failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ReleasePartitionsRequest failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getMessage());
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
            logError("GetPartitionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetPartitionStatisticsRequest failed:\n{}", e.getMessage());
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
            logError("ShowPartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ShowPartitionsRequest failed:\n{}", e.getMessage());
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
            logError("CreateAliasRequest RPC failed! Collection name:{}, alias name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateAliasRequest failed! Collection name:{}, alias name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e.getMessage());
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
            logError("DropAliasRequest RPC failed! Alias name:{}\n{}",
                    requestParam.getAlias(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DropAliasRequest failed! Alias name:{}\n{}",
                    requestParam.getAlias(), e.getMessage());
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
            logError("AlterAliasRequest RPC failed! Collection name:{}, alias name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("AlterAliasRequest failed! Collection name:{}, alias name:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getAlias(), e.getMessage());
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
            CreateIndexRequest.Builder createIndexRequestBuilder = CreateIndexRequest.newBuilder();
            List<KeyValuePair> extraParamList = assembleKvPair(requestParam.getExtraParam());
            if (CollectionUtils.isNotEmpty(extraParamList)) {
                extraParamList.forEach(createIndexRequestBuilder::addExtraParams);
            }

            // keep consistence behavior with python sdk, if the index type is flat, return succeed with a warning
            // TODO: call dropIndex if the index type is flat
            // TODO: call describeCollection to check field name
            if (requestParam.getIndexName() == "FLAT" || requestParam.getIndexName() == "BIN_FLAT") {
                return R.success(new RpcStatus("Warning: It is not necessary to build index with index_type: FLAT"));
            }

            CreateIndexRequest createIndexRequest = createIndexRequestBuilder.setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            Status response = blockingStub().createIndex(createIndexRequest);
            if (response.getErrorCode() != ErrorCode.Success) {
                return failedStatus("CreateIndexRequest", response);
            }

            if (requestParam.isSyncMode()) {
                R<Boolean> res = waitForIndex(requestParam.getCollectionName(), requestParam.getIndexName(),
                        requestParam.getSyncWaitingInterval(), requestParam.getSyncWaitingTimeout());
                if (res.getStatus() != R.Status.Success.getCode()) {
                    return failedStatus("CreateIndexRequest in sync mode", response);
                }
            }
            logDebug("CreateIndexRequest successfully! Collection name:{} Field name:{}",
                    requestParam.getCollectionName(), requestParam.getFieldName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateIndexRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateIndexRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            DescribeIndexResponse descResp = blockingStub().describeIndex(describeIndexRequest);
            if (descResp.getStatus().getErrorCode() != ErrorCode.Success || descResp.getIndexDescriptionsCount() == 0) {
                logError("Index doesn't exist:\n{}", requestParam.getIndexName());
                return R.failed(R.Status.IndexNotExist, "Index doesn't exist");
            }

            DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .setFieldName(descResp.getIndexDescriptions(0).getFieldName())
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
            logError("DropIndexRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DropIndexRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setIndexName(requestParam.getIndexName())
                    .build();

            DescribeIndexResponse response = blockingStub().describeIndex(describeIndexRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("DescribeIndexRequest successfully!");
                return R.success(response);
            } else {
                return failedStatus("DescribeIndexRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("DescribeIndexRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DescribeIndexRequest failed:\n{}", e.getMessage());
            return R.failed(e);
        }
    }

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
            logError("GetIndexStateRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexStateRequest failed:\n{}", e.getMessage());
            return R.failed(e);
        }
    }

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
            logError("GetIndexBuildProgressRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetIndexBuildProgressRequest failed:\n{}", e.getMessage());
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
            logError("DeleteRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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
            R<DescribeCollectionResponse> descResp = describeCollection(DescribeCollectionParam.newBuilder()
                    .withCollectionName(requestParam.getCollectionName())
                    .build());
            if (descResp.getStatus() != R.Status.Success.getCode()) {
                logError("Failed to describe collection: {}", requestParam.getCollectionName());
                return R.failed(R.Status.valueOf(descResp.getStatus()), descResp.getMessage());
            }

            DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
            InsertRequest insertRequest = ParamUtils.convertInsertParam(requestParam, wrapper.getFields());
            MutationResult response = blockingStub().insert(insertRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("InsertRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                return failedStatus("InsertRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("InsertRequest RPC failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
            return R.failed(e);
        } catch (Exception e) {
            logError("InsertRequest failed! Collection name:{}\n{}",
                    requestParam.getCollectionName(), e.getMessage());
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

        R<DescribeCollectionResponse> descResp = describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(requestParam.getCollectionName())
                .build());
        if (descResp.getStatus() != R.Status.Success.getCode()) {
            logDebug("Failed to describe collection: {}", requestParam.getCollectionName());
            return Futures.immediateFuture(
                    R.failed(new ClientNotConnectedException("Failed to describe collection")));
        }

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descResp.getData());
        InsertRequest insertRequest = ParamUtils.convertInsertParam(requestParam, wrapper.getFields());
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
            logError("SearchRequest RPC failed:{}", e.getMessage());
            return R.failed(e);
        } catch (ParamException e) {
            logError("SearchRequest failed:\n{}", e.getMessage());
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
            logError("QueryRequest RPC failed:{}", e.getMessage());
            return R.failed(e);
        } catch (Exception e) {
            logError("QueryRequest failed:\n{}", e.getMessage());
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
            logError("GetMetricsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetMetricsRequest failed:\n{}", e.getMessage());
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
            logError("GetFlushState RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetFlushState failed:\n{}", e.getMessage());
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
            logError("GetPersistentSegmentInfoRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetPersistentSegmentInfoRequest failed:\n{}", e.getMessage());
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
            logError("GetQuerySegmentInfoRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetQuerySegmentInfoRequest failed:\n{}", e.getMessage());
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
            logError("GetReplicasRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetReplicasRequest failed:\n{}", e.getMessage());
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
            logError("LoadBalanceRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("LoadBalanceRequest failed:\n{}", e.getMessage());
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
            logError("GetCompactionStateRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCompactionStateRequest failed:\n{}", e.getMessage());
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
            logError("ManualCompactionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ManualCompactionRequest failed:\n{}", e.getMessage());
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
            logError("GetCompactionPlansRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCompactionPlansRequest failed:\n{}", e.getMessage());
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
                return failedStatus("CreateCredential", response);
            }

            logDebug("CreateCredential successfully! Username:{}",
                    requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateCredential RPC failed! Username:{}\n{}",
                    requestParam.getUsername(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateCredential failed! Username:{},\n{}",
                    requestParam.getUsername(), e.getMessage());
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
                return failedStatus("UpdateCredential", response);
            }

            logDebug("UpdateCredential successfully! Username:{}", requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("UpdateCredential RPC failed! Username:{}\n{}",
                    requestParam.getUsername(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("UpdateCredential failed! Username:{}\n{}",
                    requestParam.getUsername(), e.getMessage());
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
                return failedStatus("DeleteCredential", response);
            }

            logDebug("DeleteCredential successfully! Username:{}", requestParam.getUsername());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("DeleteCredential RPC failed! Username:{}\n{}", requestParam.getUsername(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteCredential failed! Username:{}\n{}", requestParam.getUsername(), e.getMessage());
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
                return failedStatus("ListCredUsers", response.getStatus());
            }

            logDebug("ListCredUsers successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("ListCredUsers RPC failed! \n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DeleteCredential failed! \n{}", e.getMessage());
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

            logDebug("AddUserToRole successfully! Username:{}, RoleName:{}", requestParam.getUserName(), request.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("AddUserToRole RPC failed! Username:{} RoleName:{} \n{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("AddUserToRole failed! Username:{} RoleName:{} \n{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e.getMessage());
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

            logDebug("RemoveUserFromRole successfully! Username:{}, RoleName:{}", requestParam.getUserName(), request.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("RemoveUserFromRole RPC failed! Username:{} RoleName:{} \n{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("RemoveUserFromRole failed! Username:{} RoleName:{} \n{}",
                    requestParam.getUserName(), requestParam.getRoleName(), e.getMessage());
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
                return failedStatus("CreateRole", response);
            }
            logDebug("CreateRole successfully! RoleName:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("CreateRole RPC failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CreateRole failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getMessage());
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
                return failedStatus("DropRole", response);
            }
            logDebug("DropRole successfully! RoleName:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("DropRole RPC failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("DropRole failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getMessage());
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
                return failedStatus("SelectRole", response.getStatus());
            }
            logDebug("SelectRole successfully! RoleName:{}", requestParam.getRoleName());
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectRole RPC failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectRole failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getMessage());
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
                return failedStatus("SelectUser", response.getStatus());
            }
            logDebug("SelectUser successfully! Request:{}", requestParam);
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectUser RPC failed! Request:{} \n{}",
                    requestParam, e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectUser failed! Request:{} \n{}",
                    requestParam, e.getMessage());
            return R.failed(e);
        }
    }


    public R<RpcStatus> grantRolePrivilege(GrantRolePrivilegeParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            OperatePrivilegeRequest request = OperatePrivilegeRequest.newBuilder()
                    .setType(OperatePrivilegeType.Grant)
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
                return failedStatus("GrantRolePrivilege", response);
            }
            logDebug("GrantRolePrivilege successfully! RoleName:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("GrantRolePrivilege RPC failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GrantRolePrivilege failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getMessage());
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
            logDebug("RevokeRolePrivilege successfully! RoleName:{}", requestParam.getRoleName());
            return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
        } catch (StatusRuntimeException e) {
            logError("RevokeRolePrivilege RPC failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("RevokeRolePrivilege failed! RoleName:{} \n{}",
                    requestParam.getRoleName(), e.getMessage());
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
                return failedStatus("SelectGrant", response.getStatus());
            }
            logDebug("SelectGrantForRole successfully! Request:{},", requestParam);
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectGrantForRole RPC failed! Request:{} \n{}",
                    requestParam, e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectGrantForRole failed! Request:{} \n{}",
                    requestParam, e.getMessage());
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
                return failedStatus("SelectGrant", response.getStatus());
            }
            logDebug("SelectGrantForRoleAndObject successfully! Request:{},", requestParam);
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("SelectGrantForRoleAndObject RPC failed! Request:{} \n{}",
                    requestParam, e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("SelectGrantForRoleAndObject failed! Request:{} \n{}",
                    requestParam, e.getMessage());
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
                    .setRowBased(requestParam.isRowBased())
                    .addAllFiles(requestParam.getFiles());

            if (StringUtils.isNotEmpty(requestParam.getPartitionName())) {
                importRequest.setPartitionName(requestParam.getPartitionName());
            }

            List<KeyValuePair> options = assembleKvPair(requestParam.getOptions());
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
            logError("BulkInsert RPC failed! \n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("BulkInsert failed! \n{}", e.getMessage());
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
            logError("GetBulkInsertState RPC failed! \n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetBulkInsertState failed! \n{}", e.getMessage());
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
            logError("ListBulkInsertTasks RPC failed! \n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("ListBulkInsertTasks failed! \n{}", e.getMessage());
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
            GetLoadingProgressRequest releasePartitionsRequest = GetLoadingProgressRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .build();

            GetLoadingProgressResponse response = blockingStub().getLoadingProgress(releasePartitionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logDebug("GetLoadingProgressParam successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(response);
            } else {
                return failedStatus("ReleasePartitionsRequest", response.getStatus());
            }
        } catch (StatusRuntimeException e) {
            logError("GetLoadingProgressParam RPC failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetLoadingProgressParam failed! Collection name:{}, partition names:{}\n{}",
                    requestParam.getCollectionName(), requestParam.getPartitionNames(), e.getMessage());
            return R.failed(e);
        }
    }

    public R<CheckHealthResponse> checkHealth() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        try {
            CheckHealthResponse response = blockingStub().checkHealth(CheckHealthRequest.newBuilder().build());
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectGrant", response.getStatus());
            }
            logDebug("CheckHealth successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("CheckHealth RPC failed!", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("CheckHealth failed! ", e.getMessage());
            return R.failed(e);
        }
    }

    public R<GetVersionResponse> getVersion() {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }
        try {
            GetVersionResponse response = blockingStub().getVersion(GetVersionRequest.newBuilder().build());
            if (response.getStatus().getErrorCode() != ErrorCode.Success) {
                return failedStatus("SelectGrant", response.getStatus());
            }
            logDebug("GetVersion successfully!");
            return R.success(response);
        } catch (StatusRuntimeException e) {
            logError("GetVersion RPC failed!", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetVersion failed! ", e.getMessage());
            return R.failed(e);
        }
    }

    ///////////////////// Log Functions//////////////////////
    private void logDebug(String msg, Object... params) {
        logger.debug(msg, params);
    }

    private void logInfo(String msg, Object... params) {
        logger.info(msg, params);
    }

    private void logWarning(String msg, Object... params) {
        logger.warn(msg, params);
    }

    private void logError(String msg, Object... params) {
        logger.error(msg, params);
    }
}

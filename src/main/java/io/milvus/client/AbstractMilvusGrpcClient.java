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

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.milvus.exception.ClientNotConnectedException;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.control.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @SuppressWarnings("unchecked")
    private FieldData genFieldData(String fieldName, DataType dataType, List<?> objects) {
        if (objects == null) {
            throw new ParamException("Cannot generate FieldData from null object");
        }
        FieldData.Builder builder = FieldData.newBuilder();
        if (vectorDataType.contains(dataType)) {
            if (dataType == DataType.FloatVector) {
                List<Float> floats = new ArrayList<>();
                // each object is List<Float>
                for (Object object : objects) {
                    if (object instanceof List) {
                        List<Float> list = (List<Float>) object;
                        floats.addAll(list);
                    } else {
                        throw new ParamException("The type of FloatVector must be List<Float>");
                    }
                }

                int dim = floats.size() / objects.size();
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
                return builder.setFieldName(fieldName).setType(DataType.FloatVector).setVectors(vectorField).build();
            } else if (dataType == DataType.BinaryVector) {
                ByteBuffer totalBuf = null;
                int dim = 0;
                // each object is ByteBuffer
                for (Object object : objects) {
                    ByteBuffer buf = (ByteBuffer) object;
                    if (totalBuf == null){
                        totalBuf = ByteBuffer.allocate(buf.position() * objects.size());
                        totalBuf.put(buf.array());
                        dim = buf.position() * 8;
                    } else {
                        totalBuf.put(buf.array());
                    }
                }

                assert totalBuf != null;
                ByteString byteString = ByteString.copyFrom(totalBuf.array());
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
                return builder.setFieldName(fieldName).setType(DataType.BinaryVector).setVectors(vectorField).build();
            }
        } else {
            switch (dataType) {
                case None:
                case UNRECOGNIZED:
                    throw new ParamException("Cannot support this dataType:" + dataType);
                case Int64:
                    List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                    LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                    ScalarField scalarField1 = ScalarField.newBuilder().setLongData(longArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField1).build();
                case Int32:
                case Int16:
                case Int8:
                    List<Integer> integers = objects.stream().map(p -> p instanceof Short ? ((Short)p).intValue() :(Integer) p).collect(Collectors.toList());
                    IntArray intArray = IntArray.newBuilder().addAllData(integers).build();
                    ScalarField scalarField2 = ScalarField.newBuilder().setIntData(intArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField2).build();
                case Bool:
                    List<Boolean> booleans = objects.stream().map(p -> (Boolean) p).collect(Collectors.toList());
                    BoolArray boolArray = BoolArray.newBuilder().addAllData(booleans).build();
                    ScalarField scalarField3 = ScalarField.newBuilder().setBoolData(boolArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField3).build();
                case Float:
                    List<Float> floats = objects.stream().map(p -> (Float) p).collect(Collectors.toList());
                    FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                    ScalarField scalarField4 = ScalarField.newBuilder().setFloatData(floatArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField4).build();
                case Double:
                    List<Double> doubles = objects.stream().map(p -> (Double) p).collect(Collectors.toList());
                    DoubleArray doubleArray = DoubleArray.newBuilder().addAllData(doubles).build();
                    ScalarField scalarField5 = ScalarField.newBuilder().setDoubleData(doubleArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField5).build();
                case String:
                    List<String> strings = objects.stream().map(p -> (String) p).collect(Collectors.toList());
                    StringArray stringArray = StringArray.newBuilder().addAllData(strings).build();
                    ScalarField scalarField6 = ScalarField.newBuilder().setStringData(stringArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField6).build();
            }
        }

        return null;
    }

    private static final Set<DataType> vectorDataType = new HashSet<DataType>() {{
        add(DataType.FloatVector);
        add(DataType.BinaryVector);
    }};

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
                if ((tsNow - tsBegin) >= timeout*1000) {
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
                    logInfo("Waiting load, interval: {} ms, percentage: {}%", waitingInterval, percentage);
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
            while(true) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout*1000) {
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
                    logInfo(msg);
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
        // This method use getPersistentSegmentInfo() to check segment state.
        // If all segments state become Flushed, then we say the sync flush action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        Map<String, LongArray> collectionSegIDs = flushResponse.getCollSegIDsMap();
        collectionSegIDs.forEach((collectionName, segmentIDs) -> {
            while (segmentIDs.getDataCount() > 0) {
                long tsNow = System.currentTimeMillis();
                if ((tsNow - tsBegin) >= timeout*1000) {
                    logWarning("Waiting flush thread is timeout, flush process may not be finished");
                    break;
                }

                GetPersistentSegmentInfoRequest getSegInfoRequest = GetPersistentSegmentInfoRequest.newBuilder()
                        .setCollectionName(collectionName)
                        .build();
                GetPersistentSegmentInfoResponse response = blockingStub().getPersistentSegmentInfo(getSegInfoRequest);
                List<PersistentSegmentInfo> segmentInfoArray = response.getInfosList();
                int flushedCount = 0;
                for (int i = 0; i < segmentIDs.getDataCount(); ++i) {
                    for (PersistentSegmentInfo info : segmentInfoArray) {
                        if (info.getSegmentID() == segmentIDs.getData(i) && info.getState() == SegmentState.Flushed) {
                            flushedCount++;
                            break;
                        }
                    }
                }

                // if all segment of this collection has been flushed, break this circle and check next collection
                if (flushedCount == segmentIDs.getDataCount()) {
                    break;
                }

                try {
                    String msg = "Waiting flush, interval: " + waitingInterval + "ms. " + flushedCount +
                            " of " + segmentIDs.getDataCount() + " segments flushed.";
                    logInfo(msg);
                    TimeUnit.MILLISECONDS.sleep(waitingInterval);
                } catch (InterruptedException e) {
                    logWarning("Waiting flush thread is interrupted, flush process may not be finished");
                    break;
                }
            }
        });
    }

    private R<Boolean> waitForIndex(String collectionName, String fieldName, long waitingInterval, long timeout) {
        // This method use getIndexState() to check index state.
        // If all index state become Finished, then we say the sync index action is finished.
        // If waiting time exceed timeout, exist the circle
        long tsBegin = System.currentTimeMillis();
        while (true) {
            long tsNow = System.currentTimeMillis();
            if ((tsNow - tsBegin) >= timeout*1000) {
                String msg = "Waiting index thread is timeout, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.Success, msg);
            }

            GetIndexStateRequest request = GetIndexStateRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setFieldName(fieldName)
                    .build();

            GetIndexStateResponse response = blockingStub().getIndexState(request);
            if (response.getState() == IndexState.Finished) {
                break;
            } else if (response.getState() == IndexState.Failed) {
                String msg = "Index failed: " + response.getFailReason();
                logError(msg);
                return R.failed(R.Status.UnexpectedError, msg);
            }

            try {
                String msg = "Waiting index, interval: " + waitingInterval + "ms. ";
                logInfo(msg);
                TimeUnit.MILLISECONDS.sleep(waitingInterval);
            } catch (InterruptedException e) {
                String msg = "Waiting index thread is interrupted, index process may not be finished";
                logWarning(msg);
                return R.failed(R.Status.Success, msg);
            }
        }

        return R.failed(R.Status.Success, "Waiting index thread exist");
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
                logInfo("HasCollectionRequest successfully!");
                Boolean value = Optional.of(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);
                return R.success(value);
            } else {
                logError("HasCollectionRequest failed!\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("CreateCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("CreateCollectionRequest failed!\n{}", response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("DropCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("DropCollectionRequest failed! Collection name:{}\n{}",
                        requestParam.getCollectionName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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

            logInfo("LoadCollectionRequest successfully! Collection name:{}",
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
                logInfo("ReleaseCollectionRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("ReleaseCollectionRequest failed! Collection name:{}\n{}",
                        requestParam.getCollectionName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("DescribeCollectionRequest successfully!");
                return R.success(response);
            } else {
                logError("DescribeCollectionRequest failed!\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("GetCollectionStatisticsRequest successfully!");
                return R.success(response);
            } else {
                logError("GetCollectionStatisticsRequest failed!\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("ShowCollectionsRequest successfully!");
                return R.success(response);
            } else {
                logError("ShowCollectionsRequest failed!\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
     * Currently we don't allow client call this method since server side has no compaction function
     * Now this method is only internally used by getCollectionStatistics()
     */
//    @Override
    private R<FlushResponse> flush(@NonNull FlushParam requestParam) {
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

            if (requestParam.getSyncFlush() == Boolean.TRUE) {
                waitForFlush(response, requestParam.getSyncFlushWaitingInterval(),
                        requestParam.getSyncFlushWaitingTimeout());
            }

            logInfo("FlushRequest successfully! Collection names:{}", requestParam.getCollectionNames());
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
                logInfo("CreatePartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("CreatePartitionRequest failed! Collection name:{}, partition name:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("DropPartitionRequest successfully! Collection name:{}, partition name:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("DropPartitionRequest failed! Collection name:{}, partition name:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getPartitionName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("HasPartitionRequest successfully!");
                Boolean result = response.getValue();
                return R.success(result);
            } else {
                logError("HasPartitionRequest failed!\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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

            logInfo("LoadPartitionsRequest successfully! Collection name:{}, partition names:{}",
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
                logInfo("ReleasePartitionsRequest successfully! Collection name:{}, partition names:{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("ReleasePartitionsRequest failed! Collection name:{}, partition names:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getPartitionNames(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
            GetPartitionStatisticsRequest getPartitionStatisticsRequest = GetPartitionStatisticsRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setPartitionName(requestParam.getPartitionName())
                    .build();

            GetPartitionStatisticsResponse response =
                    blockingStub().getPartitionStatistics(getPartitionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("GetPartitionStatisticsRequest successfully!");
                return R.success(response);
            } else {
                logError("ReleasePartitionsRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("GetPartitionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetQuerySegmentInfoRequest failed:\n{}", e.getMessage());
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
                logInfo("ShowPartitionsRequest successfully!");
                return R.success(response);
            } else {
                logError("ShowPartitionsRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("CreateAliasRequest successfully! Collection name:{}, alias name:{}",
                        requestParam.getCollectionName(), requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("CreateAliasRequest failed! Collection name:{}, alias name:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getAlias(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("DropAliasRequest successfully! Alias name:{}", requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("DropAliasRequest failed! Alias name:{}\n{}",
                        requestParam.getAlias(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("AlterAliasRequest successfully! Collection name:{}, alias name:{}",
                        requestParam.getCollectionName(), requestParam.getAlias());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("AlterAliasRequest failed! Collection name:{}, alias name:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getAlias(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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

            CreateIndexRequest createIndexRequest = createIndexRequestBuilder.setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName()).build();

            Status response = blockingStub().createIndex(createIndexRequest);

            if (response.getErrorCode() != ErrorCode.Success) {
                logError("CreateIndexRequest failed! Collection name:{} Field name:{}\n{}",
                        requestParam.getCollectionName(), requestParam.getFieldName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }

            if (requestParam.isSyncMode()) {
                R<Boolean> res = waitForIndex(requestParam.getCollectionName(), requestParam.getFieldName(),
                        requestParam.getSyncWaitingInterval(), requestParam.getSyncWaitingTimeout());
                if (res.getStatus() != R.Status.Success.getCode()) {
                    logError("CreateIndexRequest failed in sync mode! Collection name:{} Field name:{}\n{}",
                            requestParam.getCollectionName(), requestParam.getFieldName(), response.getReason());
                    return R.failed(R.Status.valueOf(res.getStatus()), res.getMessage());
                }
            }

            logInfo("CreateIndexRequest successfully! Collection name:{} Field name:{}",
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
            DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                    .setCollectionName(requestParam.getCollectionName())
                    .setFieldName(requestParam.getFieldName())
                    .build();

            Status response = blockingStub().dropIndex(dropIndexRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("DropIndexRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("DropIndexRequest failed! Collection name:{}\n{}",
                        requestParam.getCollectionName(), response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                    .setFieldName(requestParam.getFieldName())
                    .build();

            DescribeIndexResponse response = blockingStub().describeIndex(describeIndexRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("DescribeIndexRequest successfully!");
                return R.success(response);
            } else {
                logError("DescribeIndexRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                    .setFieldName(requestParam.getFieldName())
                    .build();

            GetIndexStateResponse response = blockingStub().getIndexState(getIndexStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("GetIndexStateRequest successfully!");
                return R.success(response);
            } else {
                logError("GetIndexStateRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                    .build();

            GetIndexBuildProgressResponse response = blockingStub().getIndexBuildProgress(getIndexBuildProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("GetIndexBuildProgressRequest successfully!");
                return R.success(response);
            } else {
                logError("GetIndexBuildProgressRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("DeleteRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                logError("DeleteRequest failed! Collection name:{}\n{}",
                        requestParam.getCollectionName(), response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
            String collectionName = requestParam.getCollectionName();
            String partitionName = requestParam.getPartitionName();
            List<InsertParam.Field> fields = requestParam.getFields();

            //1. gen insert request
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            InsertRequest.Builder insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setPartitionName(partitionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getRowCount());

            //2. gen fieldData
            // TODO: check field type(use DescribeCollection get schema to compare)
            for (InsertParam.Field field : fields) {
                insertBuilder.addFieldsData(genFieldData(field.getName(), field.getType(), field.getValues()));
            }

            //3. call insert
            InsertRequest insertRequest = insertBuilder.build();
            MutationResult response = blockingStub().insert(insertRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("InsertRequest successfully! Collection name:{}",
                        requestParam.getCollectionName());
                return R.success(response);
            } else {
                logError("InsertRequest failed! Collection name:{}\n{}",
                        requestParam.getCollectionName(), response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
    @SuppressWarnings("unchecked")
    public R<SearchResults> search(@NonNull SearchParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            SearchRequest.Builder builder = SearchRequest.newBuilder()
                    .setDbName("")
                    .setCollectionName(requestParam.getCollectionName());
            if (!requestParam.getPartitionNames().isEmpty()) {
                requestParam.getPartitionNames().forEach(builder::addPartitionNames);
            }

            // prepare target vectors
            // TODO: check target vector dimension(use DescribeCollection get schema to compare)
            PlaceholderType plType = PlaceholderType.None;
            List<?> vectors = requestParam.getVectors();
            List<ByteString> byteStrings = new ArrayList<>();
            for (Object vector : vectors) {
                if (vector instanceof List) {
                    plType = PlaceholderType.FloatVector;
                    List<Float> list = (List<Float>) vector;
                    ByteBuffer buf = ByteBuffer.allocate(Float.BYTES * list.size());
                    buf.order(ByteOrder.LITTLE_ENDIAN);
                    list.forEach(buf::putFloat);

                    byte[] array = buf.array();
                    ByteString bs = ByteString.copyFrom(array);
                    byteStrings.add(bs);
                } else if (vector instanceof ByteBuffer) {
                    plType = PlaceholderType.BinaryVector;
                    ByteBuffer buf = (ByteBuffer) vector;
                    byte[] array = buf.array();
                    ByteString bs = ByteString.copyFrom(array);
                    byteStrings.add(bs);
                } else {
                    String msg = "Search target vector type is illegal(Only allow List<Float> or ByteBuffer)";
                    logError(msg);
                    return R.failed(R.Status.UnexpectedError, msg);
                }
            }

            PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                    .setTag(Constant.VECTOR_TAG)
                    .setType(plType);
            byteStrings.forEach(pldBuilder::addValues);

            PlaceholderValue plv = pldBuilder.build();
            PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                    .addPlaceholders(plv)
                    .build();

            ByteString byteStr = placeholderGroup.toByteString();
            builder.setPlaceholderGroup(byteStr);

            // search parameters
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.VECTOR_FIELD)
                            .setValue(requestParam.getVectorFieldName())
                            .build())
                    .addSearchParams(
                            KeyValuePair.newBuilder()
                                    .setKey(Constant.TOP_K)
                                    .setValue(String.valueOf(requestParam.getTopK()))
                                    .build())
                    .addSearchParams(
                            KeyValuePair.newBuilder()
                                    .setKey(Constant.METRIC_TYPE)
                                    .setValue(requestParam.getMetricType())
                                    .build())
                    .addSearchParams(
                            KeyValuePair.newBuilder()
                                    .setKey(Constant.ROUND_DECIMAL)
                                    .setValue(String.valueOf(requestParam.getRoundDecimal()))
                                    .build());

            if (null != requestParam.getParams() && !requestParam.getParams().isEmpty()) {
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.PARAMS)
                                .setValue(requestParam.getParams())
                                .build());
            }

            if (!requestParam.getOutFields().isEmpty()) {
                requestParam.getOutFields().forEach(builder::addOutputFields);
            }

            // always use expression since dsl is discarded
            builder.setDslType(DslType.BoolExprV1);
            if (requestParam.getExpr() != null && !requestParam.getExpr().isEmpty()) {
                builder.setDsl(requestParam.getExpr());
            }

            SearchRequest searchRequest = builder.build();
            SearchResults response = this.blockingStub().search(searchRequest);

            //TODO: truncate distance value by round decimal

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("SearchRequest successfully!");
                return R.success(response);
            } else {
                logError("SearchRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("SearchRequest RPC failed:{}", e.getMessage());
            return R.failed(e);
        } catch (Exception e) {
            logError("SearchRequest failed:\n{}", e.getMessage());
            return R.failed(e);
        }
    }

    @Override
    public R<QueryResults> query(@NonNull QueryParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            QueryRequest queryRequest = QueryRequest.newBuilder()
                    .setDbName("")
                    .setCollectionName(requestParam.getCollectionName())
                    .addAllPartitionNames(requestParam.getPartitionNames())
                    .addAllOutputFields(requestParam.getOutFields())
                    .setExpr(requestParam.getExpr())
                    .build();

            QueryResults response = this.blockingStub().query(queryRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("QueryRequest successfully!");
                return R.success(response);
            } else {
                logError("QueryRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
    public R<CalcDistanceResults> calcDistance(@NonNull CalcDistanceParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            List<List<Float>> vectors_left = requestParam.getVectorsLeft();
            List<List<Float>> vectors_right = requestParam.getVectorsRight();

            FloatArray.Builder left_float_array = FloatArray.newBuilder();
            for (List<Float> vector : vectors_left) {
                left_float_array.addAllData(vector);
            }

            FloatArray.Builder right_float_array = FloatArray.newBuilder();
            for (List<Float> vector : vectors_right) {
                right_float_array.addAllData(vector);
            }

            CalcDistanceRequest calcDistanceRequest = CalcDistanceRequest.newBuilder()
                    .setOpLeft(
                            VectorsArray.newBuilder()
                                    .setDataArray(
                                            VectorField.newBuilder()
                                                    .setFloatVector(left_float_array.build())
                                                    .setDim(vectors_left.get(0).size())
                                                    .build()
                                    )
                                    .build()
                    )
                    .setOpRight(
                            VectorsArray.newBuilder()
                                    .setDataArray(
                                            VectorField.newBuilder()
                                                    .setFloatVector(right_float_array.build())
                                                    .setDim(vectors_right.get(0).size())
                                                    .build()
                                    )
                                    .build()
                    )
                    .addParams(
                            KeyValuePair.newBuilder()
                                    .setKey("metric")
                                    .setValue(requestParam.getMetricType())
                                    .build()
                    )
                    .build();

            CalcDistanceResults response = blockingStub().calcDistance(calcDistanceRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("CalcDistanceRequest successfully!");
                return R.success(response);
            } else {
                logError("CalcDistanceRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("CalcDistanceRequest RPC failed:{}", e.getMessage());
            return R.failed(e);
        } catch (Exception e) {
            logError("CalcDistanceRequest failed:\n{}", e.getMessage());
            return R.failed(e);
        }
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
                logInfo("GetMetricsRequest successfully!");
                return R.success(response);
            } else {
                logError("GetMetricsRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("GetPersistentSegmentInfoRequest successfully!");
                return R.success(response);
            } else {
                logError("GetPersistentSegmentInfoRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("GetQuerySegmentInfoRequest successfully!");
                return R.success(response);
            } else {
                logError("GetQuerySegmentInfoRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("LoadBalanceRequest successfully!");
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logError("LoadBalanceRequest failed! \n{}", response.getReason());
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
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
                logInfo("GetCompactionStateRequest successfully!");
                return R.success(response);
            } else {
                logError("GetCompactionStateRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
    public R<ManualCompactionResponse> manualCompaction(ManualCompactionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        logInfo(requestParam.toString());

        try {
            ManualCompactionRequest manualCompactionRequest = ManualCompactionRequest.newBuilder()
                    .setCollectionID(requestParam.getCollectionID())
                    .build();

            ManualCompactionResponse response = blockingStub().manualCompaction(manualCompactionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("ManualCompactionRequest successfully!");
                return R.success(response);
            } else {
                logError("ManualCompactionRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
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
                logInfo("GetCompactionPlansRequest successfully!");
                return R.success(response);
            } else {
                logError("GetCompactionPlansRequest failed:\n{}", response.getStatus().getReason());
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("GetCompactionPlansRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        } catch (Exception e) {
            logError("GetCompactionPlansRequest failed:\n{}", e.getMessage());
            return R.failed(e);
        }
    }

    ///////////////////// Log Functions//////////////////////

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

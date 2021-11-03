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
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    protected abstract boolean clientIsReady();

    @Override
    public R<FlushResponse> flush(String collectionName, String dbName) {
        return flush(Collections.singletonList(collectionName), dbName);
    }

    @Override
    public R<FlushResponse> flush(String collectionName) {
        return flush(Collections.singletonList(collectionName), "");
    }

    @Override
    public R<FlushResponse> flush(List<String> collectionNames) {
        return flush(collectionNames, "");
    }

    @Override
    public R<FlushResponse> flush(List<String> collectionNames, String dbName) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        for (String collectionName : collectionNames) {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
        }

        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
        FlushRequest.Builder builder = FlushRequest.newBuilder().setBase(msgBase).setDbName(dbName);
        collectionNames.forEach(builder::addCollectionNames);
        FlushRequest flushRequest = builder.build();
        FlushResponse flush = null;
        try {
            flush = blockingStub().flush(flushRequest);
        } catch (Exception e) {
            logger.error("[milvus] flush rpc request error:{}", e.getMessage());
            return R.failed(e);
        }
        return R.success(flush);
    }

    @Override
    public R<MutationResult> delete(DeleteParam deleteParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setBase(MsgBase.newBuilder().setMsgType(MsgType.Delete).build())
                .setCollectionName(deleteParam.getCollectionName())
                .setPartitionName(deleteParam.getPartitionName())
                .build();

        MutationResult delete = null;
        try {
            delete = blockingStub().delete(deleteRequest);
        } catch (Exception e) {
            logger.error("[milvus] delete rpc request error:{}", e.getMessage());
            return R.failed(e);
        }
        return R.success(delete);
    }

    @Override
    public R<MutationResult> insert(InsertParam insertParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        String collectionName = insertParam.getCollectionName();
        String partitionName = insertParam.getPartitionName();
        List<InsertParam.Field> fields = insertParam.getFields();

        //1. gen insert request
        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
        InsertRequest.Builder insertBuilder = InsertRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName)
                .setBase(msgBase)
                .setNumRows(insertParam.getRowCount());

        //2. gen fieldData
        // TODO: check field type(use DescribeCollection get schema to compare)
        for (InsertParam.Field field : fields) {
            insertBuilder.addFieldsData(genFieldData(field.getName(), field.getType(), field.getValues()));
        }

        //3. call insert
        InsertRequest insertRequest = insertBuilder.build();
        MutationResult insert = null;
        try {
            insert = blockingStub().insert(insertRequest);
        } catch (Exception e) {
            logger.error("[milvus] insert rpc request error:{}", e.getMessage());
            return R.failed(e);
        }
        return R.success(insert);
    }

    @Override
    public R<SearchResults> search(SearchParam searchParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setDbName(searchParam.getDbName())
                .setCollectionName(searchParam.getCollectionName());
        if (!searchParam.getPartitionNames().isEmpty()) {
            searchParam.getPartitionNames().forEach(builder::addPartitionNames);
        }

        // prepare target vectors
        // TODO: check target vector dimension(use DescribeColection get schema to compare)
        List<?> vectors = searchParam.getVectors();
        List<ByteString> byteStrings = vectors.stream().map(vector -> {
            if (vector instanceof ByteBuffer) {
                return ByteString.copyFrom((ByteBuffer) vector);
            }

            if (vector instanceof List) {
                List list = (List) vector;
                if (list.get(0) instanceof Float) {
                    ByteBuffer buf = ByteBuffer.allocate(Float.BYTES * list.size());
                    list.forEach(v -> buf.putFloat((Float) v));

                    byte[] array = buf.array();
                    return ByteString.copyFrom(array);
                }
            }

            logger.error("Search target vector type is illegal(Only allow List<Float> or ByteBuffer)");
            return null;
        }).collect(Collectors.toList());

        PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                .setTag(Constant.VECTOR_TAG)
                .setType(PlaceholderType.FloatVector);
        byteStrings.forEach(pldBuilder::addValues);

        PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                .addPlaceholders(pldBuilder.build())
                .build();

        builder.setPlaceholderGroup(placeholderGroup.toByteString());

        // search parameters
        builder.addSearchParams(
                KeyValuePair.newBuilder()
                        .setKey(Constant.VECTOR_FIELD)
                        .setValue(searchParam.getVectorFieldName())
                        .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(searchParam.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.METRIC_TYPE)
                                .setValue(searchParam.getMetricType())
                                .build());

        if (null != searchParam.getParams() && !searchParam.getParams().isEmpty()) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.PARAMS)
                            .setValue(searchParam.getParams())
                            .build());
        }

        if (!searchParam.getOutFields().isEmpty()) {
            searchParam.getOutFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (searchParam.getExpr() != null && !searchParam.getExpr().isEmpty()) {
            builder.setDsl(searchParam.getExpr());
        }

        SearchRequest searchRequest = builder.build();
        SearchResults search;
        try {
            search = this.blockingStub().search(searchRequest);
        } catch (Exception e) {
            logger.error("[milvus] search rpc request error:{}", e.getMessage());
            return R.failed(e);
        }

        return R.success(search);
    }

    @Override
    public R<QueryResults> query(QueryParam queryParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        QueryRequest queryRequest = QueryRequest.newBuilder()
                .setDbName(queryParam.getDbName())
                .setCollectionName(queryParam.getCollectionName())
                .addAllPartitionNames(queryParam.getPartitionNames())
                .addAllOutputFields(queryParam.getOutFields())
                .setExpr(queryParam.getExpr())
                .build();

        QueryResults query;
        try {
            query = this.blockingStub().query(queryRequest);
        } catch (Exception e) {
//            e.printStackTrace();
            logger.error("[milvus] query rpc request error:{}", e.getMessage());
            return R.failed(e);
        }
        return R.success(query);
    }

    @Override
    public R<CalcDistanceResults> calcDistance(CalcDistanceParam calcDistanceParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        List<List<Float>> vectors_left = calcDistanceParam.getVectorsLeft();
        List<List<Float>> vectors_right = calcDistanceParam.getVectorsRight();

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
                                .setValue(calcDistanceParam.getMetricType())
                                .build()
                )
                .build();
        CalcDistanceResults calcDistanceResults;
        try {
            calcDistanceResults = blockingStub().calcDistance(calcDistanceRequest);
        } catch (Exception e) {
            logger.error("[milvus] calDistance rpc request error:{}", e.getMessage());
            return R.failed(e);
        }
        return R.success(calcDistanceResults);
    }

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
                        List list = (List) object;
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
                ByteString byteString = null;
                int dim = 0;
                // each object is ByteBuffer
                for (Object object : objects) {
                    ByteBuffer buf = (ByteBuffer) object;
                    ByteString tempStr = ByteString.copyFrom((ByteBuffer) buf);
                    if (byteString == null){
                        byteString = tempStr;
                        dim = buf.position() * 8;
                    } else {
                        byteString.concat(tempStr);
                    }
                }

                VectorField vectorField = VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
                return builder.setFieldName(fieldName).setType(DataType.BinaryVector).setVectors(vectorField).build();
            }
        } else {
            switch (dataType) {
                case None:
                case UNRECOGNIZED:
                    throw new ParamException("Cannot support this dataType:" + dataType);
                case Int64:
                case Int32:
                case Int16:
                    List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                    LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                    ScalarField scalarField1 = ScalarField.newBuilder().setLongData(longArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField1).build();
                case Int8:
                    List<Integer> integers = objects.stream().map(p -> (Integer) p).collect(Collectors.toList());
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

    @Override
    public R<Boolean> hasCollection(HasCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        BoolResponse response;
        try {
            response = blockingStub().hasCollection(hasCollectionRequest);
            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Has collection check successfully!\n{}", requestParam.toString());
                Boolean value = Optional.of(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);
                return R.success(value);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logger.error("[milvus] hasCollection:{} request error: {}", requestParam.getCollectionName(), e.getMessage());
            return R.failed(e);
        }
    }


    @Override
    public R<RpcStatus> createCollection(CreateCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        // Check whether the parameters are correct or not
        if (requestParam == null) {
            return R.failed(new ParamException("Request param can not be null"));
        }

        // Construct CollectionSchema Params
        CollectionSchema.Builder collectionSchemaBuilder = CollectionSchema.newBuilder();
        collectionSchemaBuilder.setName(requestParam.getCollectionName())
                .setDescription(requestParam.getDescription());

        for (FieldType fieldType : requestParam.getFieldTypes()) {
            FieldSchema.Builder fieldSchemaBuilder = FieldSchema.newBuilder()
                    .setFieldID(fieldType.getFieldID())
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
        }

        // Construct CreateCollectionRequest
        CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setShardsNum(requestParam.getShardsNum())
                .setSchema(collectionSchemaBuilder.build().toByteString())
                .build();

        Status response;
        try {
            response = blockingStub().createCollection(createCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Created collection " + requestParam.getCollectionName() + " successfully!\n{}",
                        requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                logInfo("Created collection " + requestParam.getCollectionName() + " failed!\n{}", response);
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("createCollection " + requestParam.getCollectionName() + " RPC failed:\n{}",
                    e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropCollection(DropCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DropCollectionRequest dropCollectionRequest = DropCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        Status response;
        try {
            response = blockingStub().dropCollection(dropCollectionRequest);
            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Drop collection successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("dropCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }


    @Override
    public R<RpcStatus> loadCollection(LoadCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        Status response;
        try {
            response = blockingStub().loadCollection(loadCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Load collection successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("loadCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        Status response;
        try {
            response = blockingStub().releaseCollection(releaseCollectionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Release collection successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("releaseCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }

    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DescribeCollectionRequest describeCollectionRequest = DescribeCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        DescribeCollectionResponse response;
        try {
            response = blockingStub().describeCollection(describeCollectionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Describe collection successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("describeCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        GetCollectionStatisticsRequest getCollectionStatisticsRequest = GetCollectionStatisticsRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        GetCollectionStatisticsResponse response;
        try {
            response = blockingStub().getCollectionStatistics(getCollectionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get collection statistics successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("getCollectionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(ShowCollectionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        ShowCollectionsRequest.Builder showCollectionRequestBuilder = ShowCollectionsRequest.newBuilder();

        String[] collectionNames = requestParam.getCollectionNames();
        if (collectionNames == null || collectionNames.length <= 0) {
            return R.failed(R.Status.ParamError, "Collection name not specified");
        }

        // add collectionNames
        Arrays.stream(collectionNames).forEach(showCollectionRequestBuilder::addCollectionNames);

        ShowCollectionsRequest showCollectionsRequest = showCollectionRequestBuilder
                .setType(requestParam.getShowType()).build();

        ShowCollectionsResponse response;
        try {
            response = blockingStub().showCollections(showCollectionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Show collection successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("showCollectionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createPartition(CreatePartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        CreatePartitionRequest createPartitionRequest = CreatePartitionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setPartitionName(requestParam.getPartitionName())
                .build();

        Status response;
        try {
            response = blockingStub().createPartition(createPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Create partition successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("createPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropPartition(DropPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DropPartitionRequest dropPartitionRequest = DropPartitionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setPartitionName(requestParam.getPartitionName())
                .build();

        Status response;
        try {
            response = blockingStub().dropPartition(dropPartitionRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Drop partition successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("createPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasPartition(HasPartitionParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        HasPartitionRequest hasPartitionRequest = HasPartitionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setPartitionName(requestParam.getPartitionName())
                .build();

        BoolResponse response;
        try {
            response = blockingStub().hasPartition(hasPartitionRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("HasPartition call successfully!\n{}", requestParam.toString());

                Boolean result = Optional.ofNullable(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);

                return R.success(result);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("hasPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        LoadPartitionsRequest.Builder loadPartitionsRequestBuilder = LoadPartitionsRequest.newBuilder();

        String[] partitionNames = requestParam.getPartitionNames();
        // add partitionNames
        Arrays.stream(partitionNames).forEach(loadPartitionsRequestBuilder::addPartitionNames);

        LoadPartitionsRequest loadPartitionsRequest = loadPartitionsRequestBuilder
                .setCollectionName(requestParam.getCollectionName()).build();

        Status response;
        try {
            response = blockingStub().loadPartitions(loadPartitionsRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Load partition successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("loadPartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        ReleasePartitionsRequest.Builder releasePartitionsRequestBuilder = ReleasePartitionsRequest.newBuilder();

        String[] partitionNames = requestParam.getPartitionNames();
        // add partitionNames
        Arrays.stream(partitionNames).forEach(releasePartitionsRequestBuilder::addPartitionNames);

        ReleasePartitionsRequest releasePartitionsRequest = releasePartitionsRequestBuilder
                .setCollectionName(requestParam.getCollectionName()).build();

        Status response;
        try {
            response = blockingStub().releasePartitions(releasePartitionsRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Release partition successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("releasePartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        GetPartitionStatisticsRequest getPartitionStatisticsRequest = GetPartitionStatisticsRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setPartitionName(requestParam.getPartitionName())
                .build();

        GetPartitionStatisticsResponse response;
        try {
            response = blockingStub().getPartitionStatistics(getPartitionStatisticsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get partition statistics successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("getPartitionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(ShowPartitionsParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        ShowPartitionsRequest showPartitionsRequest = ShowPartitionsRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        ShowPartitionsResponse response;
        try {
            response = blockingStub().showPartitions(showPartitionsRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Show partition successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("showPartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createAlias(CreateAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        CreateAliasRequest createAliasRequest = CreateAliasRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setAlias(requestParam.getAlias())
                .build();

        Status response;
        try {
            response = blockingStub().createAlias(createAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Create alias successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("createAlias RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropAlias(DropAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DropAliasRequest dropAliasRequest = DropAliasRequest.newBuilder()
                .setAlias(requestParam.getAlias())
                .build();

        Status response;
        try {
            response = blockingStub().dropAlias(dropAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Drop alias successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("dropAlias RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> alterAlias(AlterAliasParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        AlterAliasRequest alterAliasRequest = AlterAliasRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setAlias(requestParam.getAlias())
                .build();

        Status response;
        try {
            response = blockingStub().alterAlias(alterAliasRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Alter alias successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("alterAlias RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createIndex(CreateIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        CreateIndexRequest.Builder createIndexRequestBuilder = CreateIndexRequest.newBuilder();
        List<KeyValuePair> extraParamList = assembleKvPair(requestParam.getExtraParam());

        if (CollectionUtils.isNotEmpty(extraParamList)) {
            extraParamList.forEach(createIndexRequestBuilder::addExtraParams);
        }

        CreateIndexRequest createIndexRequest = createIndexRequestBuilder.setCollectionName(requestParam.getCollectionName())
                .setFieldName(requestParam.getFieldName()).build();

        Status response;
        try {
            response = blockingStub().createIndex(createIndexRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Create index successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("createIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropIndex(DropIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DropIndexRequest dropIndexRequest = DropIndexRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setFieldName(requestParam.getFieldName())
                .build();

        Status response;
        try {
            response = blockingStub().dropIndex(dropIndexRequest);

            if (response.getErrorCode() == ErrorCode.Success) {
                logInfo("Drop index successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()), response.getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("dropIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        DescribeIndexRequest describeIndexRequest = DescribeIndexRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setFieldName(requestParam.getFieldName())
                .build();

        DescribeIndexResponse response;
        try {
            response = blockingStub().describeIndex(describeIndexRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Describe index successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("describeIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        GetIndexStateRequest getIndexStateRequest = GetIndexStateRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setFieldName(requestParam.getFieldName())
                .build();

        GetIndexStateResponse response;

        try {
            response = blockingStub().getIndexState(getIndexStateRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get index state successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("getIndexState RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam) {
        if (!clientIsReady()) {
            return R.failed(new ClientNotConnectedException("Client rpc channel is not ready"));
        }

        GetIndexBuildProgressRequest getIndexBuildProgressRequest = GetIndexBuildProgressRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        GetIndexBuildProgressResponse response;

        try {
            response = blockingStub().getIndexBuildProgress(getIndexBuildProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get index build progress successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()),
                        response.getStatus().getReason());
            }
        } catch (StatusRuntimeException e) {
            logError("getIndexBuildProgress RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

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

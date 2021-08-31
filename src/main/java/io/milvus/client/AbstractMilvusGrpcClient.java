package io.milvus.client;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import io.grpc.StatusRuntimeException;
import io.milvus.grpc.BoolResponse;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.CreateCollectionRequest;
import io.milvus.grpc.CreateIndexRequest;
import io.milvus.grpc.CreatePartitionRequest;
import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexRequest;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.DropCollectionRequest;
import io.milvus.grpc.DropIndexRequest;
import io.milvus.grpc.DropPartitionRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.GetCollectionStatisticsRequest;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.GetIndexBuildProgressRequest;
import io.milvus.grpc.GetIndexBuildProgressResponse;
import io.milvus.grpc.GetIndexStateRequest;
import io.milvus.grpc.GetIndexStateResponse;
import io.milvus.grpc.GetPartitionStatisticsRequest;
import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.HasCollectionRequest;
import io.milvus.grpc.HasPartitionRequest;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LoadCollectionRequest;
import io.milvus.grpc.LoadPartitionsRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.ReleaseCollectionRequest;
import io.milvus.grpc.ReleasePartitionsRequest;
import io.milvus.grpc.ShowCollectionsRequest;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowPartitionsRequest;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.grpc.Status;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.collection.ShowCollectionParam;
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
import io.milvus.param.partition.ShowPartitionParam;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractMilvusGrpcClient implements MilvusClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMilvusGrpcClient.class);

    protected abstract MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub();

    protected abstract MilvusServiceGrpc.MilvusServiceFutureStub futureStub();

    protected abstract boolean maybeAvailable();



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
        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Flush).build();
        FlushRequest.Builder builder = FlushRequest.newBuilder().setBase(msgBase).setDbName(dbName);
        collectionNames.forEach(builder::addCollectionNames);
        FlushRequest flushRequest = builder.build();
        FlushResponse flush = null;
        try {
            flush = blockingStub().flush(flushRequest);
        } catch (Exception e) {
            return R.failed(e);
        }
        return R.success(flush);
    }

    @Override
    public R<MutationResult> delete(DeleteParam deleteParam) {

        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setBase(MsgBase.newBuilder().setMsgType(MsgType.Delete).build())
                .setCollectionName(deleteParam.getCollectionName())
                .setPartitionName(deleteParam.getPartitionName())
                .build();

        MutationResult delete = null;
        try {
            delete = blockingStub().delete(deleteRequest);
        } catch (Exception e) {
            return R.failed(e);
        }
        return R.success(delete);
    }

    @Override
    public R<MutationResult> insert(InsertParam insertParam) {
        //check params : two steps
        // 1、check sdk incoming params
        String collectionName = insertParam.getCollectionName();
        String partitionName = insertParam.getPartitionName();
        int fieldNum = insertParam.getFieldNum();
        List<String> fieldNames = insertParam.getFieldNames();
        List<DataType> dataTypes = insertParam.getDataTypes();
        List<List<?>> fieldValues = insertParam.getFieldValues();
        Integer filedNameSize = Optional.ofNullable(fieldNames).map(List::size).orElse(0);
        Integer dTypeSize = Optional.ofNullable(dataTypes).map(List::size).orElse(0);
        int fieldValueSize = Optional.ofNullable(fieldValues).map(List::size).orElse(0);
        if (fieldNum != fieldValueSize || fieldNum != filedNameSize || fieldNum != dTypeSize) {
            throw new ParamException("size is illegal");
        }

        //2、need to DDL request to get collection meta schema and check vector dim;
        // whether the collection schema needs to be cached locally todo
        assert fieldValues != null;
        int numRow = fieldValues.get(0).size();

        //3、gen insert request
        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
        InsertRequest.Builder insertBuilder = InsertRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName)
                .setBase(msgBase)
                .setNumRows(numRow);

        // gen fieldData
        for (int i = 0; i < fieldNum; i++) {
            insertBuilder.addFieldsData(genFieldData(fieldNames.get(i), dataTypes.get(i), fieldValues.get(i)));
        }

        InsertRequest insertRequest = insertBuilder.build();
        MutationResult insert = null;
        try {
            insert = blockingStub().insert(insertRequest);
        } catch (Exception e) {
            R.failed(e);
        }
        return R.success(insert);

    }

    @Override
    public R<SearchResults> search(SearchParam searchParam) {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setDbName(searchParam.getDbName())
                .setCollectionName(searchParam.getCollectionName());
        if (!searchParam.getPartitionNames().isEmpty()) {
            searchParam.getPartitionNames().forEach(builder::addPartitionNames);
        }

        List<List<Float>> vectors = searchParam.getVectors();
        List<ByteString> byteStrings = vectors.stream().map(vector -> {
            ByteBuffer allocate1 = ByteBuffer.allocate(16);
            vector.forEach(allocate1::putFloat);
            byte[] array = allocate1.array();
            return ByteString.copyFrom(array);
        }).collect(Collectors.toList());


        PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                .setTag("$0")
                .setType(PlaceholderType.FloatVector);
        byteStrings.forEach(pldBuilder::addValues);

        PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                .addPlaceholders(pldBuilder.build())
                .build();

        builder.setPlaceholderGroup(placeholderGroup.toByteString());

        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey("anns_field")
                                .setValue(searchParam.getVectorFieldName())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey("topk")
                                .setValue(String.valueOf(searchParam.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey("metric_type")
                                .setValue(searchParam.getMetricType().name())
                                .build());

        if (!searchParam.getParams().isEmpty() && null != searchParam.getParams().get("params")
                && !searchParam.getParams().get("params").isEmpty()) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey("params")
                            .setValue(searchParam.getParams().get("params"))
                            .build());
        }

        if (!searchParam.getOutFields().isEmpty()) {
            searchParam.getOutFields().forEach(builder::addOutputFields);
        }

        builder.setDslType(searchParam.getDslType());
        if (searchParam.getDsl() == null || searchParam.getDsl().isEmpty()) {
            builder.setDsl(searchParam.getDsl());
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

    private FieldData genFieldData(String fieldName, DataType dataType, List<?> objects) {
        if (objects == null) {
            throw new ParamException("params is null");
        }
        FieldData.Builder builder = FieldData.newBuilder();
        if (vectorDataType.contains(dataType)) {
            if (dataType == DataType.FloatVector) {
                List<Float> floats = new ArrayList<>();
                // 每个object是个list
                for (Object object : objects) {
                    if (object instanceof List) {
                        List list = (List) object;
                        list.forEach(o -> floats.add((Float) o));
                    } else {
                        throw new ParamException("参数有问题");
                    }
                }
                int dim = floats.size() / objects.size();
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
                return builder.setFieldName(fieldName).setType(DataType.FloatVector).setVectors(vectorField).build();
            } else if (dataType == DataType.BinaryVector) {
                List<ByteBuffer> bytes = objects.stream().map(p -> (ByteBuffer) p).collect(Collectors.toList());
                ;
                ByteString byteString = ByteString.copyFrom((ByteBuffer) bytes);
                int dim = objects.size();
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
                return builder.setFieldName(fieldName).setType(DataType.BinaryVector).setVectors(vectorField).build();
            }


        } else {
            switch (dataType) {
                case None:
                case UNRECOGNIZED:
                    throw new ParamException("not support this dataType:" + dataType);
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
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
        }

        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .build();

        BoolResponse response;
        try {
            response = blockingStub().hasCollection(hasCollectionRequest);
            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Has collection check successfully!\n{}", requestParam.toString());
                Boolean aBoolean = Optional.ofNullable(response)
                        .map(BoolResponse::getValue)
                        .orElse(false);
                return R.success(aBoolean);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }

        } catch (StatusRuntimeException e) {
            logger.error("[milvus] hasCollection:{} request error: {}", requestParam.getCollectionName(), e.getMessage());
            return R.failed(e);
        }

    }


    @Override
    public R<RpcStatus> createCollection(CreateCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // Check whether the parameters are correct or not
        if (requestParam == null || StringUtils.isBlank(requestParam.getCollectionName())
                || requestParam.getShardsNum() <= 0
                || requestParam.getFieldTypes().length == 0) {
            return R.failed(R.Status.ParamError);
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

            // assemble indexParams for CollectionSchema
            List<KeyValuePair> indexParamsList = assembleKvPair(fieldType.getIndexParams());
            if (CollectionUtils.isNotEmpty(indexParamsList)) {
                indexParamsList.forEach(fieldSchemaBuilder::addIndexParams);
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
                logInfo("Created collection successfully!\n{}", requestParam.toString());
                return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
            } else {
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("createCollection RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropCollection(DropCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("dropCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }


    @Override
    public R<RpcStatus> loadCollection(LoadCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("loadCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releaseCollection(ReleaseCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("releaseCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }

    }

    @Override
    public R<DescribeCollectionResponse> describeCollection(DescribeCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("describeCollectionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetCollectionStatisticsResponse> getCollectionStatistics(GetCollectionStatisticsParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name is empty or not
        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getCollectionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<ShowCollectionsResponse> showCollections(ShowCollectionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }
        ShowCollectionsRequest.Builder showCollectionRequestBuilder = ShowCollectionsRequest.newBuilder();

        String[] collectionNames = requestParam.getCollectionNames();
        if (collectionNames == null || collectionNames.length <= 0) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("showCollectionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createPartition(CreatePartitionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name or partition name is empty or not
        if (checkPartitionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("createPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropPartition(DropPartitionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name or partition name is empty or not
        if (checkPartitionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("createPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<Boolean> hasPartition(HasPartitionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name or partition name is empty or not
        if (checkPartitionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("hasPartitionRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> loadPartitions(LoadPartitionsParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check requestParam
        if (requestParam == null || StringUtils.isEmpty(requestParam.getCollectionName())
                || requestParam.getPartitionNames() == null
                || requestParam.getPartitionNames().length == 0) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("loadPartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> releasePartitions(ReleasePartitionsParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check requestParam
        if (requestParam == null || StringUtils.isEmpty(requestParam.getCollectionName())
                || requestParam.getPartitionNames() == null
                || requestParam.getPartitionNames().length == 0) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("releasePartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetPartitionStatisticsResponse> getPartitionStatistics(GetPartitionStatisticsParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if collection name or partition name is empty or not
        if (checkPartitionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getPartitionStatisticsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<ShowPartitionsResponse> showPartitions(ShowPartitionParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        if (checkCollectionParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("showPartitionsRequest RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> createIndex(CreateIndexParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // check whether if create index param is valid or not
        if (requestParam == null || StringUtils.isEmpty(requestParam.getCollectionName())
                || StringUtils.isEmpty(requestParam.getFieldName())
                || MapUtils.isEmpty(requestParam.getExtraParam())) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("createIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<RpcStatus> dropIndex(DropIndexParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        if (checkIndexParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("dropIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<DescribeIndexResponse> describeIndex(DescribeIndexParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        if (checkIndexParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("describeIndex RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetIndexStateResponse> getIndexState(GetIndexStateParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        if (checkIndexParam(requestParam)) {
            return R.failed(R.Status.ParamError);
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
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getIndexState RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    @Override
    public R<GetIndexBuildProgressResponse> getIndexBuildProgress(GetIndexBuildProgressParam requestParam) {
        if (checkServerConnection()) {
            return R.failed(R.Status.ConnectFailed);
        }

        // indexName is required in this rpc
        if (checkIndexParam(requestParam) || StringUtils.isEmpty(requestParam.getIndexName())) {
            return R.failed(R.Status.ParamError);
        }

        GetIndexBuildProgressRequest getIndexBuildProgressRequest = GetIndexBuildProgressRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .setFieldName(requestParam.getFieldName())
                .setIndexName(requestParam.getIndexName())
                .build();

        GetIndexBuildProgressResponse response;

        try {
            response = blockingStub().getIndexBuildProgress(getIndexBuildProgressRequest);

            if (response.getStatus().getErrorCode() == ErrorCode.Success) {
                logInfo("Get index build progress successfully!\n{}", requestParam.toString());
                return R.success(response);
            } else {
                return R.failed(R.Status.valueOf(response.getStatus().getErrorCode().getNumber()));
            }
        } catch (StatusRuntimeException e) {
            logError("getIndexBuildProgress RPC failed:\n{}", e.getStatus().toString());
            return R.failed(e);
        }
    }

    private <T> boolean checkCollectionParam(T requestParam) {
        Class<?> clazz = requestParam.getClass();
        try {
            Field collectionNameField = clazz.getDeclaredField("collectionName");
            collectionNameField.setAccessible(true);
            String collectionName = (String) collectionNameField.get(requestParam);
            // if collectionName is empty, return true, else return false
            return StringUtils.isEmpty(collectionName);
        } catch (Exception e) {
            logError("checkCollectionParam failed:\n{}", e.getMessage());
            return true;
        }
    }

    private <T> boolean checkPartitionParam(T requestParam) {
        Class<?> clazz = requestParam.getClass();
        try {
            Field collectionNameField = clazz.getDeclaredField("collectionName");
            collectionNameField.setAccessible(true);
            String collectionName = (String) collectionNameField.get(requestParam);

            Field partitionNameField = clazz.getDeclaredField("partitionName");
            partitionNameField.setAccessible(true);
            String partitionName = (String) partitionNameField.get(requestParam);

            // if collectionName or partitionName is empty, return true, else return false
            return (StringUtils.isEmpty(collectionName) || StringUtils.isEmpty(partitionName));
        } catch (Exception e) {
            // if param check failed, return true
            return true;
        }
    }

    private <T> boolean checkIndexParam(T requestParam) {
        Class<?> clazz = requestParam.getClass();
        try {
            Field collectionNameField = clazz.getDeclaredField("collectionName");
            collectionNameField.setAccessible(true);
            String collectionName = (String) collectionNameField.get(requestParam);

            Field fieldNameField = clazz.getDeclaredField("fieldName");
            fieldNameField.setAccessible(true);
            String fieldName = (String) fieldNameField.get(requestParam);
            // if collectionName or fieldName is empty, return true, else return false
            return (StringUtils.isEmpty(collectionName) || StringUtils.isEmpty(fieldName));
        } catch (Exception e) {
            // if param check failed, return true
            return true;
        }
    }

    private boolean checkServerConnection() {
        if (!maybeAvailable()) {
            logWarning("You are not connected to Milvus server");
            return true;
        }
        return false;
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

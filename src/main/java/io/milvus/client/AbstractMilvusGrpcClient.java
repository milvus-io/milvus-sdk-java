package io.milvus.client;

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

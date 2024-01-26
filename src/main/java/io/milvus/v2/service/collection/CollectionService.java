package io.milvus.v2.service.collection;

import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.utils.SchemaUtils;

public class CollectionService extends BaseService {
    public IndexService indexService = new IndexService();

    public R<RpcStatus> createCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());
        FieldSchema vectorSchema = FieldSchema.newBuilder()
                .setName(request.getVectorFieldName())
                .setDataType(DataType.FloatVector)
                .setIsPrimaryKey(Boolean.FALSE)
                .addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(request.getDimension())).build())
                .build();

        FieldSchema idSchema = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.valueOf(request.getPrimaryFieldType()))
                .setIsPrimaryKey(Boolean.TRUE)
                .setAutoID(request.getAutoID())
                .build();
        if(request.getPrimaryFieldType().equals("VarChar") && request.getMaxLength() != null){
            idSchema = idSchema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(request.getMaxLength())).build()).build();
        }

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .addFields(vectorSchema)
                .addFields(idSchema)
                .setEnableDynamicField(Boolean.TRUE)
                .build();


        CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(schema.toByteString())
                .build();

        Status status = blockingStub.createCollection(createCollectionRequest);
        rpcUtils.handleResponse(title, status);

        //create index
        IndexParam indexParam = IndexParam.builder()
                        .metricType(IndexParam.MetricType.valueOf(request.getMetricType()))
                        .fieldName("vector")
                        .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParam(indexParam)
                        .collectionName(request.getCollectionName())
                        .build();
        indexService.createIndex(blockingStub, createIndexReq);
        //load collection
        loadCollection(blockingStub, LoadCollectionReq.builder().collectionName(request.getCollectionName()).build());
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> createCollectionWithSchema(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionWithSchemaReq request) {
        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());

        //convert CollectionSchema to io.milvus.grpc.CollectionSchema
        CollectionSchema grpcSchema = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .setDescription(request.getCollectionSchema().getDescription())
                .setEnableDynamicField(request.getCollectionSchema().getEnableDynamicField())
                .build();
        for (CreateCollectionWithSchemaReq.FieldSchema fieldSchema : request.getCollectionSchema().getFieldSchemaList()) {
            grpcSchema = grpcSchema.toBuilder().addFields(SchemaUtils.convertToGrpcFieldSchema(fieldSchema)).build();
        }

        //create collection
        CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(grpcSchema.toByteString())
                .build();

        Status createCollectionResponse = blockingStub.createCollection(createCollectionRequest);
        rpcUtils.handleResponse(title, createCollectionResponse);

        //create index
        if(request.getIndexParams() != null && !request.getIndexParams().isEmpty()) {
            for(IndexParam indexParam : request.getIndexParams()) {
                CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParam(indexParam)
                        .collectionName(request.getCollectionName())
                        .build();
                indexService.createIndex(blockingStub, createIndexReq);
            }
        }

        //load collection
        loadCollection(blockingStub, LoadCollectionReq.builder().collectionName(request.getCollectionName()).build());

        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<ListCollectionsResp> listCollections(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub) {
        ShowCollectionsRequest showCollectionsRequest = ShowCollectionsRequest.newBuilder()
                .build();
        ShowCollectionsResponse response = milvusServiceBlockingStub.showCollections(showCollectionsRequest);
        ListCollectionsResp a = ListCollectionsResp.builder()
                .collectionNames(response.getCollectionNamesList())
                .build();

        return R.success(a);
    }

    public R<RpcStatus> dropCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DropCollectionReq request) {

        String title = String.format("DropCollectionRequest collectionName:%s", request.getCollectionName());
        DropCollectionRequest dropCollectionRequest = DropCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.dropCollection(dropCollectionRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<Boolean> hasCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, HasCollectionReq request) {
        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        BoolResponse response = milvusServiceBlockingStub.hasCollection(hasCollectionRequest);
        return R.success(response.getValue());
    }

    public R<DescribeCollectionResp> describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DescribeCollectionReq request) {
        //check collection exists
        checkCollectionExist(milvusServiceBlockingStub, request.getCollectionName());

        DescribeCollectionRequest describeCollectionRequest = DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        DescribeCollectionResponse response = milvusServiceBlockingStub.describeCollection(describeCollectionRequest);

        DescribeCollectionResp describeCollectionResp = DescribeCollectionResp.builder()
                .collectionName(response.getCollectionName())
                .description(response.getSchema().getDescription())
                .numOfPartitions(response.getNumPartitions())
                .collectionSchema(SchemaUtils.convertFromGrpcCollectionSchema(response.getSchema()))
                .autoID(response.getSchema().getFieldsList().stream().anyMatch(FieldSchema::getAutoID))
                .enableDynamicField(response.getSchema().getEnableDynamicField())
                .fieldNames(response.getSchema().getFieldsList().stream().map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()))
                .vectorFieldName(response.getSchema().getFieldsList().stream().filter(fieldSchema -> fieldSchema.getDataType() == DataType.FloatVector || fieldSchema.getDataType() == DataType.BinaryVector).map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()))
                .primaryFieldName(response.getSchema().getFieldsList().stream().filter(FieldSchema::getIsPrimaryKey).map(FieldSchema::getName).collect(java.util.stream.Collectors.toList()).get(0))
                .createTime(response.getCreatedTimestamp())
                .build();

        return R.success(describeCollectionResp);
    }

    public R<RpcStatus> renameCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, RenameCollectionReq request) {
        String title = String.format("RenameCollectionRequest collectionName:%s", request.getCollectionName());
        RenameCollectionRequest renameCollectionRequest = RenameCollectionRequest.newBuilder()
                .setOldName(request.getCollectionName())
                .setNewName(request.getNewCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.renameCollection(renameCollectionRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> loadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, LoadCollectionReq request) {
        String title = String.format("LoadCollectionRequest collectionName:%s", request.getCollectionName());
        LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.loadCollection(loadCollectionRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<RpcStatus> releaseCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, ReleaseCollectionReq request) {
        String title = String.format("ReleaseCollectionRequest collectionName:%s", request.getCollectionName());
        ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.releaseCollection(releaseCollectionRequest);
        rpcUtils.handleResponse(title, status);
        return R.success(new RpcStatus(RpcStatus.SUCCESS_MSG));
    }

    public R<Boolean> getLoadState(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, GetLoadStateReq request) {
        // getLoadState
        String title = String.format("GetLoadStateRequest collectionName:%s", request.getCollectionName());
        GetLoadStateRequest getLoadStateRequest = GetLoadStateRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        GetLoadStateResponse response = milvusServiceBlockingStub.getLoadState(getLoadStateRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        return R.success(response.getState() == LoadState.LoadStateLoaded);
    }

    public R<GetCollectionStatsResp> getCollectionStats(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetCollectionStatsReq request) {
        String title = String.format("GetCollectionStatisticsRequest collectionName:%s", request.getCollectionName());
        GetCollectionStatisticsRequest getCollectionStatisticsRequest = GetCollectionStatisticsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        GetCollectionStatisticsResponse response = blockingStub.getCollectionStatistics(getCollectionStatisticsRequest);

        rpcUtils.handleResponse(title, response.getStatus());
        GetCollectionStatsResp getCollectionStatsResp = GetCollectionStatsResp.builder()
                .numOfEntities(response.getStatsList().stream().filter(stat -> stat.getKey().equals("row_count")).map(stat -> Long.parseLong(stat.getValue())).findFirst().get())
                .build();
        return R.success(getCollectionStatsResp);
    }
}

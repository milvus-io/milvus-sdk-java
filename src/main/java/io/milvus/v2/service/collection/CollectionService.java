package io.milvus.v2.service.collection;

import io.milvus.grpc.*;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.utils.SchemaUtils;

import java.util.Collections;

public class CollectionService extends BaseService {
    public IndexService indexService = new IndexService();

    public void createCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        if (request.getCollectionSchema() != null) {
            //create collections with schema
            createCollectionWithSchema(blockingStub, request);
            return;
        }
        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());
        FieldSchema vectorSchema = FieldSchema.newBuilder()
                .setName(request.getVectorFieldName())
                .setDataType(DataType.FloatVector)
                .setIsPrimaryKey(Boolean.FALSE)
                .addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(request.getDimension())).build())
                .build();

        FieldSchema idSchema = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.valueOf(request.getPrimaryFieldType().name()))
                .setIsPrimaryKey(Boolean.TRUE)
                .setAutoID(request.getAutoID())
                .build();
        if(request.getPrimaryFieldType().name().equals("VarChar") && request.getMaxLength() != null){
            idSchema = idSchema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(request.getMaxLength())).build()).build();
        }

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .addFields(vectorSchema)
                .addFields(idSchema)
                .setEnableDynamicField(request.getEnableDynamicField())
                .build();


        CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(schema.toByteString())
                .setShardsNum(request.getNumShards())
                .build();

        Status status = blockingStub.createCollection(createCollectionRequest);
        rpcUtils.handleResponse(title, status);

        //create index
        IndexParam indexParam = IndexParam.builder()
                        .metricType(IndexParam.MetricType.valueOf(request.getMetricType()))
                        .fieldName("vector")
                        .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParams(Collections.singletonList(indexParam))
                        .collectionName(request.getCollectionName())
                        .build();
        indexService.createIndex(blockingStub, createIndexReq);
        //load collection
        loadCollection(blockingStub, LoadCollectionReq.builder().collectionName(request.getCollectionName()).build());
    }

    public void createCollectionWithSchema(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());
        if (request.getEnableDynamicField() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Please specify enableDynamicField in collection schema when creating collection with schema");
        }

        //convert CollectionSchema to io.milvus.grpc.CollectionSchema
        CollectionSchema grpcSchema = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .setDescription(request.getCollectionSchema().getDescription())
                .setEnableDynamicField(request.getCollectionSchema().getEnableDynamicField())
                .build();
        for (CreateCollectionReq.FieldSchema fieldSchema : request.getCollectionSchema().getFieldSchemaList()) {
            grpcSchema = grpcSchema.toBuilder().addFields(SchemaUtils.convertToGrpcFieldSchema(request.getPartitionKeyField(), fieldSchema)).build();
        }

        //create collection
        CreateCollectionRequest createCollectionRequest = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(grpcSchema.toByteString())
                .setNumPartitions(request.getNumPartitions())
                .setShardsNum(request.getNumShards())
                .build();

        Status createCollectionResponse = blockingStub.createCollection(createCollectionRequest);
        rpcUtils.handleResponse(title, createCollectionResponse);

        //create index
        if(request.getIndexParams() != null && !request.getIndexParams().isEmpty()) {
            for(IndexParam indexParam : request.getIndexParams()) {
                CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParams(Collections.singletonList(indexParam))
                        .collectionName(request.getCollectionName())
                        .build();
                indexService.createIndex(blockingStub, createIndexReq);
            }
            //load collection
            loadCollection(blockingStub, LoadCollectionReq.builder().collectionName(request.getCollectionName()).build());
        }
    }

    public ListCollectionsResp listCollections(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub) {
        ShowCollectionsRequest showCollectionsRequest = ShowCollectionsRequest.newBuilder()
                .build();
        ShowCollectionsResponse response = milvusServiceBlockingStub.showCollections(showCollectionsRequest);
        ListCollectionsResp listCollectionsResp = ListCollectionsResp.builder()
                .collectionNames(response.getCollectionNamesList())
                .build();

        return listCollectionsResp;
    }

    public void dropCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DropCollectionReq request) {

        String title = String.format("DropCollectionRequest collectionName:%s", request.getCollectionName());
        DropCollectionRequest dropCollectionRequest = DropCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.dropCollection(dropCollectionRequest);
        rpcUtils.handleResponse(title, status);

        if (request.getAsync()) {
            WaitForDropCollection(milvusServiceBlockingStub, request);
        }
    }

    public Boolean hasCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, HasCollectionReq request) {
        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        BoolResponse response = milvusServiceBlockingStub.hasCollection(hasCollectionRequest);
        rpcUtils.handleResponse("HasCollectionRequest", response.getStatus());
        return response.getValue();
    }

    public DescribeCollectionResp describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DescribeCollectionReq request) {
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

        return describeCollectionResp;
    }

    public void renameCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, RenameCollectionReq request) {
        String title = String.format("RenameCollectionRequest collectionName:%s", request.getCollectionName());
        RenameCollectionRequest renameCollectionRequest = RenameCollectionRequest.newBuilder()
                .setOldName(request.getCollectionName())
                .setNewName(request.getNewCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.renameCollection(renameCollectionRequest);
        rpcUtils.handleResponse(title, status);
    }

    public void loadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, LoadCollectionReq request) {
        String title = String.format("LoadCollectionRequest collectionName:%s", request.getCollectionName());
        LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setReplicaNumber(request.getNumReplicas())
                .build();
        Status status = milvusServiceBlockingStub.loadCollection(loadCollectionRequest);
        rpcUtils.handleResponse(title, status);
        if (request.getAsync()) {
            WaitForLoadCollection(milvusServiceBlockingStub, request);
        }
    }

    public void releaseCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, ReleaseCollectionReq request) {
        String title = String.format("ReleaseCollectionRequest collectionName:%s", request.getCollectionName());
        ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = milvusServiceBlockingStub.releaseCollection(releaseCollectionRequest);
        rpcUtils.handleResponse(title, status);
        if (request.getAsync()) {
            waitForCollectionRelease(milvusServiceBlockingStub, request);
        }
    }

    public Boolean getLoadState(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, GetLoadStateReq request) {
        // getLoadState
        String title = String.format("GetLoadStateRequest collectionName:%s", request.getCollectionName());
        GetLoadStateRequest getLoadStateRequest = GetLoadStateRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        if(request.getPartitionName() != null) {
            getLoadStateRequest = getLoadStateRequest.toBuilder().addPartitionNames(request.getPartitionName()).build();
        }
        GetLoadStateResponse response = milvusServiceBlockingStub.getLoadState(getLoadStateRequest);
        rpcUtils.handleResponse(title, response.getStatus());
        return response.getState() == LoadState.LoadStateLoaded;
    }

    public GetCollectionStatsResp getCollectionStats(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetCollectionStatsReq request) {
        String title = String.format("GetCollectionStatisticsRequest collectionName:%s", request.getCollectionName());
        GetCollectionStatisticsRequest getCollectionStatisticsRequest = GetCollectionStatisticsRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        GetCollectionStatisticsResponse response = blockingStub.getCollectionStatistics(getCollectionStatisticsRequest);

        rpcUtils.handleResponse(title, response.getStatus());
        GetCollectionStatsResp getCollectionStatsResp = GetCollectionStatsResp.builder()
                .numOfEntities(response.getStatsList().stream().filter(stat -> stat.getKey().equals("row_count")).map(stat -> Long.parseLong(stat.getValue())).findFirst().get())
                .build();
        return getCollectionStatsResp;
    }

    public CreateCollectionReq.CollectionSchema createSchema(Boolean enableDynamicField, String description) {
        return CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(enableDynamicField)
                .description(description)
                .build();
    }

    public void waitForCollectionRelease(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, ReleaseCollectionReq request) {
        boolean isLoaded = true;
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (isLoaded) {
            // Call the getLoadState method
            isLoaded = getLoadState(milvusServiceBlockingStub, GetLoadStateReq.builder().collectionName(request.getCollectionName()).build());
            if (isLoaded) {
                // Check if timeout is exceeded
                if (System.currentTimeMillis() - startTime > request.getTimeout()) {
                    throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load collection timeout");
                }
                // Wait for a certain period before checking again
                try {
                    Thread.sleep(500); // Sleep for 0.5 second. Adjust this value as needed.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Thread was interrupted, Failed to complete operation");
                    return; // or handle interruption appropriately
                }
            }
        }
    }

    private void WaitForLoadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, LoadCollectionReq request) {
        boolean isLoaded = false;
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (!isLoaded) {
            // Call the getLoadState method
            isLoaded = getLoadState(milvusServiceBlockingStub, GetLoadStateReq.builder().collectionName(request.getCollectionName()).build());
            if (!isLoaded) {
                // Check if timeout is exceeded
                if (System.currentTimeMillis() - startTime > request.getTimeout()) {
                    throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load collection timeout");
                }
                // Wait for a certain period before checking again
                try {
                    Thread.sleep(500); // Sleep for 0.5 second. Adjust this value as needed.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Thread was interrupted, Failed to complete operation");
                    return; // or handle interruption appropriately
                }
            }
        }
    }

    private void WaitForDropCollection(MilvusServiceGrpc.MilvusServiceBlockingStub milvusServiceBlockingStub, DropCollectionReq request) {
        boolean hasCollection = true;
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (hasCollection) {
            // Call the getLoadState method
            hasCollection = hasCollection(milvusServiceBlockingStub, HasCollectionReq.builder().collectionName(request.getCollectionName()).build());
            if (hasCollection) {
                // Check if timeout is exceeded
                if (System.currentTimeMillis() - startTime > request.getTimeout()) {
                    throw new MilvusClientException(ErrorCode.SERVER_ERROR, "drop collection timeout");
                }
                // Wait for a certain period before checking again
                try {
                    Thread.sleep(500); // Sleep for 0.5 second. Adjust this value as needed.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Thread was interrupted, Failed to complete operation");
                    return; // or handle interruption appropriately
                }
            }
        }
    }
}

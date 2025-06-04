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

package io.milvus.v2.service.collection;

import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.*;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.utils.SchemaUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectionService extends BaseService {
    public IndexService indexService = new IndexService();

    public Void createCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        if (request.getCollectionSchema() != null) {
            //create collections with schema
            createCollectionWithSchema(blockingStub, request);
            return null;
        }

        if (request.getDimension() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Dimension is undefined.");
        }

        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());
        FieldSchema vectorSchema = FieldSchema.newBuilder()
                .setName(request.getVectorFieldName())
                .setDataType(DataType.FloatVector)
                .setIsPrimaryKey(Boolean.FALSE)
                .addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(request.getDimension())).build())
                .build();

        FieldSchema idSchema = FieldSchema.newBuilder()
                .setName(request.getPrimaryFieldName())
                .setDataType(DataType.valueOf(request.getIdType().name()))
                .setIsPrimaryKey(Boolean.TRUE)
                .setAutoID(request.getAutoID())
                .build();
        if (request.getIdType().name().equals("VarChar") && request.getMaxLength() != null) {
            idSchema = idSchema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(request.getMaxLength())).build()).build();
        }

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .setDescription(request.getDescription())
                .addFields(vectorSchema)
                .addFields(idSchema)
                .setEnableDynamicField(request.getEnableDynamicField())
                .build();

        CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(schema.toByteString())
                .setShardsNum(request.getNumShards())
                .setConsistencyLevelValue(request.getConsistencyLevel().getCode());

        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status status = blockingStub.createCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        //create index
        IndexParam indexParam = IndexParam.builder()
                        .metricType(IndexParam.MetricType.valueOf(request.getMetricType()))
                        .fieldName(request.getVectorFieldName())
                        .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParams(Collections.singletonList(indexParam))
                        .collectionName(request.getCollectionName())
                        .sync(false)
                        .build();
        indexService.createIndex(blockingStub, createIndexReq);
        //load collection, set async to true since no need to wait loading progress
        try {
            //TimeUnit.MILLISECONDS.sleep(1000);
            loadCollection(blockingStub, LoadCollectionReq.builder()
                    .sync(false)
                    .collectionName(request.getCollectionName())
                    .build());
        } catch (Exception e) {
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load collection failed: " + e);
        }
        return null;
    }

    public Void createCollectionWithSchema(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        String title = String.format("CreateCollectionRequest collectionName:%s", request.getCollectionName());

        //convert CollectionSchema to io.milvus.grpc.CollectionSchema
        CollectionSchema.Builder grpcSchemaBuilder = CollectionSchema.newBuilder()
                .setName(request.getCollectionName())
                .setDescription(request.getDescription())
                .setEnableDynamicField(request.getCollectionSchema().isEnableDynamicField());
        List<String> outputFields = new ArrayList<>();
        for (CreateCollectionReq.Function function : request.getCollectionSchema().getFunctionList()) {
            grpcSchemaBuilder.addFunctions(SchemaUtils.convertToGrpcFunction(function)).build();
            outputFields.addAll(function.getOutputFieldNames());
        }
        for (CreateCollectionReq.FieldSchema fieldSchema : request.getCollectionSchema().getFieldSchemaList()) {
            FieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema);
            if (outputFields.contains(fieldSchema.getName())) {
                grpcFieldSchema = grpcFieldSchema.toBuilder().setIsFunctionOutput(true).build();
            }
            grpcSchemaBuilder.addFields(grpcFieldSchema);
        }

        //create collection
        CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(grpcSchemaBuilder.build().toByteString())
                .setShardsNum(request.getNumShards())
                .setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }
        if (request.getNumPartitions() != null) {
            builder.setNumPartitions(request.getNumPartitions());
        }
        Status createCollectionResponse = blockingStub.createCollection(builder.build());
        rpcUtils.handleResponse(title, createCollectionResponse);

        //create index
        if(request.getIndexParams() != null && !request.getIndexParams().isEmpty()) {
            for(IndexParam indexParam : request.getIndexParams()) {
                CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .indexParams(Collections.singletonList(indexParam))
                        .collectionName(request.getCollectionName())
                        .sync(false)
                        .build();
                indexService.createIndex(blockingStub, createIndexReq);
            }
            //load collection, set async to true since no need to wait loading progress
            loadCollection(blockingStub, LoadCollectionReq.builder()
                    .sync(false)
                    .collectionName(request.getCollectionName())
                    .build());
        }

        return null;
    }

    public ListCollectionsResp listCollections(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
        ShowCollectionsRequest showCollectionsRequest = ShowCollectionsRequest.newBuilder()
                .build();
        ShowCollectionsResponse response = blockingStub.showCollections(showCollectionsRequest);
        ListCollectionsResp listCollectionsResp = ListCollectionsResp.builder()
                .collectionNames(response.getCollectionNamesList())
                .build();

        return listCollectionsResp;
    }

    public Void dropCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionReq request) {

        String title = String.format("DropCollectionRequest collectionName:%s", request.getCollectionName());
        DropCollectionRequest dropCollectionRequest = DropCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = blockingStub.dropCollection(dropCollectionRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void alterCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterCollectionPropertiesReq request) {
        String title = String.format("AlterCollectionPropertiesReq collectionName:%s", request.getCollectionName());
        AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterCollection(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void alterCollectionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterCollectionFieldReq request) {
        String title = String.format("AlterCollectionFieldReq collectionName:%s", request.getCollectionName());
        AlterCollectionFieldRequest.Builder builder = AlterCollectionFieldRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName());
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterCollectionField(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionPropertiesReq request) {
        String title = String.format("DropCollectionPropertiesReq collectionName:%s", request.getCollectionName());
        AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterCollection(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropCollectionFieldProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionFieldPropertiesReq request) {
        String title = String.format("DropCollectionFieldPropertiesReq collectionName:%s fieldName:%s",
                request.getCollectionName(), request.getFieldName());

        AlterCollectionFieldRequest.Builder builder = AlterCollectionFieldRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName())
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        Status response = blockingStub.alterCollectionField(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Boolean hasCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HasCollectionReq request) {
        HasCollectionRequest hasCollectionRequest = HasCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        BoolResponse response = blockingStub.hasCollection(hasCollectionRequest);
        rpcUtils.handleResponse("HasCollectionRequest", response.getStatus());
        return response.getValue();
    }

    public DescribeCollectionResp describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeCollectionReq request) {
        String title = String.format("DescribeCollectionRequest collectionName:%s, databaseName:%s", request.getCollectionName(), request.getDatabaseName());
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        DescribeCollectionResponse response = blockingStub.describeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return convertUtils.convertDescCollectionResp(response);
    }

    public Void renameCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RenameCollectionReq request) {
        String title = String.format("RenameCollectionRequest collectionName:%s", request.getCollectionName());
        RenameCollectionRequest renameCollectionRequest = RenameCollectionRequest.newBuilder()
                .setOldName(request.getCollectionName())
                .setNewName(request.getNewCollectionName())
                .build();
        Status status = blockingStub.renameCollection(renameCollectionRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void loadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadCollectionReq request) {
        String title = String.format("LoadCollectionRequest collectionName:%s", request.getCollectionName());
        LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(request.getRefresh())
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(request.getSkipLoadDynamicField())
                .addAllResourceGroups(request.getResourceGroups())
                .build();
        Status status = blockingStub.loadCollection(loadCollectionRequest);
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadCollection(blockingStub, request.getCollectionName(), request.getTimeout());
        }

        return null;
    }

    public Void refreshLoad(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RefreshLoadReq request) {
        String title = String.format("RefreshLoadRequest collectionName:%s", request.getCollectionName());
        LoadCollectionRequest loadCollectionRequest = LoadCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setRefresh(true)
                .build();
        Status status = blockingStub.loadCollection(loadCollectionRequest);
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadCollection(blockingStub, request.getCollectionName(), request.getTimeout());
        }

        return null;
    }

    public Void releaseCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ReleaseCollectionReq request) {
        String title = String.format("ReleaseCollectionRequest collectionName:%s", request.getCollectionName());
        ReleaseCollectionRequest releaseCollectionRequest = ReleaseCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        Status status = blockingStub.releaseCollection(releaseCollectionRequest);
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Boolean getLoadState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetLoadStateReq request) {
        // getLoadState
        String title = String.format("GetLoadStateRequest collectionName:%s", request.getCollectionName());
        GetLoadStateRequest getLoadStateRequest = GetLoadStateRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .build();
        if(request.getPartitionName() != null) {
            getLoadStateRequest = getLoadStateRequest.toBuilder().addPartitionNames(request.getPartitionName()).build();
        }
        GetLoadStateResponse response = blockingStub.getLoadState(getLoadStateRequest);
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

    public static CreateCollectionReq.CollectionSchema createSchema() {
        return CreateCollectionReq.CollectionSchema.builder()
                .build();
    }

    public DescribeReplicasResp describeReplicas(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                 DescribeReplicasReq request) {
        if (StringUtils.isEmpty(request.getCollectionName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid collection name");
        }

        String title = String.format("DescribeReplicas collectionName:%s", request.getCollectionName());

        GetReplicasRequest.Builder requestBuilder = GetReplicasRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setWithShardNodes(true);

        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            requestBuilder.setDbName(request.getDatabaseName());
        }

        GetReplicasResponse response = blockingStub.getReplicas(requestBuilder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<ReplicaInfo> replicas = new ArrayList<>();
        List<io.milvus.grpc.ReplicaInfo> rpcReplicas = response.getReplicasList();
        for (io.milvus.grpc.ReplicaInfo info : rpcReplicas) {
            List<ShardReplica> shardReplicas = new ArrayList<>();
            List<io.milvus.grpc.ShardReplica> rpcShardReplicas = info.getShardReplicasList();
            for (io.milvus.grpc.ShardReplica shardReplica : rpcShardReplicas) {
                shardReplicas.add(ShardReplica.builder()
                        .leaderID(shardReplica.getLeaderID())
                        .leaderAddress(shardReplica.getLeaderAddr())
                        .channelName(shardReplica.getDmChannelName())
                        .nodeIDs(shardReplica.getNodeIdsList())
                        .build());
            }

            replicas.add(ReplicaInfo.builder()
                    .replicaID(info.getReplicaID())
                    .collectionID(info.getCollectionID())
                    .partitionIDs(info.getPartitionIdsList())
                    .nodeIDs(info.getNodeIdsList())
                    .resourceGroupName(info.getResourceGroupName())
                    .numOutboundNode(info.getNumOutboundNodeMap())
                    .shardReplicas(shardReplicas)
                    .build());
        }

        return DescribeReplicasResp.builder()
                .replicas(replicas)
                .build();
    }

    private void WaitForLoadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                       String collectionName, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (true) {
            // Call the getLoadState method
            boolean isLoaded = getLoadState(blockingStub, GetLoadStateReq.builder().collectionName(collectionName).build());
            if (isLoaded) {
                return;
            }

            // Check if timeout is exceeded
            if (System.currentTimeMillis() - startTime > timeoutMs) {
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

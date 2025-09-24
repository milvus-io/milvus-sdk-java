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

import io.milvus.common.utils.GTsDict;
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

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Create collection: '%s' in database: '%s'", collectionName, dbName);
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
                .setName(collectionName)
                .setDescription(request.getDescription())
                .addFields(vectorSchema)
                .addFields(idSchema)
                .setEnableDynamicField(request.getEnableDynamicField())
                .build();

        CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setSchema(schema.toByteString())
                .setShardsNum(request.getNumShards())
                .setConsistencyLevelValue(request.getConsistencyLevel().getCode());

        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status status = blockingStub.createCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        //create index
        IndexParam indexParam = IndexParam.builder()
                        .metricType(IndexParam.MetricType.valueOf(request.getMetricType()))
                        .fieldName(request.getVectorFieldName())
                        .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                        .databaseName(dbName)
                        .collectionName(collectionName)
                        .indexParams(Collections.singletonList(indexParam))
                        .sync(false)
                        .build();
        indexService.createIndex(blockingStub, createIndexReq);
        //load collection, set sync to false since no need to wait loading progress
        try {
            loadCollection(blockingStub, LoadCollectionReq.builder()
                    .databaseName(dbName)
                    .collectionName(collectionName)
                    .sync(false)
                    .build());
        } catch (Exception e) {
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load collection failed: " + e);
        }
        return null;
    }

    public Void createCollectionWithSchema(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Create collection: '%s' in database: '%s'", collectionName, dbName);

        //convert CollectionSchema to io.milvus.grpc.CollectionSchema
        CollectionSchema.Builder grpcSchemaBuilder = CollectionSchema.newBuilder()
                .setName(collectionName)
                .setDescription(request.getDescription())
                .setEnableDynamicField(request.getCollectionSchema().isEnableDynamicField());
        List<String> outputFields = new ArrayList<>();
        for (CreateCollectionReq.Function function : request.getCollectionSchema().getFunctionList()) {
            grpcSchemaBuilder.addFunctions(SchemaUtils.convertToGrpcFunction(function)).build();
            outputFields.addAll(function.getOutputFieldNames());
        }
        // normal fields
        for (CreateCollectionReq.FieldSchema fieldSchema : request.getCollectionSchema().getFieldSchemaList()) {
            FieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema);
            if (outputFields.contains(fieldSchema.getName())) {
                grpcFieldSchema = grpcFieldSchema.toBuilder().setIsFunctionOutput(true).build();
            }
            grpcSchemaBuilder.addFields(grpcFieldSchema);
        }
        // struct fields
        for (CreateCollectionReq.StructFieldSchema fieldSchema : request.getCollectionSchema().getStructFields()) {
            StructArrayFieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcStructFieldSchema(fieldSchema);
            grpcSchemaBuilder.addStructArrayFields(grpcFieldSchema);
        }

        //create collection
        CreateCollectionRequest.Builder builder = CreateCollectionRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setSchema(grpcSchemaBuilder.build().toByteString())
                .setShardsNum(request.getNumShards())
                .setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
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
                        .databaseName(dbName)
                        .collectionName(collectionName)
                        .indexParams(Collections.singletonList(indexParam))
                        .sync(false)
                        .build();
                indexService.createIndex(blockingStub, createIndexReq);
            }
            //load collection, set sync to true since no need to wait loading progress
            loadCollection(blockingStub, LoadCollectionReq.builder()
                    .databaseName(dbName)
                    .collectionName(collectionName)
                    .sync(false)
                    .build());
        }

        return null;
    }

    public ListCollectionsResp listCollections(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName) {
        String title = String.format("List collections in database: '%s'", dbName);
        ShowCollectionsRequest.Builder builder = ShowCollectionsRequest.newBuilder();
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        ShowCollectionsResponse response = blockingStub.showCollections(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());

        List<CollectionInfo> collectionInfos = new ArrayList<>();
        for (int i = 0; i < response.getCollectionNamesCount(); i++) {
            CollectionInfo collectionInfo = CollectionInfo.builder()
                    .collectionName(response.getCollectionNames(i))
                    .build();
            // Milvus version >= 2.6.1 will additionally return shardNum
            List<Integer> shardsNums = response.getShardsNumList();
            if (CollectionUtils.isNotEmpty(shardsNums)) {
                collectionInfo.setShardNum(response.getShardsNum(i));
            }
            collectionInfos.add(collectionInfo);
        }

        return ListCollectionsResp.builder()
                .collectionNames(response.getCollectionNamesList())
                .collectionInfos(collectionInfos)
                .build();
    }

    public Void dropCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Drop collection: '%s' in database: '%s'", collectionName, dbName);
        DropCollectionRequest.Builder builder = DropCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.dropCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        // remove the last write timestamp for this collection
        String key = GTsDict.CombineCollectionName(actualDbName(dbName), collectionName);
        GTsDict.getInstance().removeCollectionTs(key);
        return null;
    }

    public Void alterCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterCollectionPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Alter properties of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.alterCollection(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void addCollectionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AddCollectionFieldReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Add field to collection: '%s' in database: '%s'", collectionName, dbName);
        AddCollectionFieldRequest.Builder builder = AddCollectionFieldRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFieldReqToFieldSchema(request);
        FieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema);
        builder.setSchema(grpcFieldSchema.toByteString());

        Status response = blockingStub.addCollectionField(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void alterCollectionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterCollectionFieldReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Alter field of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionFieldRequest.Builder builder = AlterCollectionFieldRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFieldName(request.getFieldName());
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(request.getProperties());
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addProperties);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.alterCollectionField(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Drop properties of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.alterCollection(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Void dropCollectionFieldProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionFieldPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String fieldName = request.getFieldName();
        String title = String.format("Drop properties of field: '%s' of collection: '%s' in database: '%s'",
                fieldName, collectionName, dbName);

        AlterCollectionFieldRequest.Builder builder = AlterCollectionFieldRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFieldName(fieldName)
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.alterCollectionField(builder.build());
        rpcUtils.handleResponse(title, response);

        return null;
    }

    public Boolean hasCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HasCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Has collection: '%s' in database:'%s'", collectionName, dbName);
        HasCollectionRequest.Builder builder = HasCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        BoolResponse response = blockingStub.hasCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return response.getValue();
    }

    public DescribeCollectionResp describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Describe collection: '%s' in database: '%s'", collectionName, dbName);
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        DescribeCollectionResponse response = blockingStub.describeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return convertUtils.convertDescCollectionResp(response);
    }

    public List<DescribeCollectionResp> batchDescribeCollections(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, BatchDescribeCollectionReq request) {
        String dbName = request.getDatabaseName();
        List<String> collectionNames = request.getCollectionNames();
        String title = String.format("Batch describe collections: '%s' in database: '%s'", collectionNames, dbName);
        BatchDescribeCollectionRequest.Builder builder = BatchDescribeCollectionRequest.newBuilder()
                .addAllCollectionName(collectionNames);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        BatchDescribeCollectionResponse response = blockingStub.batchDescribeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return convertUtils.convertDescCollectionsResp(response);
    }

    public Void renameCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RenameCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String newName = request.getNewCollectionName();
        String title = String.format("Rename collection: '%s' to '%s' in database: '%s'", collectionName, newName, dbName);
        RenameCollectionRequest.Builder builder = RenameCollectionRequest.newBuilder()
                .setOldName(collectionName)
                .setNewName(newName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.renameCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Void loadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Load collection: '%s' in database: '%s'", collectionName, dbName);
        LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(request.getRefresh())
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(request.getSkipLoadDynamicField())
                .addAllResourceGroups(request.getResourceGroups());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.loadCollection(builder.build());
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadCollection(blockingStub, dbName, collectionName, request.getTimeout());
        }

        return null;
    }

    public Void refreshLoad(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RefreshLoadReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Refresh load collection: '%s' in database: '%s'", collectionName, dbName);
        LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setRefresh(true);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.loadCollection(builder.build());
        rpcUtils.handleResponse(title, status);
        if (request.getSync()) {
            WaitForLoadCollection(blockingStub, dbName, collectionName, request.getTimeout());
        }

        return null;
    }

    public Void releaseCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, ReleaseCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Release collection: '%s' in database: '%s'", collectionName, dbName);
        ReleaseCollectionRequest.Builder builder = ReleaseCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.releaseCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        return null;
    }

    public Boolean getLoadState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetLoadStateReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        String title = String.format("Get load state of collection: '%s' in database: '%s'", collectionName, dbName);
        GetLoadStateRequest.Builder builder = GetLoadStateRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (StringUtils.isNotEmpty(partitionName)) {
            builder.addPartitionNames(partitionName);
        }
        GetLoadStateResponse response = blockingStub.getLoadState(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        // throw error if cannot find the collection of partition
        if (response.getState() == LoadState.LoadStateNotExist) {
            String msg = String.format("collection: '%s' doesn't exist in database: '%s'", collectionName, dbName);
            if (StringUtils.isNotEmpty(partitionName)) {
                msg = String.format("partition: '%s' of %s", partitionName, msg);
            }
            throw new MilvusClientException(ErrorCode.SERVER_ERROR, msg);
        }
        return response.getState() == LoadState.LoadStateLoaded;
    }

    public GetCollectionStatsResp getCollectionStats(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetCollectionStatsReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Get statistics of collection: '%s' in database: '%s'", collectionName, dbName);
        GetCollectionStatisticsRequest.Builder builder = GetCollectionStatisticsRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        GetCollectionStatisticsResponse response = blockingStub.getCollectionStatistics(builder.build());

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
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        if (StringUtils.isEmpty(collectionName)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Invalid collection name");
        }

        String title = String.format("Describe replicas of collection: '%s' in database: '%s'", collectionName, dbName);
        GetReplicasRequest.Builder requestBuilder = GetReplicasRequest.newBuilder()
                .setCollectionName(collectionName)
                .setWithShardNodes(true);
        if (StringUtils.isNotEmpty(dbName)) {
            requestBuilder.setDbName(dbName);
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

    private void WaitForLoadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String databaseName,
                                       String collectionName, long timeoutMs) {
        long startTime = System.currentTimeMillis(); // Capture start time/ Timeout in milliseconds (60 seconds)

        while (true) {
            // Call the getLoadState method
            boolean isLoaded = getLoadState(blockingStub, GetLoadStateReq.builder()
                    .databaseName(databaseName)
                    .collectionName(collectionName)
                    .build());
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
                logger.error("Thread was interrupted, Failed to complete operation");
                return; // or handle interruption appropriately
            }
        }
    }
}

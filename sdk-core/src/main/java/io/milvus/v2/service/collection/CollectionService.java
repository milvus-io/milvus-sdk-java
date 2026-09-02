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

import io.grpc.StatusRuntimeException;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.DescribeReplicasResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.GetLoadStateResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.utils.SchemaUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Service for collection-related operations, such as creating, loading, and describing collections.
 */
public class CollectionService extends BaseService {
    private static final String ALLOW_INSERT_AUTO_ID = "allow_insert_auto_id";

    public IndexService indexService = new IndexService();

    /**
     * Creates a collection. When a {@code collectionSchema} is provided the collection is
     * created from the schema; otherwise a simple schema is built from the request parameters.
     * An index is created on the vector field and the collection is loaded afterwards.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the create collection request
     * @return {@code null}
     */
    public Void createCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        if (request.getCollectionSchema() != null) {
            //create collections with schema
            request.getCollectionSchema().verify();
            createCollectionWithSchema(blockingStub, request);
            return null;
        }

        if (request.getDimension() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Dimension is undefined.");
        }
        io.milvus.v2.common.DataType idType = request.getIdType();
        if (idType == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Primary key type is required");
        }
        if (idType != io.milvus.v2.common.DataType.Int64 && idType != io.milvus.v2.common.DataType.VarChar) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Primary key type must be Int64 or VarChar, got: " + idType.name());
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
        invalidateSchemaCache(dbName, collectionName);

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

    /**
     * Creates a collection with the provided {@code CreateCollectionReq.CollectionSchema}.
     * Indices are created for the given index parameters and the collection is loaded afterwards.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the create collection request
     * @return {@code null}
     */
    public Void createCollectionWithSchema(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, CreateCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Create collection: '%s' in database: '%s'", collectionName, dbName);

        //convert CollectionSchema to io.milvus.grpc.CollectionSchema
        CollectionSchema.Builder grpcSchemaBuilder = CollectionSchema.newBuilder()
                .setName(collectionName)
                .setDescription(request.getDescription())
                .setEnableDynamicField(request.getCollectionSchema().isEnableDynamicField())
                .setExternalSource(request.getCollectionSchema().getExternalSource())
                .setExternalSpec(JsonUtils.toJsonString(request.getCollectionSchema().getExternalSpec()));
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
        invalidateSchemaCache(dbName, collectionName);

        //create index
        if (request.getIndexParams() != null && !request.getIndexParams().isEmpty()) {
            for (IndexParam indexParam : request.getIndexParams()) {
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

    /**
     * Lists the collections in the given database.
     *
     * @param blockingStub the gRPC blocking stub
     * @param dbName the database name
     * @return the list collections response
     */
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

    /**
     * Drops the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop collection request
     * @return {@code null}
     */
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

        invalidateSchemaCache(dbName, collectionName);
        invalidateTsCache(dbName, collectionName);
        return null;
    }

    /**
     * Truncates all data in the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the truncate collection request
     * @return {@code null}
     */
    public Void truncateCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, TruncateCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Truncate collection: '%s' in database: '%s'", collectionName, dbName);
        TruncateCollectionRequest.Builder builder = TruncateCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.truncateCollection(builder.build()).getStatus();
        rpcUtils.handleResponse(title, status);

        invalidateTsCache(dbName, collectionName);
        return null;
    }

    /**
     * Alters the properties of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the alter collection properties request
     * @return {@code null}
     */
    public Void alterCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AlterCollectionPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        boolean invalidatesSchema = request.getProperties().containsKey(ALLOW_INSERT_AUTO_ID);
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
        if (invalidatesSchema) {
            invalidateSchemaCache(dbName, collectionName);
        }

        return null;
    }

    /**
     * Adds a field to the specified collection. Falls back to the legacy
     * {@code addCollectionField} RPC when the server does not support schema alteration.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the add collection field request
     * @return {@code null}
     */
    public Void addCollectionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AddCollectionFieldReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Add field to collection: '%s' in database: '%s'", collectionName, dbName);

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFieldReqToFieldSchema(request);
        FieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema, true);
        AlterCollectionSchemaRequest.FieldInfo fieldInfo = AlterCollectionSchemaRequest.FieldInfo.newBuilder()
                .setFieldSchema(grpcFieldSchema)
                .build();
        AlterCollectionSchemaRequest.AddRequest addRequest = AlterCollectionSchemaRequest.AddRequest.newBuilder()
                .addFieldInfos(fieldInfo)
                .build();

        AlterCollectionSchemaRequest.Builder builder = AlterCollectionSchemaRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAction(AlterCollectionSchemaRequest.Action.newBuilder()
                        .setAddRequest(addRequest)
                        .build());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        try {
            AlterCollectionSchemaResponse response = blockingStub.alterCollectionSchema(builder.build());
            rpcUtils.handleResponse(title, response.getAlterStatus());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != io.grpc.Status.Code.UNIMPLEMENTED) {
                throw e;
            }

            AddCollectionFieldRequest.Builder legacyBuilder = AddCollectionFieldRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setSchema(grpcFieldSchema.toByteString());
            if (StringUtils.isNotEmpty(dbName)) {
                legacyBuilder.setDbName(dbName);
            }

            AddCollectionFieldRequest legacyRequest = legacyBuilder.build();
            rpcUtils.retry(() -> {
                Status response = blockingStub.addCollectionField(legacyRequest);
                rpcUtils.handleResponse(title, response);
                return null;
            });
        }
        invalidateSchemaCache(dbName, collectionName);
        return null;
    }

    /**
     * Adds a struct field to the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the add collection struct field request
     * @return {@code null}
     */
    public Void addCollectionStructField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, AddCollectionStructFieldReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Add struct field to collection: '%s' in database: '%s'", collectionName, dbName);

        CreateCollectionReq.StructFieldSchema structFieldSchema;
        try {
            structFieldSchema = request.toStructFieldSchema();
        } catch (ParamException e) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, e.getMessage());
        }
        AddCollectionStructFieldRequest.Builder builder = AddCollectionStructFieldRequest.newBuilder()
                .setCollectionName(collectionName)
                .setStructArrayFieldSchema(SchemaUtils.convertToGrpcStructFieldSchema(structFieldSchema));
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.addCollectionStructField(builder.build());
        rpcUtils.handleResponse(title, response);
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Alters the properties of a field in the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the alter collection field request
     * @return {@code null}
     */
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
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Drops a field from the specified collection, identified by either a field name or a field ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop collection field request
     * @return {@code null}
     */
    public Void dropCollectionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionFieldReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String fieldName = request.getFieldName();
        Long fieldId = request.getFieldId();
        boolean hasFieldName = StringUtils.isNotBlank(fieldName);
        boolean hasFieldId = fieldId != null && fieldId > 0;
        if (hasFieldName == hasFieldId) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Exactly one of fieldName or fieldId must be provided.");
        }

        String title = String.format("Drop field of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionSchemaRequest.DropRequest.Builder dropBuilder = AlterCollectionSchemaRequest.DropRequest.newBuilder();
        if (hasFieldName) {
            dropBuilder.setFieldName(fieldName);
        } else {
            dropBuilder.setFieldId(fieldId);
        }

        AlterCollectionSchemaRequest.Builder builder = AlterCollectionSchemaRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAction(AlterCollectionSchemaRequest.Action.newBuilder()
                        .setDropRequest(dropBuilder)
                        .build());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        AlterCollectionSchemaResponse response = blockingStub.alterCollectionSchema(builder.build());
        rpcUtils.handleResponse(title, response.getAlterStatus());
        invalidateSchemaCache(dbName, collectionName);
        return null;
    }

    /**
     * Drops the specified properties from the collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop collection properties request
     * @return {@code null}
     */
    public Void dropCollectionProperties(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DropCollectionPropertiesReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        boolean invalidatesSchema = request.getPropertyKeys().contains(ALLOW_INSERT_AUTO_ID);
        String title = String.format("Drop properties of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionRequest.Builder builder = AlterCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllDeleteKeys(request.getPropertyKeys());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status response = blockingStub.alterCollection(builder.build());
        rpcUtils.handleResponse(title, response);
        if (invalidatesSchema) {
            invalidateSchemaCache(dbName, collectionName);
        }

        return null;
    }

    /**
     * Drops the specified properties from a field of the collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop collection field properties request
     * @return {@code null}
     */
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
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Checks whether the specified collection exists.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the has collection request
     * @return {@code true} if the collection exists, otherwise {@code false}
     */
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

    /**
     * Describes the specified collection by name or collection ID.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the describe collection request
     * @return the describe collection response
     */
    public DescribeCollectionResp describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DescribeCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        Long collectionId = request.getCollectionId();
        String title = String.format("Describe collection: '%s'(id: %s) in database: '%s'", collectionName, collectionId, dbName);
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder();
        if (StringUtils.isNotEmpty(collectionName)) {
            builder.setCollectionName(collectionName);
        }
        if (collectionId != null) {
            builder.setCollectionID(collectionId);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        DescribeCollectionResponse response = blockingStub.describeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return convertUtils.convertDescCollectionResp(response);
    }

    /**
     * Describes multiple collections by names or collection IDs in a single request.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the batch describe collection request
     * @return the list of describe collection responses
     */
    public List<DescribeCollectionResp> batchDescribeCollections(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, BatchDescribeCollectionReq request) {
        String dbName = request.getDatabaseName();
        List<String> collectionNames = request.getCollectionNames();
        List<Long> collectionIds = request.getCollectionIds();
        String title = String.format("Batch describe collections: '%s'(ids: %s) in database: '%s'", collectionNames, collectionIds, dbName);
        BatchDescribeCollectionRequest.Builder builder = BatchDescribeCollectionRequest.newBuilder();
        if (CollectionUtils.isNotEmpty(collectionNames)) {
            builder.addAllCollectionName(collectionNames);
        }
        if (CollectionUtils.isNotEmpty(collectionIds)) {
            builder.addAllCollectionID(collectionIds);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        BatchDescribeCollectionResponse response = blockingStub.batchDescribeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return convertUtils.convertDescCollectionsResp(response);
    }

    /**
     * Renames the specified collection, optionally moving it to a target database.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the rename collection request
     * @return {@code null}
     */
    public Void renameCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RenameCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String newName = request.getNewCollectionName();
        String targetDbName = request.getTargetDbName();
        String title = String.format("Rename collection: '%s' to '%s' in database: '%s'", collectionName, newName, dbName);
        RenameCollectionRequest.Builder builder = RenameCollectionRequest.newBuilder()
                .setOldName(collectionName)
                .setNewName(newName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (StringUtils.isNotEmpty(targetDbName)) {
            builder.setNewDBName(targetDbName);
        }
        Status status = blockingStub.renameCollection(builder.build());
        rpcUtils.handleResponse(title, status);

        String sourceDb = actualDbName(dbName);
        String targetDb = StringUtils.isNotEmpty(targetDbName) ? targetDbName : sourceDb;
        invalidateSchemaCache(sourceDb, collectionName);
        invalidateSchemaCache(targetDb, newName);
        CollectionTsCache.getInstance().move(getEndpoint(), sourceDb, collectionName, targetDb, newName);

        return null;
    }

    /**
     * Loads the specified collection into memory. When {@code sync} is enabled, waits until
     * the load progress reaches 100%.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the load collection request
     * @return {@code null}
     */
    public Void loadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, LoadCollectionReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        boolean sync = Boolean.TRUE.equals(request.getSync());
        boolean refresh = Boolean.TRUE.equals(request.getRefresh());
        boolean skipLoadDynamicField = Boolean.TRUE.equals(request.getSkipLoadDynamicField());
        String title = String.format("Load collection: '%s' in database: '%s'", collectionName, dbName);
        LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setReplicaNumber(request.getNumReplicas())
                .setRefresh(refresh)
                .addAllLoadFields(request.getLoadFields())
                .setSkipLoadDynamicField(skipLoadDynamicField)
                .addAllResourceGroups(request.getResourceGroups());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub = blockingStub;
        if (request.getTimeout() != null && request.getTimeout() > 0) {
            tempBlockingStub = tempBlockingStub.withDeadlineAfter(request.getTimeout(), TimeUnit.MILLISECONDS);
        }
        Status status = tempBlockingStub.loadCollection(builder.build());
        rpcUtils.handleResponse(title, status);
        if (sync) {
            waitForLoadCollection(blockingStub, dbName, collectionName, request.getTimeout(), refresh);
        }

        return null;
    }

    /**
     * Refreshes the loaded collection data so that newly flushed data becomes searchable.
     * When {@code sync} is enabled, waits until the refresh load completes.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the refresh load request
     * @return {@code null}
     */
    public Void refreshLoad(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RefreshLoadReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        boolean sync = Boolean.TRUE.equals(request.getSync());
        String title = String.format("Refresh load collection: '%s' in database: '%s'", collectionName, dbName);
        LoadCollectionRequest.Builder builder = LoadCollectionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setRefresh(true);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub = blockingStub;
        if (request.getTimeout() != null && request.getTimeout() > 0) {
            tempBlockingStub = tempBlockingStub.withDeadlineAfter(request.getTimeout(), TimeUnit.MILLISECONDS);
        }
        Status status = tempBlockingStub.loadCollection(builder.build());
        rpcUtils.handleResponse(title, status);
        if (sync) {
            waitForLoadCollection(blockingStub, dbName, collectionName, request.getTimeout(), true);
        }

        return null;
    }

    /**
     * Releases the specified collection from memory.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the release collection request
     * @return {@code null}
     */
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

    /**
     * Returns the load state of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get load state request
     * @return {@code true} when the collection is loaded, otherwise {@code false}
     */
    public Boolean getLoadState(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetLoadStateReq request) {
        return getLoadStateResponse(blockingStub, request).getState() == LoadState.LoadStateLoaded;
    }

    /**
     * Returns the load state of the specified collection together with the loading progress
     * when the collection is still loading.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get load state request
     * @return the get load state response
     */
    public GetLoadStateResp getLoadStateV2(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetLoadStateReq request) {
        GetLoadStateResponse response = getLoadStateResponse(blockingStub, request);
        GetLoadStateResp.GetLoadStateRespBuilder respBuilder = GetLoadStateResp.builder()
                .state(response.getState());
        if (response.getState() == LoadState.LoadStateLoading) {
            respBuilder.progress(getLoadingProgress(blockingStub, request, false, null));
        }

        return respBuilder.build();
    }

    private GetLoadStateResponse getLoadStateResponse(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                      GetLoadStateReq request) {
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
        return response;
    }

    private Long getLoadingProgress(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                    GetLoadStateReq request,
                                    boolean refreshLoad,
                                    Long timeoutMs) {
        GetLoadingProgressResponse response = getLoadingProgressInternal(blockingStub, request, timeoutMs);
        return refreshLoad ? response.getRefreshProgress() : response.getProgress();
    }

    private GetLoadingProgressResponse getLoadingProgressInternal(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                                  GetLoadStateReq request,
                                                                  Long timeoutMs) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String partitionName = request.getPartitionName();
        GetLoadingProgressRequest.Builder builder = GetLoadingProgressRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        if (StringUtils.isNotEmpty(partitionName)) {
            builder.addPartitionNames(partitionName);
        }
        MilvusServiceGrpc.MilvusServiceBlockingStub tempBlockingStub = blockingStub;
        if (timeoutMs != null && timeoutMs > 0) {
            tempBlockingStub = tempBlockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
        }
        GetLoadingProgressResponse response = tempBlockingStub.getLoadingProgress(builder.build());
        String title = String.format("Get loading progress of collection: '%s' in database: '%s'", collectionName, dbName);
        rpcUtils.handleResponse(title, response.getStatus());
        return response;
    }

    /**
     * Returns the statistics of the specified collection, including the row count.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the get collection stats request
     * @return the collection statistics response
     */
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
        Map<String, String> stats = new HashMap<>();
        response.getStatsList().forEach(stat -> stats.put(stat.getKey(), stat.getValue()));
        GetCollectionStatsResp getCollectionStatsResp = GetCollectionStatsResp.builder()
                .numOfEntities(response.getStatsList().stream().filter(stat -> stat.getKey().equals("row_count")).map(stat -> Long.parseLong(stat.getValue())).findFirst().get())
                .stats(stats)
                .build();
        return getCollectionStatsResp;
    }

    /**
     * Creates an empty {@code CollectionSchema} builder result for use with the create collection API.
     *
     * @return an empty collection schema
     */
    public static CreateCollectionReq.CollectionSchema createSchema() {
        return CreateCollectionReq.CollectionSchema.builder()
                .build();
    }

    /**
     * Describes the replicas of the specified collection, including their shard replicas.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the describe replicas request
     * @return the describe replicas response
     */
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

    /**
     * Adds a function (for example BM25) to the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the add collection function request
     * @return {@code null}
     */
    public Void addCollectionFunction(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                      AddCollectionFunctionReq request) {
        if (request.getFunction() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function cannot be null.");
        }

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Add function to collection: '%s' in database: '%s'", collectionName, dbName);
        AddCollectionFunctionRequest.Builder builder = AddCollectionFunctionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFunctionSchema(SchemaUtils.convertToGrpcFunction(request.getFunction()));
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.addCollectionFunction(builder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Adds a function output field together with its bound index to the specified collection.
     * Only BM25 (with SparseFloatVector) and MINHASH (with BinaryVector) function fields are supported.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the add function field request
     * @return {@code null}
     */
    public Void addFunctionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                 AddFunctionFieldReq request) {
        if (request.getFunction() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function cannot be null.");
        }
        if (request.getFunction().getOutputFieldNames() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Function output field names cannot be null.");
        }
        FunctionType functionType = request.getFunction().getFunctionType();
        io.milvus.v2.common.DataType expectedOutputType;
        if (functionType == FunctionType.BM25) {
            expectedOutputType = io.milvus.v2.common.DataType.SparseFloatVector;
        } else if (functionType == FunctionType.MINHASH) {
            expectedOutputType = io.milvus.v2.common.DataType.BinaryVector;
        } else {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "addFunctionField only supports FunctionType.BM25 with SparseFloatVector "
                            + "or FunctionType.MINHASH with BinaryVector for now, got " + functionType);
        }
        if (request.getDataType() != expectedOutputType) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    String.format("addFunctionField requires %s output field for %s, got %s",
                            expectedOutputType, functionType, request.getDataType()));
        }
        IndexParam indexParam = request.getIndexParam();
        if (indexParam == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Bound index cannot be null.");
        }
        if (StringUtils.isNotEmpty(indexParam.getFieldName())
                && !StringUtils.equals(request.getFieldName(), indexParam.getFieldName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Bound index field must be empty or match the field being added.");
        }
        if (indexParam.getIndexType() == null
                || indexParam.getIndexType() == IndexParam.IndexType.None) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Bound index must specify a non-None index type.");
        }

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Add function field to collection: '%s' in database: '%s'", collectionName, dbName);

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFieldReqToFieldSchema(request);
        FieldSchema grpcFieldSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema)
                .toBuilder()
                .setIsFunctionOutput(true)
                .build();
        AlterCollectionSchemaRequest.FieldInfo.Builder fieldInfoBuilder = AlterCollectionSchemaRequest.FieldInfo.newBuilder()
                .setFieldSchema(grpcFieldSchema)
                .addExtraParams(KeyValuePair.newBuilder()
                        .setKey(Constant.INDEX_TYPE)
                        .setValue(indexParam.getIndexType().getName())
                        .build());
        if (StringUtils.isNotEmpty(indexParam.getIndexName())) {
            fieldInfoBuilder.setIndexName(indexParam.getIndexName());
        }
        if (indexParam.getMetricType() != null) {
            fieldInfoBuilder.addExtraParams(KeyValuePair.newBuilder()
                    .setKey(Constant.METRIC_TYPE)
                    .setValue(indexParam.getMetricType().name())
                    .build());
        }
        Map<String, Object> extraParams = indexParam.getExtraParams();
        if (extraParams != null && !extraParams.isEmpty()) {
            for (Map.Entry<String, Object> entry : extraParams.entrySet()) {
                if (Constant.INDEX_TYPE.equals(entry.getKey())
                        || (Constant.METRIC_TYPE.equals(entry.getKey()) && indexParam.getMetricType() != null)) {
                    continue;
                }
                fieldInfoBuilder.addExtraParams(KeyValuePair.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue().toString())
                        .build());
            }
        }
        AlterCollectionSchemaRequest.FieldInfo fieldInfo = fieldInfoBuilder.build();
        AlterCollectionSchemaRequest.AddRequest addRequest = AlterCollectionSchemaRequest.AddRequest.newBuilder()
                .addFieldInfos(fieldInfo)
                .addFuncSchema(SchemaUtils.convertToGrpcFunction(request.getFunction()))
                .build();

        AlterCollectionSchemaRequest.Builder builder = AlterCollectionSchemaRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAction(AlterCollectionSchemaRequest.Action.newBuilder()
                        .setAddRequest(addRequest)
                        .build());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        AlterCollectionSchemaResponse response = blockingStub.alterCollectionSchema(builder.build());
        rpcUtils.handleResponse(title, response.getAlterStatus());
        invalidateSchemaCache(dbName, collectionName);
        return null;
    }

    /**
     * Alters a function of the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the alter collection function request
     * @return {@code null}
     */
    public Void alterCollectionFunction(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                        AlterCollectionFunctionReq request) {
        if (request.getFunction() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function cannot be null.");
        }

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Alter function of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionFunctionRequest.Builder builder = AlterCollectionFunctionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFunctionName(request.getFunction().getName())
                .setFunctionSchema(SchemaUtils.convertToGrpcFunction(request.getFunction()));
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        Status status = blockingStub.alterCollectionFunction(builder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Drops a function from the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop collection function request
     * @return {@code null}
     */
    public Void dropCollectionFunction(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                       DropCollectionFunctionReq request) {
        if (StringUtils.isEmpty(request.getFunctionName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function name cannot be empty.");
        }

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Drop function to collection: '%s' in database: '%s'", collectionName, dbName);
        DropCollectionFunctionRequest.Builder builder = DropCollectionFunctionRequest.newBuilder()
                .setCollectionName(collectionName)
                .setFunctionName(request.getFunctionName());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        Status status = blockingStub.dropCollectionFunction(builder.build());
        rpcUtils.handleResponse(title, status);
        invalidateSchemaCache(dbName, collectionName);

        return null;
    }

    /**
     * Drops the output fields of a function from the specified collection.
     *
     * @param blockingStub the gRPC blocking stub
     * @param request the drop function field request
     * @return {@code null}
     */
    public Void dropFunctionField(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                  DropFunctionFieldReq request) {
        if (StringUtils.isEmpty(request.getFunctionName())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Function name cannot be empty.");
        }

        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Drop function field of collection: '%s' in database: '%s'", collectionName, dbName);
        AlterCollectionSchemaRequest.Builder builder = AlterCollectionSchemaRequest.newBuilder()
                .setCollectionName(collectionName)
                .setAction(AlterCollectionSchemaRequest.Action.newBuilder()
                        .setDropRequest(AlterCollectionSchemaRequest.DropRequest.newBuilder()
                                .setFunctionName(request.getFunctionName())
                                .setDropFunctionOutputFields(true)
                                .build())
                        .build());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        AlterCollectionSchemaResponse response = blockingStub.alterCollectionSchema(builder.build());
        rpcUtils.handleResponse(title, response.getAlterStatus());
        invalidateSchemaCache(dbName, collectionName);
        return null;
    }

    private void waitForLoadCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String databaseName,
                                       String collectionName, Long timeoutMs, boolean refreshLoad) {
        long startTime = System.currentTimeMillis();
        GetLoadStateReq request = GetLoadStateReq.builder()
                .databaseName(databaseName)
                .collectionName(collectionName)
                .build();

        while (true) {
            if (getLoadingProgress(blockingStub, request, refreshLoad, timeoutMs) >= 100L) {
                return;
            }

            if (timeoutMs != null && timeoutMs > 0 && System.currentTimeMillis() - startTime > timeoutMs) {
                throw new MilvusClientException(ErrorCode.SERVER_ERROR, "Load collection timeout");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread was interrupted, Failed to complete operation");
                return;
            }
        }
    }

}

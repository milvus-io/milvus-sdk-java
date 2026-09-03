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

import io.milvus.grpc.LoadCollectionRequest;
import io.milvus.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.DescribeReplicasResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.GetLoadStateResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CollectionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Test
    void testListCollections() {
        ListCollectionsResp a = client_v2.listCollections();
    }

    @Test
    void testCreateCollection() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        client_v2.createCollection(req);
    }

    @Test
    void testEnableDynamicSchema() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .enableDynamicField(false)
                .build();
        Assertions.assertFalse(req.getEnableDynamicField());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build());

        req = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .build();
        Assertions.assertTrue(req.getEnableDynamicField());
        Assertions.assertTrue(req.getCollectionSchema().isEnableDynamicField());

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .enableDynamicField(false)
                .collectionSchema(collectionSchema)
                .build()
        );

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .enableDynamicField(false)
                .build()
        );
    }

    @Test
    void testCreateCollectionWithSchema() {

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build())
                .addField(AddFieldReq.builder().fieldName("description").dataType(DataType.VarChar).maxLength(64).build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .build();
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName("description")
                .indexType(IndexParam.IndexType.INVERTED)
                .build();


        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .indexParams(Arrays.asList(indexParam, indexParam2))
                .indexParam(IndexParam.builder() // fluent api, add index param
                        .fieldName("id")
                        .indexType(IndexParam.IndexType.INVERTED)
                        .build()
                )
                .build();
        client_v2.createCollection(request);

        AlterCollectionReq req = AlterCollectionReq.builder()
                .collectionName("test")
                .property("prop", "val")
                .build();
        assertEquals("val", req.getProperties().get("prop"));
    }

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.dropCollection(req);
    }

    @Test
    void testTruncateCollection() {
        TruncateCollectionReq req = TruncateCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.truncateCollection(req);
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.hasCollection(req);
    }

    @Test
    void testDescribeCollection() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionById() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionByNameAndId() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testBatchDescribeCollectionsByNames() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Arrays.asList("test", "test2"))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionIds(Arrays.asList(123456L, 789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByNamesAndIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Collections.singletonList("test"))
                .collectionIds(Collections.singletonList(789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testRenameCollection() {
        RenameCollectionReq req = RenameCollectionReq.builder()
                .collectionName("test2")
                .newCollectionName("test")
                .build();
        client_v2.renameCollection(req);
    }

    @Test
    void testLoadCollection() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.loadCollection(req);

    }

    @Test
    void testLoadCollectionWithPriority() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .priority("High")
                .build();
        client_v2.loadCollection(req);

        ArgumentCaptor<LoadCollectionRequest> captor = ArgumentCaptor.forClass(LoadCollectionRequest.class);
        verify(blockingStub).loadCollection(captor.capture());
        assertEquals("high", captor.getValue().getLoadParamsMap().get("load_priority"));
    }

    @Test
    void testLoadCollectionPriorityLowercased() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .priority("LOW")
                .build();
        client_v2.loadCollection(req);

        ArgumentCaptor<LoadCollectionRequest> captor = ArgumentCaptor.forClass(LoadCollectionRequest.class);
        verify(blockingStub).loadCollection(captor.capture());
        assertEquals("low", captor.getValue().getLoadParamsMap().get("load_priority"));
    }

    @Test
    void testReleaseCollection() {
        ReleaseCollectionReq req = ReleaseCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.releaseCollection(req);
    }

    @Test
    void testGetLoadState() {
        GetLoadStateReq req = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.getLoadState(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testGetCollectionStats() {
        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
        Assertions.assertEquals(10L, resp.getNumOfEntities());
        Assertions.assertEquals("10", resp.getStats().get("row_count"));
    }

    @Test
    void testCreateCollectionFastPathSetsProperties() {
        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(2)
                .numPartitions(4)
                .property("collection.ttl.seconds", "100")
                .build();
        client_v2.createCollection(request);

        ArgumentCaptor<io.milvus.grpc.CreateCollectionRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.CreateCollectionRequest.class);
        verify(blockingStub).createCollection(captor.capture());
        io.milvus.grpc.CreateCollectionRequest rpcRequest = captor.getValue();
        Assertions.assertEquals(4, rpcRequest.getNumPartitions());
        boolean found = rpcRequest.getPropertiesList().stream()
                .anyMatch(kv -> kv.getKey().equals("collection.ttl.seconds") && kv.getValue().equals("100"));
        Assertions.assertTrue(found);
    }

    @Test
    void testCreateCollectionNumericBoundsValidated() {
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(0)
                .build());
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(2)
                .numPartitions(0)
                .build());
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(2)
                .numShards(0)
                .build());
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(2)
                .idType(DataType.VarChar)
                .maxLength(0)
                .build());
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .dimension(2)
                .idType(DataType.VarChar)
                .maxLength(65536)
                .build());
    }

    @Test
    void testDescribeCollectionSurfacesAliasesAndSchemaMetadata() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        io.milvus.grpc.CollectionSchema schema = io.milvus.grpc.CollectionSchema.newBuilder()
                .setName("test")
                .setEnableDynamicField(true)
                .setVersion(3)
                .addFields(io.milvus.grpc.FieldSchema.newBuilder()
                        .setFieldID(100L)
                        .setName("id")
                        .setDataType(io.milvus.grpc.DataType.Int64)
                        .setIsPrimaryKey(true)
                        .build())
                .addFields(io.milvus.grpc.FieldSchema.newBuilder()
                        .setFieldID(101L)
                        .setName("text")
                        .setDataType(io.milvus.grpc.DataType.VarChar)
                        .build())
                .addFunctions(io.milvus.grpc.FunctionSchema.newBuilder()
                        .setId(200L)
                        .setName("bm25")
                        .setType(io.milvus.grpc.FunctionType.BM25)
                        .addInputFieldNames("text")
                        .addInputFieldIds(101L)
                        .addOutputFieldNames("sparse")
                        .addOutputFieldIds(102L)
                        .build())
                .build();
        io.milvus.grpc.DescribeCollectionResponse describeResponse = io.milvus.grpc.DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus)
                .setCollectionName("test")
                .setCollectionID(1L)
                .setSchema(schema)
                .setNumPartitions(2)
                .addAliases("alias1")
                .setConsistencyLevel(io.milvus.grpc.ConsistencyLevel.Bounded)
                .setUpdateTimestamp(123456L)
                .build();
        when(blockingStub.describeCollection(any())).thenReturn(describeResponse);

        DescribeCollectionResp resp = client_v2.describeCollection(DescribeCollectionReq.builder()
                .collectionName("test")
                .build());
        Assertions.assertEquals(Collections.singletonList("alias1"), resp.getAliases());
        Assertions.assertEquals("Bounded", resp.getConsistencyLevelName());
        Assertions.assertEquals(123456L, resp.getUpdateTimestamp());

        CreateCollectionReq.FieldSchema idField = resp.getCollectionSchema().getField("id");
        Assertions.assertEquals(100L, idField.getFieldId());

        CreateCollectionReq.Function function = resp.getCollectionSchema().getFunctionList().get(0);
        Assertions.assertEquals(200L, function.getId());
        Assertions.assertEquals(Collections.singletonList(101L), function.getInputFieldIds());
        Assertions.assertEquals(Collections.singletonList(102L), function.getOutputFieldIds());
    }

    @Test
    void testFieldSchemaServerAssignedAttributesDefaultNull() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .build();
        // server-assigned attributes are null until populated by describe_collection
        Assertions.assertNull(fieldSchema.getFieldId());
        Assertions.assertNull(fieldSchema.getIsDynamic());
        Assertions.assertNull(fieldSchema.getIsFunctionOutput());
    }

    @Test
    void testFieldSchemaServerAssignedAttributesSetters() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .build();
        fieldSchema.setFieldId(100L);
        fieldSchema.setIsDynamic(Boolean.TRUE);
        fieldSchema.setIsFunctionOutput(Boolean.TRUE);
        Assertions.assertEquals(100L, fieldSchema.getFieldId());
        Assertions.assertEquals(Boolean.TRUE, fieldSchema.getIsDynamic());
        Assertions.assertEquals(Boolean.TRUE, fieldSchema.getIsFunctionOutput());
    }

    @Test
    void testFieldSchemaToStringOmitsServerAssignedAttributesWhenNull() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .build();
        String str = fieldSchema.toString();
        Assertions.assertFalse(str.contains("fieldId="));
        Assertions.assertFalse(str.contains("isDynamic="));
        Assertions.assertFalse(str.contains("isFunctionOutput="));
    }

    @Test
    void testFieldSchemaToStringIncludesServerAssignedAttributesWhenSet() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .build();
        fieldSchema.setFieldId(100L);
        fieldSchema.setIsDynamic(Boolean.TRUE);
        fieldSchema.setIsFunctionOutput(Boolean.TRUE);
        String str = fieldSchema.toString();
        Assertions.assertTrue(str.contains("fieldId=100"));
        Assertions.assertTrue(str.contains("isDynamic=true"));
        Assertions.assertTrue(str.contains("isFunctionOutput=true"));
    }

    @Test
    void testFunctionServerAssignedAttributesDefaultEmpty() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .build();
        Assertions.assertNull(function.getId());
        Assertions.assertTrue(function.getInputFieldIds().isEmpty());
        Assertions.assertTrue(function.getOutputFieldIds().isEmpty());
    }

    @Test
    void testFunctionServerAssignedAttributesSetters() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .build();
        function.setId(200L);
        function.setInputFieldIds(Collections.singletonList(101L));
        function.setOutputFieldIds(Collections.singletonList(102L));
        Assertions.assertEquals(200L, function.getId());
        Assertions.assertEquals(Collections.singletonList(101L), function.getInputFieldIds());
        Assertions.assertEquals(Collections.singletonList(102L), function.getOutputFieldIds());
    }

    @Test
    void testFunctionToStringOmitsServerAssignedAttributesWhenEmpty() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .build();
        String str = function.toString();
        Assertions.assertFalse(str.contains("id="));
        Assertions.assertFalse(str.contains("inputFieldIds="));
        Assertions.assertFalse(str.contains("outputFieldIds="));
    }

    @Test
    void testFunctionToStringIncludesServerAssignedAttributesWhenSet() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .build();
        function.setId(200L);
        function.setInputFieldIds(Collections.singletonList(101L));
        function.setOutputFieldIds(Collections.singletonList(102L));
        String str = function.toString();
        Assertions.assertTrue(str.contains("id=200"));
        Assertions.assertTrue(str.contains("inputFieldIds=[101]"));
        Assertions.assertTrue(str.contains("outputFieldIds=[102]"));
    }

    @Test
    void testDescribeCollectionRespSettersRoundTrip() {
        DescribeCollectionResp resp = DescribeCollectionResp.builder()
                .collectionName("test")
                .build();
        resp.setAliases(Collections.singletonList("alias1"));
        resp.setConsistencyLevelName("Bounded");
        resp.setUpdateTimestamp(123456L);
        resp.setEnableDynamicField(Boolean.TRUE);
        resp.setAutoID(Boolean.FALSE);
        resp.setNumOfPartitions(2L);
        resp.setShardsNum(1);
        Assertions.assertEquals(Collections.singletonList("alias1"), resp.getAliases());
        Assertions.assertEquals("Bounded", resp.getConsistencyLevelName());
        Assertions.assertEquals(123456L, resp.getUpdateTimestamp());
        Assertions.assertEquals(Boolean.TRUE, resp.getEnableDynamicField());
        Assertions.assertEquals(Boolean.FALSE, resp.getAutoID());
        Assertions.assertEquals(2L, resp.getNumOfPartitions());
        Assertions.assertEquals(1, resp.getShardsNum());
    }

    @Test
    void testDescribeCollectionRespRemainingSettersRoundTrip() {
        DescribeCollectionResp resp = DescribeCollectionResp.builder().build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        resp.setCollectionName("coll");
        resp.setCollectionID(1L);
        resp.setDatabaseName("db");
        resp.setDescription("desc");
        resp.setFieldNames(Arrays.asList("id", "vector"));
        resp.setVectorFieldNames(Collections.singletonList("vector"));
        resp.setPrimaryFieldName("id");
        resp.setCollectionSchema(schema);
        resp.setConsistencyLevel(io.milvus.v2.common.ConsistencyLevel.BOUNDED);
        resp.setCreateTime(10L);
        resp.setCreateUtcTime(20L);
        Assertions.assertEquals("coll", resp.getCollectionName());
        Assertions.assertEquals(1L, resp.getCollectionID());
        Assertions.assertEquals("db", resp.getDatabaseName());
        Assertions.assertEquals("desc", resp.getDescription());
        Assertions.assertEquals(Arrays.asList("id", "vector"), resp.getFieldNames());
        Assertions.assertEquals(Collections.singletonList("vector"), resp.getVectorFieldNames());
        Assertions.assertEquals("id", resp.getPrimaryFieldName());
        Assertions.assertEquals(schema, resp.getCollectionSchema());
        Assertions.assertEquals(io.milvus.v2.common.ConsistencyLevel.BOUNDED, resp.getConsistencyLevel());
        Assertions.assertEquals(10L, resp.getCreateTime());
        Assertions.assertEquals(20L, resp.getCreateUtcTime());
    }

    @Test
    void testFunctionRegularSettersRoundTrip() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().build();
        function.setName("fn");
        function.setDescription("desc");
        function.setFunctionType(io.milvus.common.clientenum.FunctionType.BM25);
        function.setInputFieldNames(Collections.singletonList("text"));
        function.setOutputFieldNames(Collections.singletonList("sparse"));
        Map<String, String> params = new HashMap<>();
        params.put("k", "60");
        function.setParams(params);
        Assertions.assertEquals("fn", function.getName());
        Assertions.assertEquals("desc", function.getDescription());
        Assertions.assertEquals(io.milvus.common.clientenum.FunctionType.BM25, function.getFunctionType());
        Assertions.assertEquals(Collections.singletonList("text"), function.getInputFieldNames());
        Assertions.assertEquals(Collections.singletonList("sparse"), function.getOutputFieldNames());
        Assertions.assertEquals("60", function.getParams().get("k"));
    }

    @Test
    void testFieldSchemaRegularSettersRoundTrip() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .build();
        fieldSchema.setName("new_id");
        fieldSchema.setDescription("desc");
        fieldSchema.setIsPrimaryKey(Boolean.TRUE);
        fieldSchema.setIsClusteringKey(Boolean.TRUE);
        fieldSchema.setAutoID(Boolean.FALSE);
        fieldSchema.setDefaultValue(5L);
        Assertions.assertEquals("new_id", fieldSchema.getName());
        Assertions.assertEquals("desc", fieldSchema.getDescription());
        Assertions.assertEquals(Boolean.TRUE, fieldSchema.getIsPrimaryKey());
        Assertions.assertEquals(Boolean.TRUE, fieldSchema.getIsClusteringKey());
        Assertions.assertEquals(Boolean.FALSE, fieldSchema.getAutoID());
        Assertions.assertEquals(5L, fieldSchema.getDefaultValue());
    }

    @Test
    void testCreateSchemaStaticHelper() {
        CreateCollectionReq.CollectionSchema schema = client_v2.createSchema();
        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.getFieldSchemaList().isEmpty());
        Assertions.assertFalse(schema.isEnableDynamicField());
    }

    @Test
    void testAddCollectionField() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.addCollectionField(any())).thenReturn(successStatus);
        AddCollectionFieldReq request = AddCollectionFieldReq.builder()
                .collectionName("test")
                .fieldName("extra")
                .dataType(DataType.VarChar)
                .maxLength(128)
                .build();
        client_v2.addCollectionField(request);
        verify(blockingStub).addCollectionField(any());
    }

    @Test
    void testAddCollectionFunction() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.addCollectionFunction(any())).thenReturn(successStatus);
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25_fn")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build();
        AddCollectionFunctionReq request = AddCollectionFunctionReq.builder()
                .collectionName("test")
                .function(function)
                .build();
        client_v2.addCollectionFunction(request);
        verify(blockingStub).addCollectionFunction(any());
    }

    @Test
    void testAlterCollectionFunction() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.alterCollectionFunction(any())).thenReturn(successStatus);
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25_fn")
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build();
        AlterCollectionFunctionReq request = AlterCollectionFunctionReq.builder()
                .collectionName("test")
                .function(function)
                .build();
        client_v2.alterCollectionFunction(request);
        verify(blockingStub).alterCollectionFunction(any());
    }

    @Test
    void testDropCollectionFunction() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.dropCollectionFunction(any())).thenReturn(successStatus);
        DropCollectionFunctionReq request = DropCollectionFunctionReq.builder()
                .collectionName("test")
                .functionName("bm25_fn")
                .build();
        client_v2.dropCollectionFunction(request);
        verify(blockingStub).dropCollectionFunction(any());
    }

    @Test
    void testDropCollectionFieldProperties() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.alterCollectionField(any())).thenReturn(successStatus);
        DropCollectionFieldPropertiesReq request = DropCollectionFieldPropertiesReq.builder()
                .collectionName("test")
                .fieldName("extra")
                .propertyKeys(Collections.singletonList("key"))
                .build();
        client_v2.dropCollectionFieldProperties(request);
        verify(blockingStub).alterCollectionField(any());
    }

    @Test
    void testRefreshLoad() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.loadCollection(any())).thenReturn(successStatus);
        RefreshLoadReq request = RefreshLoadReq.builder()
                .collectionName("test")
                .sync(false)
                .build();
        client_v2.refreshLoad(request);
        verify(blockingStub).loadCollection(any());
    }

    @Test
    void testGetLoadStateV2() {
        GetLoadStateReq request = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        GetLoadStateResp resp = client_v2.getLoadStateV2(request);
        Assertions.assertNotNull(resp);
        Assertions.assertEquals(io.milvus.grpc.LoadState.LoadStateLoaded, resp.getState());
    }

    @Test
    void testDescribeReplicas() {
        io.milvus.grpc.Status successStatus = io.milvus.grpc.Status.newBuilder().setCode(0).build();
        when(blockingStub.getReplicas(any())).thenReturn(io.milvus.grpc.GetReplicasResponse.newBuilder()
                .setStatus(successStatus)
                .build());
        DescribeReplicasReq request = DescribeReplicasReq.builder()
                .collectionName("test")
                .build();
        DescribeReplicasResp resp = client_v2.describeReplicas(request);
        Assertions.assertNotNull(resp);
        Assertions.assertTrue(resp.getReplicas().isEmpty());
    }

}
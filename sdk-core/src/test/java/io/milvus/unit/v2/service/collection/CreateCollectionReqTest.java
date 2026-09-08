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

package io.milvus.unit.v2.service.collection;

import io.milvus.common.clientenum.FunctionType;
import io.milvus.exception.ParamException;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CreateCollectionReqTest {

    private CreateCollectionReq.FieldSchema primaryKey() {
        return CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build();
    }

    // ---------------------------------------------------------------------
    // CreateCollectionReq
    // ---------------------------------------------------------------------

    @Test
    void builderBuildsWithAllFields() {
        List<IndexParam> indexParams = Collections.singletonList(
                IndexParam.builder().fieldName("vector").indexType(IndexParam.IndexType.FLAT).build());
        Map<String, String> properties = Collections.singletonMap("mmap.enabled", "true");
        CreateCollectionReq request = CreateCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .description("desc")
                .dimension(128)
                .primaryFieldName("pk")
                .idType(DataType.VarChar)
                .maxLength(256)
                .vectorFieldName("vec")
                .metricType(IndexParam.MetricType.IP.name())
                .autoID(true)
                .enableDynamicField(true)
                .numShards(2)
                .numPartitions(8)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .indexParams(indexParams)
                .properties(properties)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("desc", request.getDescription());
        assertEquals(128, request.getDimension());
        assertEquals("pk", request.getPrimaryFieldName());
        assertEquals(DataType.VarChar, request.getIdType());
        assertEquals(256, request.getMaxLength());
        assertEquals("vec", request.getVectorFieldName());
        assertEquals(IndexParam.MetricType.IP.name(), request.getMetricType());
        assertTrue(request.getAutoID());
        assertTrue(request.getEnableDynamicField());
        assertEquals(2, request.getNumShards());
        assertEquals(8, request.getNumPartitions());
        assertEquals(ConsistencyLevel.STRONG, request.getConsistencyLevel());
        assertEquals(indexParams, request.getIndexParams());
        assertEquals(properties, request.getProperties());
    }

    @Test
    void defaultsApply() {
        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("coll")
                .build();

        assertEquals("", request.getDescription());
        assertEquals("id", request.getPrimaryFieldName());
        assertEquals(DataType.Int64, request.getIdType());
        assertEquals(65535, request.getMaxLength());
        assertEquals("vector", request.getVectorFieldName());
        assertEquals(IndexParam.MetricType.COSINE.name(), request.getMetricType());
        assertFalse(request.getAutoID());
        assertTrue(request.getEnableDynamicField());
        assertEquals(1, request.getNumShards());
        assertTrue(request.getIndexParams().isEmpty());
        assertNull(request.getNumPartitions());
        assertNull(request.getDimension());
        assertNull(request.getDatabaseName());
        assertEquals(ConsistencyLevel.BOUNDED, request.getConsistencyLevel());
        assertTrue(request.getProperties().isEmpty());
    }

    @Test
    void builderRejectsNullCollectionName() {
        assertThrows(IllegalArgumentException.class,
                () -> CreateCollectionReq.builder().collectionName(null));
        assertThrows(IllegalArgumentException.class,
                () -> CreateCollectionReq.builder().build());
    }

    @Test
    void setterRejectsNullCollectionName() {
        CreateCollectionReq request = CreateCollectionReq.builder().collectionName("coll").build();
        assertThrows(IllegalArgumentException.class, () -> request.setCollectionName(null));
    }

    @Test
    void settersUpdateGetters() {
        CreateCollectionReq request = CreateCollectionReq.builder().collectionName("coll").build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDescription("d2");
        assertEquals("d2", request.getDescription());

        request.setDimension(64);
        assertEquals(64, request.getDimension());

        request.setPrimaryFieldName("pk2");
        assertEquals("pk2", request.getPrimaryFieldName());

        request.setIdType(DataType.VarChar);
        assertEquals(DataType.VarChar, request.getIdType());

        request.setMaxLength(128);
        assertEquals(128, request.getMaxLength());

        request.setVectorFieldName("vec2");
        assertEquals("vec2", request.getVectorFieldName());

        request.setMetricType("L2");
        assertEquals("L2", request.getMetricType());

        request.setAutoID(true);
        assertTrue(request.getAutoID());

        request.setEnableDynamicField(false);
        assertFalse(request.getEnableDynamicField());

        request.setNumShards(4);
        assertEquals(4, request.getNumShards());

        request.setNumPartitions(16);
        assertEquals(16, request.getNumPartitions());

        request.setConsistencyLevel(ConsistencyLevel.EVENTUALLY);
        assertEquals(ConsistencyLevel.EVENTUALLY, request.getConsistencyLevel());

        List<IndexParam> params = Collections.singletonList(
                IndexParam.builder().fieldName("f").indexType(IndexParam.IndexType.IVF_FLAT).build());
        request.setIndexParams(params);
        assertEquals(params, request.getIndexParams());
    }

    @Test
    void indexParamAppendsToIndexParams() {
        IndexParam first = IndexParam.builder().fieldName("a").build();
        IndexParam second = IndexParam.builder().fieldName("b").build();
        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("coll")
                .indexParam(first)
                .indexParam(second)
                .build();
        assertEquals(2, request.getIndexParams().size());
        assertEquals(first, request.getIndexParams().get(0));
        assertEquals(second, request.getIndexParams().get(1));
    }

    @Test
    void propertyAddsSingleEntry() {
        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("coll")
                .property("a", "1")
                .property("b", "2")
                .build();
        assertEquals("1", request.getProperties().get("a"));
        assertEquals("2", request.getProperties().get("b"));
    }

    @Test
    void propertiesAreCopiedNotShared() {
        Map<String, String> properties = new java.util.HashMap<>();
        properties.put("k", "v");
        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("coll")
                .properties(properties)
                .build();
        properties.put("k2", "v2");
        assertEquals(1, request.getProperties().size());
        request.getProperties().put("k3", "v3");
        assertFalse(properties.containsKey("k3"));
    }

    @Test
    void enableDynamicFieldConflictsWithSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(false)
                .build();
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .enableDynamicField(true)
                .collectionSchema(schema));
        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionSchema(schema)
                .enableDynamicField(true));
    }

    // ---------------------------------------------------------------------
    // CollectionSchema
    // ---------------------------------------------------------------------

    @Test
    void collectionSchemaBuilderBuildsWithAllFields() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(primaryKey()))
                .structFields(Collections.emptyList())
                .enableDynamicField(true)
                .functionList(Collections.emptyList())
                .externalSource("")
                .build();

        assertEquals(1, schema.getFieldSchemaList().size());
        assertTrue(schema.isEnableDynamicField());
        assertTrue(schema.getStructFields().isEmpty());
        assertTrue(schema.getFunctionList().isEmpty());
        assertEquals("", schema.getExternalSource());
        assertNull(schema.getExternalSpec());
    }

    @Test
    void collectionSchemaSettersUpdateGetters() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.setFieldSchemaList(Collections.singletonList(primaryKey()));
        assertEquals(1, schema.getFieldSchemaList().size());

        schema.setEnableDynamicField(true);
        assertTrue(schema.isEnableDynamicField());

        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().name("fn").build();
        schema.setFunctionList(Collections.singletonList(function));
        assertEquals(1, schema.getFunctionList().size());

        schema.setExternalSource("ext");
        assertEquals("ext", schema.getExternalSource());

        com.google.gson.JsonObject spec = new com.google.gson.JsonObject();
        spec.addProperty("bucket", "b");
        schema.setExternalSpec(spec);
        assertEquals(spec, schema.getExternalSpec());

        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();
        schema.setStructFields(Collections.singletonList(struct));
        assertEquals(1, schema.getStructFields().size());
    }

    @Test
    void collectionSchemaAddFieldRoutesStructFields() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();

        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("chunks")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(20)
                .structFields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("content")
                        .dataType(DataType.Text)
                        .build()))
                .build());

        assertEquals(1, schema.getFieldSchemaList().size());
        assertEquals(1, schema.getStructFields().size());
        assertEquals("chunks", schema.getStructFields().get(0).getName());
    }

    @Test
    void collectionSchemaGetFieldAndGetStructField() {
        CreateCollectionReq.FieldSchema idField = primaryKey();
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(idField))
                .structFields(Collections.singletonList(struct))
                .build();

        assertEquals(idField, schema.getField("id"));
        assertNull(schema.getField("missing"));
        assertEquals(struct, schema.getStructField("chunks"));
        assertNull(schema.getStructField("missing"));
    }

    @Test
    void collectionSchemaAddFunction() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().name("fn").build();
        schema.addFunction(function);
        assertEquals(1, schema.getFunctionList().size());
        assertEquals(function, schema.getFunctionList().get(0));
    }

    @Test
    void collectionSchemaVerifyRejectsMissingPrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("other")
                                .dataType(DataType.VarChar)
                                .build()))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyRejectsMultiplePrimaryKeys() {
        CreateCollectionReq.FieldSchema fieldA = CreateCollectionReq.FieldSchema.builder()
                .name("a").dataType(DataType.Int64).isPrimaryKey(true).build();
        CreateCollectionReq.FieldSchema fieldB = CreateCollectionReq.FieldSchema.builder()
                .name("b").dataType(DataType.Int64).isPrimaryKey(true).build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(fieldA, fieldB))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyRejectsInvalidPrimaryKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("pk").dataType(DataType.Float).isPrimaryKey(true).build()))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyRejectsAutoIdOnNonPrimary() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("pk").dataType(DataType.Int64).isPrimaryKey(true).build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("other").dataType(DataType.VarChar).autoID(true).build()))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyRejectsPartitionKeyEqualsPrimary() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("pk").dataType(DataType.Int64)
                                .isPrimaryKey(true).isPartitionKey(true).build()))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyRejectsUnsupportedClusteringKeyType() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("pk").dataType(DataType.Int64).isPrimaryKey(true).build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("ck").dataType(DataType.BinaryVector).isClusteringKey(true).build()))
                .build();
        assertThrows(MilvusClientException.class, schema::verify);
    }

    @Test
    void collectionSchemaVerifyPassesForValidSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("pk").dataType(DataType.Int64).isPrimaryKey(true).autoID(true).build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("vec").dataType(DataType.FloatVector).dimension(8).build()))
                .build();
        schema.verify();
    }

    @Test
    void collectionSchemaVerifySkipsExternalSource() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .externalSource("s3")
                .build();
        schema.verify();
    }

    // ---------------------------------------------------------------------
    // FieldSchema
    // ---------------------------------------------------------------------

    @Test
    void fieldSchemaBuilderBuildsWithAllFields() {
        Map<String, String> typeParams = Collections.singletonMap("enable_analyzer", "true");
        CreateCollectionReq.FieldSchema field = CreateCollectionReq.FieldSchema.builder()
                .name("text")
                .description("text field")
                .dataType(DataType.VarChar)
                .maxLength(1024)
                .dimension(0)
                .isPrimaryKey(true)
                .isPartitionKey(false)
                .isClusteringKey(true)
                .autoID(true)
                .elementType(null)
                .maxCapacity(10)
                .isNullable(true)
                .defaultValue("dv")
                .enableAnalyzer(true)
                .analyzerParams(Collections.singletonMap("tokenizer", "standard"))
                .enableMatch(true)
                .typeParams(typeParams)
                .multiAnalyzerParams(Collections.singletonMap("m", 2))
                .externalField("ext")
                .build();

        assertEquals("text", field.getName());
        assertEquals("text field", field.getDescription());
        assertEquals(DataType.VarChar, field.getDataType());
        assertEquals(1024, field.getMaxLength());
        assertTrue(field.getIsPrimaryKey());
        assertFalse(field.getIsPartitionKey());
        assertTrue(field.getIsClusteringKey());
        assertTrue(field.getAutoID());
        assertEquals(10, field.getMaxCapacity());
        assertTrue(field.getIsNullable());
        assertEquals("dv", field.getDefaultValue());
        assertTrue(field.getEnableAnalyzer());
        assertEquals(Collections.singletonMap("tokenizer", "standard"), field.getAnalyzerParams());
        assertTrue(field.getEnableMatch());
        assertEquals(typeParams, field.getTypeParams());
        assertEquals(Collections.singletonMap("m", 2), field.getMultiAnalyzerParams());
        assertEquals("ext", field.getExternalField());
    }

    @Test
    void fieldSchemaDefaultsApply() {
        CreateCollectionReq.FieldSchema field = CreateCollectionReq.FieldSchema.builder()
                .name("f")
                .dataType(DataType.Int64)
                .build();

        assertEquals("", field.getDescription());
        assertEquals(65535, field.getMaxLength());
        assertFalse(field.getIsPrimaryKey());
        assertFalse(field.getIsPartitionKey());
        assertFalse(field.getIsClusteringKey());
        assertFalse(field.getAutoID());
        assertFalse(field.getIsNullable());
        assertNull(field.getDefaultValue());
        assertNull(field.getDimension());
        assertNull(field.getElementType());
        assertNull(field.getMaxCapacity());
        assertNull(field.getEnableAnalyzer());
        assertNull(field.getEnableMatch());
        assertEquals("", field.getExternalField());
    }

    @Test
    void fieldSchemaSettersUpdateGetters() {
        CreateCollectionReq.FieldSchema field = CreateCollectionReq.FieldSchema.builder().build();

        field.setName("f2");
        assertEquals("f2", field.getName());

        field.setDescription("d");
        assertEquals("d", field.getDescription());

        field.setDataType(DataType.FloatVector);
        assertEquals(DataType.FloatVector, field.getDataType());

        field.setMaxLength(100);
        assertEquals(100, field.getMaxLength());

        field.setDimension(8);
        assertEquals(8, field.getDimension());

        field.setIsPrimaryKey(true);
        assertTrue(field.getIsPrimaryKey());

        field.setIsPartitionKey(true);
        assertTrue(field.getIsPartitionKey());

        field.setIsClusteringKey(true);
        assertTrue(field.getIsClusteringKey());

        field.setAutoID(true);
        assertTrue(field.getAutoID());

        field.setElementType(DataType.VarChar);
        assertEquals(DataType.VarChar, field.getElementType());

        field.setMaxCapacity(5);
        assertEquals(5, field.getMaxCapacity());

        field.setIsNullable(true);
        assertTrue(field.getIsNullable());

        field.setDefaultValue("x");
        assertEquals("x", field.getDefaultValue());

        field.setEnableAnalyzer(true);
        assertTrue(field.getEnableAnalyzer());

        field.setEnableMatch(true);
        assertTrue(field.getEnableMatch());

        field.setTypeParams(Collections.singletonMap("k", "v"));
        assertEquals(Collections.singletonMap("k", "v"), field.getTypeParams());

        field.setMultiAnalyzerParams(Collections.singletonMap("m", 1));
        assertEquals(Collections.singletonMap("m", 1), field.getMultiAnalyzerParams());

        field.setExternalField("ext");
        assertEquals("ext", field.getExternalField());

        field.setFieldId(99L);
        assertEquals(99L, field.getFieldId());

        field.setIsDynamic(true);
        assertTrue(field.getIsDynamic());

        field.setIsFunctionOutput(true);
        assertTrue(field.getIsFunctionOutput());

        List<Map<String, Object>> indexes = Collections.singletonList(Collections.singletonMap("type", "FLAT"));
        field.setIndexes(indexes);
        assertEquals(indexes, field.getIndexes());
    }

    @Test
    void fieldSchemaBuilderFactory() {
        assertNotNull(CreateCollectionReq.FieldSchema.builder());
    }

    // ---------------------------------------------------------------------
    // Function
    // ---------------------------------------------------------------------

    @Test
    void functionBuilderBuildsWithAllFields() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("bm25")
                .description("bm25 fn")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Arrays.asList("text"))
                .outputFieldNames(Arrays.asList("sparse"))
                .params(Collections.singletonMap("k1", "1.2"))
                .build();

        assertEquals("bm25", function.getName());
        assertEquals("bm25 fn", function.getDescription());
        assertEquals(FunctionType.BM25, function.getFunctionType());
        assertEquals(Arrays.asList("text"), function.getInputFieldNames());
        assertEquals(Arrays.asList("sparse"), function.getOutputFieldNames());
        assertEquals(Collections.singletonMap("k1", "1.2"), function.getParams());
    }

    @Test
    void functionParamAddsSingleEntry() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("fn")
                .param("a", "1")
                .param("b", "2")
                .build();
        assertEquals("1", function.getParams().get("a"));
        assertEquals("2", function.getParams().get("b"));
    }

    @Test
    void functionDefaultsApply() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().build();
        assertEquals("", function.getName());
        assertEquals("", function.getDescription());
        assertEquals(FunctionType.UNKNOWN, function.getFunctionType());
        assertTrue(function.getInputFieldNames().isEmpty());
        assertTrue(function.getOutputFieldNames().isEmpty());
        assertTrue(function.getParams().isEmpty());
        assertNull(function.getId());
        assertTrue(function.getInputFieldIds().isEmpty());
        assertTrue(function.getOutputFieldIds().isEmpty());
    }

    @Test
    void functionSettersUpdateGetters() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().build();

        function.setName("f2");
        assertEquals("f2", function.getName());

        function.setDescription("d");
        assertEquals("d", function.getDescription());

        function.setFunctionType(FunctionType.BM25);
        assertEquals(FunctionType.BM25, function.getFunctionType());

        function.setInputFieldNames(Arrays.asList("in1", "in2"));
        assertEquals(Arrays.asList("in1", "in2"), function.getInputFieldNames());

        function.setOutputFieldNames(Arrays.asList("out1"));
        assertEquals(Arrays.asList("out1"), function.getOutputFieldNames());

        function.setParams(Collections.singletonMap("k", "v"));
        assertEquals(Collections.singletonMap("k", "v"), function.getParams());

        function.setId(5L);
        assertEquals(5L, function.getId());

        function.setInputFieldIds(Arrays.asList(1L, 2L));
        assertEquals(Arrays.asList(1L, 2L), function.getInputFieldIds());

        function.setOutputFieldIds(Arrays.asList(3L));
        assertEquals(Arrays.asList(3L), function.getOutputFieldIds());
    }

    @Test
    void functionBuilderFactory() {
        assertNotNull(CreateCollectionReq.Function.builder());
    }

    // ---------------------------------------------------------------------
    // StructFieldSchema
    // ---------------------------------------------------------------------

    @Test
    void structFieldSchemaBuilderBuildsWithAllFields() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .description("chunk list")
                .fields(Collections.singletonList(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("content")
                                .dataType(DataType.Text)
                                .build()))
                .maxCapacity(20)
                .nullable(true)
                .typeParams(Collections.singletonMap("k", "v"))
                .build();

        assertEquals("chunks", struct.getName());
        assertEquals("chunk list", struct.getDescription());
        assertEquals(1, struct.getFields().size());
        assertEquals(20, struct.getMaxCapacity());
        assertTrue(struct.getNullable());
        assertEquals(Collections.singletonMap("k", "v"), struct.getTypeParams());
        assertEquals(DataType.Array, struct.getDataType());
        assertEquals(DataType.Struct, struct.getElementType());
    }

    @Test
    void structFieldSchemaDefaultsApply() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();

        assertEquals("", struct.getDescription());
        assertTrue(struct.getFields().isEmpty());
        assertNull(struct.getMaxCapacity());
        assertFalse(struct.getNullable());
        assertTrue(struct.getTypeParams().isEmpty());
    }

    @Test
    void structFieldSchemaSettersNormalizeNullable() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();

        struct.setName("chunks2");
        assertEquals("chunks2", struct.getName());

        struct.setDescription("d");
        assertEquals("d", struct.getDescription());

        struct.setMaxCapacity(30);
        assertEquals(30, struct.getMaxCapacity());

        struct.setNullable(true);
        assertTrue(struct.getNullable());

        struct.setTypeParams(Collections.singletonMap("k", "v"));
        assertEquals(Collections.singletonMap("k", "v"), struct.getTypeParams());

        CreateCollectionReq.FieldSchema field = CreateCollectionReq.FieldSchema.builder()
                .name("content")
                .dataType(DataType.Text)
                .build();
        struct.setFields(Collections.singletonList(field));
        assertEquals(Collections.singletonList(field), struct.getFields());

        struct.setNullable(null);
        assertFalse(struct.getNullable());
    }

    @Test
    void structFieldSchemaTypeParamAddsSingleEntry() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .typeParam("a", "1")
                .typeParam("b", "2")
                .build();
        assertEquals("1", struct.getTypeParams().get("a"));
        assertEquals("2", struct.getTypeParams().get("b"));
    }

    @Test
    void structFieldSchemaAddFieldAddsSubField() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();
        struct.addField(AddFieldReq.builder()
                .fieldName("content")
                .dataType(DataType.Text)
                .build());
        assertEquals(1, struct.getFields().size());
        assertEquals("content", struct.getFields().get(0).getName());
    }

    @Test
    void structFieldSchemaAddFieldRejectsForbiddenAttributes() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .build();

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("nested")
                .dataType(DataType.Array)
                .elementType(DataType.Int64)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("part")
                .dataType(DataType.VarChar)
                .isPartitionKey(true)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("clust")
                .dataType(DataType.VarChar)
                .isClusteringKey(true)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("auto")
                .dataType(DataType.Int64)
                .autoID(true)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("null")
                .dataType(DataType.VarChar)
                .isNullable(true)
                .build()));

        assertThrows(ParamException.class, () -> struct.addField(AddFieldReq.builder()
                .fieldName("dv")
                .dataType(DataType.VarChar)
                .defaultValue("x")
                .build()));
    }

    @Test
    void structFieldSchemaBuilderFactory() {
        assertNotNull(CreateCollectionReq.StructFieldSchema.builder());
    }

    @Test
    void toStringContainsFields() {
        CreateCollectionReq request = CreateCollectionReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Collections.singletonList(primaryKey()))
                .build();
        assertTrue(schema.toString().contains("id"));

        CreateCollectionReq.FieldSchema field = CreateCollectionReq.FieldSchema.builder().name("f").build();
        assertTrue(field.toString().contains("f"));

        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder().name("fn").build();
        assertTrue(function.toString().contains("fn"));

        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks").build();
        assertTrue(struct.toString().contains("chunks"));
    }
}

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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.SchemaUtils;
import io.milvus.v2.utils.ConvertUtils;

import io.milvus.grpc.BatchDescribeCollectionResponse;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.ConsistencyLevel;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.ElementIndices;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.FunctionSchema;
import io.milvus.grpc.FunctionType;
import io.milvus.grpc.IDs;
import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.IndexState;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.Status;
import io.milvus.grpc.StructArrayFieldSchema;
import io.milvus.param.Constant;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

@Tag("unit")
public class ConvertUtilsTest {

    @Test
    void testToProtoDataTypeAndToSdkDataType() {
        Assertions.assertEquals(DataType.Int64, ConvertUtils.toProtoDataType(io.milvus.v2.common.DataType.Int64));
        Assertions.assertEquals(DataType.FloatVector, ConvertUtils.toProtoDataType(io.milvus.v2.common.DataType.FloatVector));
        Assertions.assertEquals(DataType.None, ConvertUtils.toProtoDataType(null));

        Assertions.assertEquals(io.milvus.v2.common.DataType.Int64, ConvertUtils.toSdkDataType(DataType.Int64));
        Assertions.assertEquals(io.milvus.v2.common.DataType.FloatVector, ConvertUtils.toSdkDataType(DataType.FloatVector));
        Assertions.assertEquals(io.milvus.v2.common.DataType.None, ConvertUtils.toSdkDataType(null));
    }

    @Test
    void testGetEntitiesFromQueryResults() {
        ConvertUtils convertUtils = new ConvertUtils();

        FieldData countField = FieldData.newBuilder()
                .setFieldName("count(*)")
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder()
                        .setLongData(LongArray.newBuilder().addData(42L).build())
                        .build())
                .build();
        QueryResults countResponse = QueryResults.newBuilder()
                .addFieldsData(countField)
                .build();
        List<QueryResp.QueryResult> countEntities = convertUtils.getEntities(countResponse);
        Assertions.assertEquals(1, countEntities.size());
        Assertions.assertEquals(42L, countEntities.get(0).getEntity().get("count(*)"));

        FieldData idField = FieldData.newBuilder()
                .setFieldName("id")
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder()
                        .setLongData(LongArray.newBuilder().addData(1L).addData(2L).build())
                        .build())
                .build();
        QueryResults normalResponse = QueryResults.newBuilder()
                .addOutputFields("id")
                .addFieldsData(idField)
                .build();
        List<QueryResp.QueryResult> entities = convertUtils.getEntities(normalResponse);
        Assertions.assertEquals(2, entities.size());
        Assertions.assertEquals(1L, entities.get(0).getEntity().get("id"));
        Assertions.assertEquals(2L, entities.get(1).getEntity().get("id"));
    }

    @Test
    void testGetEntitiesFromQueryResultsWithElementIndices() {
        ConvertUtils convertUtils = new ConvertUtils();
        FieldData idField = FieldData.newBuilder()
                .setFieldName("id")
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder()
                        .setLongData(LongArray.newBuilder().addData(1L).build())
                        .build())
                .build();
        QueryResults response = QueryResults.newBuilder()
                .addOutputFields("id")
                .addFieldsData(idField)
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).addData(1L).build())
                        .build())
                .build();
        List<QueryResp.QueryResult> entities = convertUtils.getEntities(response);
        Assertions.assertEquals(2, entities.size());
        Assertions.assertEquals(0L, entities.get(0).getElementOffset());
        Assertions.assertEquals(1L, entities.get(1).getElementOffset());

        QueryResults mismatch = QueryResults.newBuilder()
                .addOutputFields("id")
                .addFieldsData(FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(ScalarField.newBuilder()
                                .setLongData(LongArray.newBuilder().addData(1L).addData(2L).build())
                                .build())
                        .build())
                .addElementIndices(ElementIndices.newBuilder()
                        .setIndices(LongArray.newBuilder().addData(0L).build())
                        .build())
                .build();
        Assertions.assertThrows(MilvusClientException.class, () -> convertUtils.getEntities(mismatch));
    }

    @Test
    void testGetEntitiesFromSearchResults() {
        ConvertUtils convertUtils = new ConvertUtils();

        FieldData idField = FieldData.newBuilder()
                .setFieldName("id")
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder()
                        .setLongData(LongArray.newBuilder().addData(7L).build())
                        .build())
                .build();
        SearchResultData data = SearchResultData.newBuilder()
                .setNumQueries(1)
                .setTopK(1)
                .addTopks(1L)
                .addOutputFields("id")
                .addFieldsData(idField)
                .addScores(0.95f)
                .setIds(IDs.newBuilder()
                        .setIntId(LongArray.newBuilder().addData(7L).build())
                        .build())
                .build();
        SearchResults response = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setResults(data)
                .build();

        List<List<SearchResp.SearchResult>> results = convertUtils.getEntities(response);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(1, results.get(0).size());
        Assertions.assertEquals(0.95f, results.get(0).get(0).getScore());
        Assertions.assertEquals(7L, results.get(0).get(0).getId());
        Assertions.assertEquals(7L, results.get(0).get(0).getEntity().get("id"));
    }

    @Test
    void testConvertDescCollectionsResp() {
        ConvertUtils convertUtils = new ConvertUtils();
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .build();
        CollectionSchema schema = CollectionSchema.newBuilder()
                .addFields(idField)
                .build();
        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionName("c1")
                .setCollectionID(1L)
                .setDbName("default")
                .setSchema(schema)
                .setNumPartitions(1)
                .setCreatedTimestamp(0L)
                .setCreatedUtcTimestamp(0L)
                .setConsistencyLevel(ConsistencyLevel.Bounded)
                .setShardsNum(1)
                .build();
        BatchDescribeCollectionResponse batch = BatchDescribeCollectionResponse.newBuilder()
                .addResponses(response)
                .build();

        List<DescribeCollectionResp> resp = convertUtils.convertDescCollectionsResp(batch);
        Assertions.assertEquals(1, resp.size());
        Assertions.assertEquals("c1", resp.get(0).getCollectionName());
    }

    @Test
    void testConvertToDescribeIndexResp() {
        ConvertUtils convertUtils = new ConvertUtils();

        KeyValuePair indexType = KeyValuePair.newBuilder()
                .setKey(Constant.INDEX_TYPE).setValue("HNSW").build();
        KeyValuePair metricType = KeyValuePair.newBuilder()
                .setKey(Constant.METRIC_TYPE).setValue("COSINE").build();
        IndexDescription description = IndexDescription.newBuilder()
                .setIndexName("idx")
                .setFieldName("vector")
                .setIndexID(9L)
                .setState(IndexState.Finished)
                .addParams(indexType)
                .addParams(metricType)
                .build();

        DescribeIndexResp resp = convertUtils.convertToDescribeIndexResp(
                Collections.singletonList(description));
        Assertions.assertEquals(1, resp.getIndexDescriptions().size());
        DescribeIndexResp.IndexDesc desc = resp.getIndexDescriptions().get(0);
        Assertions.assertEquals("idx", desc.getIndexName());
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.COSINE, desc.getMetricType());
    }

    @Test
    void testConvertDescCollectionRespFieldNamesIncludeStructFields() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .build();

        FieldSchema vectorField = FieldSchema.newBuilder()
                .setName("vector")
                .setDataType(DataType.FloatVector)
                .build();

        CreateCollectionReq.StructFieldSchema structFieldSchema = CreateCollectionReq.StructFieldSchema.builder()
                .name("clips")
                .maxCapacity(10)
                .build();
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("vec")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(8)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("bin_vec")
                .dataType(io.milvus.v2.common.DataType.BinaryVector)
                .dimension(64)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("f16_vec")
                .dataType(io.milvus.v2.common.DataType.Float16Vector)
                .dimension(16)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("bf16_vec")
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .dimension(16)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("i8_vec")
                .dataType(io.milvus.v2.common.DataType.Int8Vector)
                .dimension(16)
                .build());

        StructArrayFieldSchema rpcStructFieldSchema = SchemaUtils.convertToGrpcStructFieldSchema(structFieldSchema);

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setEnableDynamicField(false)
                .addFields(idField)
                .addFields(vectorField)
                .addStructArrayFields(rpcStructFieldSchema)
                .build();

        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionName("test")
                .setCollectionID(1L)
                .setDbName("default")
                .setSchema(schema)
                .setNumPartitions(1)
                .setCreatedTimestamp(0L)
                .setCreatedUtcTimestamp(0L)
                .setConsistencyLevel(ConsistencyLevel.Bounded)
                .setShardsNum(1)
                .build();

        DescribeCollectionResp resp = new ConvertUtils().convertDescCollectionResp(response);
        Assertions.assertTrue(resp.getFieldNames().contains("id"));
        Assertions.assertTrue(resp.getFieldNames().contains("vector"));
        Assertions.assertTrue(resp.getFieldNames().contains("clips"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("vector"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[bin_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[f16_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[bf16_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[i8_vec]"));
    }

    @Test
    void testConvertDescCollectionRespExposesAliasesUpdateTimestampAndSchemaIds() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .setFieldID(1L)
                .build();
        FieldSchema embeddingField = FieldSchema.newBuilder()
                .setName("embedding")
                .setDataType(DataType.FloatVector)
                .setFieldID(2L)
                .setIsFunctionOutput(true)
                .build();
        FieldSchema dynamicField = FieldSchema.newBuilder()
                .setName("$meta")
                .setDataType(DataType.JSON)
                .setIsDynamic(true)
                .setFieldID(3L)
                .build();
        FunctionSchema functionSchema = FunctionSchema.newBuilder()
                .setName("bm25")
                .setType(FunctionType.BM25)
                .setId(7L)
                .addInputFieldIds(1L)
                .addOutputFieldIds(2L)
                .build();
        CollectionSchema schema = CollectionSchema.newBuilder()
                .setEnableDynamicField(true)
                .setEnableNamespace(true)
                .setVersion(42)
                .addFields(idField)
                .addFields(embeddingField)
                .addFields(dynamicField)
                .addFunctions(functionSchema)
                .build();

        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionName("test")
                .setCollectionID(1L)
                .setDbName("default")
                .setSchema(schema)
                .setNumPartitions(1)
                .setCreatedTimestamp(0L)
                .setCreatedUtcTimestamp(0L)
                .setConsistencyLevel(ConsistencyLevel.Bounded)
                .setShardsNum(1)
                .addAliases("test_alias")
                .setUpdateTimestamp(123456L)
                .build();

        DescribeCollectionResp resp = new ConvertUtils().convertDescCollectionResp(response);

        Assertions.assertEquals(java.util.Collections.singletonList("test_alias"), resp.getAliases());
        Assertions.assertEquals(123456L, resp.getUpdateTimestamp());
        Assertions.assertEquals(Boolean.TRUE, resp.getEnableNamespace());
        Assertions.assertEquals(42, resp.getSchemaVersion());

        CreateCollectionReq.FieldSchema idFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("id")).findFirst().orElseThrow(() -> new AssertionError("id field not found"));
        Assertions.assertEquals(1L, idFieldResp.getFieldId());
        CreateCollectionReq.FieldSchema dynamicFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("$meta")).findFirst().orElseThrow(() -> new AssertionError("$meta field not found"));
        Assertions.assertEquals(Boolean.TRUE, dynamicFieldResp.getIsDynamic());
        CreateCollectionReq.FieldSchema embeddingFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("embedding")).findFirst().orElseThrow(() -> new AssertionError("embedding field not found"));
        Assertions.assertEquals(Boolean.TRUE, embeddingFieldResp.getIsFunctionOutput());

        CreateCollectionReq.Function functionResp = resp.getCollectionSchema().getFunctionList().get(0);
        Assertions.assertEquals(7L, functionResp.getId());
        Assertions.assertEquals(java.util.Collections.singletonList(1L), functionResp.getInputFieldIds());
        Assertions.assertEquals(java.util.Collections.singletonList(2L), functionResp.getOutputFieldIds());
    }
}

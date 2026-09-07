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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.DataUtils;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class DmlDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testDeleteUpsert() {
        String randomCollectionName = generator.generate(10);

        // create a new db
        String testDbName = "test_delete_db";
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(testDbName)
                .build());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(4)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(1024)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String, Object>() {{
                    put("nlist", 64);
                }})
                .build());
        // create collection in the test db
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("pk", "pk_" + i);
            row.addProperty("text", "desc_" + i);
            row.add("float_vector", JsonUtils.toJsonTree(new float[]{(float) i, (float) (i + 1), (float) (i + 2), (float) (i + 3)}));
            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(10, insertResp.getInsertCnt());

        // delete
        DeleteResp deleteResp = client.delete(DeleteReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .ids(Arrays.asList("pk_5", "pk_8"))
                .build());
        Assertions.assertEquals(2, deleteResp.getDeleteCnt());

        // get row count
        long rowCount = getRowCount(testDbName, randomCollectionName);
        Assertions.assertEquals(8L, rowCount);

        // upsert
        // id=5 and id=8 has been deleted, need to provide all fields
        {
            JsonObject row1 = new JsonObject();
            row1.addProperty("pk", "pk_5");
            row1.addProperty("text", "updated_5");
            row1.add("float_vector", JsonUtils.toJsonTree(new float[]{5.0f, 5.0f, 5.0f, 5.0f}));

            JsonObject row2 = new JsonObject();
            row2.addProperty("pk", "pk_8");
            row2.addProperty("text", "updated_8");
            row2.add("float_vector", JsonUtils.toJsonTree(new float[]{5.0f, 5.0f, 5.0f, 5.0f}));

            UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                    .databaseName(testDbName)
                    .collectionName(randomCollectionName)
                    .data(Arrays.asList(row1, row2))
                    .build());
            Assertions.assertEquals(2, upsertResp.getUpsertCnt());
            Assertions.assertEquals(2, upsertResp.getPrimaryKeys().size());
        }
        // id=2 is a partial update, "text" old value is reused
        {
            JsonObject row = new JsonObject();
            row.addProperty("pk", "pk_2");
            row.add("float_vector", JsonUtils.toJsonTree(new float[]{5.0f, 5.0f, 5.0f, 5.0f}));

            UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                    .databaseName(testDbName)
                    .collectionName(randomCollectionName)
                    .data(Collections.singletonList(row))
                    .partialUpdate(true)
                    .build());
            Assertions.assertEquals(1, upsertResp.getUpsertCnt());
            Assertions.assertEquals(1, upsertResp.getPrimaryKeys().size());
        }

        // get row count
        rowCount = getRowCount(testDbName, randomCollectionName);
        Assertions.assertEquals(10L, rowCount);

        // verify
        QueryResp queryResp = client.query(QueryReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .ids(Arrays.asList("pk_2", "pk_5", "pk_8"))
                .outputFields(Collections.singletonList("*"))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(3, queryResults.size());

        {
            QueryResp.QueryResult result = queryResults.get(0);
            Map<String, Object> entity = result.getEntity();
            Assertions.assertTrue(entity.containsKey("pk"));
            Assertions.assertEquals("pk_2", entity.get("pk"));
            Assertions.assertEquals("desc_2", entity.get("text"));
            Assertions.assertTrue(entity.containsKey("float_vector"));
            Assertions.assertInstanceOf(List.class, entity.get("float_vector"));
            List<Float> vector1 = (List<Float>) entity.get("float_vector");
            for (Float f : vector1) {
                Assertions.assertEquals(5.0f, f);
            }
        }

        {
            QueryResp.QueryResult result = queryResults.get(1);
            Map<String, Object> entity = result.getEntity();
            Assertions.assertTrue(entity.containsKey("pk"));
            Assertions.assertEquals("pk_5", entity.get("pk"));
            Assertions.assertEquals("updated_5", entity.get("text"));
            Assertions.assertTrue(entity.containsKey("float_vector"));
            Assertions.assertInstanceOf(List.class, entity.get("float_vector"));
            List<Float> vector2 = (List<Float>) entity.get("float_vector");
            for (Float f : vector2) {
                Assertions.assertEquals(5.0f, f);
            }
        }

        client.dropCollection(DropCollectionReq.builder()
                .databaseName(testDbName)
                .collectionName(randomCollectionName)
                .build());
    }

    @Test
    void testInsertUpsertValidation() {
        String autoIdCollection = "validation_auto_" + generator.generate(8);
        String schemaCollection = "validation_schema_" + generator.generate(8);

        try {
            CreateCollectionReq.CollectionSchema autoIdSchema = CreateCollectionReq.CollectionSchema.builder()
                    .enableDynamicField(true)
                    .build();
            autoIdSchema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(true)
                    .build());
            autoIdSchema.addField(AddFieldReq.builder()
                    .fieldName("vector")
                    .dataType(DataType.FloatVector)
                    .dimension(2)
                    .build());
            autoIdSchema.addField(AddFieldReq.builder()
                    .fieldName("tag")
                    .dataType(DataType.VarChar)
                    .maxLength(100)
                    .build());
            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(autoIdCollection)
                    .collectionSchema(autoIdSchema)
                    .indexParams(Collections.singletonList(IndexParam.builder()
                            .fieldName("vector")
                            .indexType(IndexParam.IndexType.FLAT)
                            .metricType(IndexParam.MetricType.L2)
                            .build()))
                    .property("allow_insert_auto_id", "true")
                    .build());
            client.loadCollection(LoadCollectionReq.builder()
                    .collectionName(autoIdCollection)
                    .sync(true)
                    .build());

            // Insert excludes an auto-ID field when it is not provided.
            JsonObject insertWithoutId = new JsonObject();
            insertWithoutId.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
            insertWithoutId.addProperty("tag", "generated-id");
            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(autoIdCollection)
                    .data(Collections.singletonList(insertWithoutId))
                    .build());
            Assertions.assertEquals(1, insertResp.getInsertCnt());

            // Insert accepts an explicitly provided auto-ID field.
            JsonObject insertWithId = new JsonObject();
            insertWithId.addProperty("id", 100L);
            insertWithId.add("vector", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
            insertWithId.addProperty("tag", "explicit-id");
            insertResp = client.insert(InsertReq.builder()
                    .collectionName(autoIdCollection)
                    .data(Collections.singletonList(insertWithId))
                    .build());
            Assertions.assertEquals(1, insertResp.getInsertCnt());

            // Explicit auto-ID must be supplied for every row or omitted from every row, independent of row order.
            List<List<JsonObject>> mixedAutoIdBatches = Arrays.asList(
                    Arrays.asList(insertWithoutId, insertWithId),
                    Arrays.asList(insertWithId, insertWithoutId));
            for (List<JsonObject> batch : mixedAutoIdBatches) {
                MilvusClientException mixedAutoId = Assertions.assertThrows(MilvusClientException.class,
                        () -> client.insert(InsertReq.builder()
                                .collectionName(autoIdCollection)
                                .data(batch)
                                .build()));
                Assertions.assertEquals(ErrorCode.INVALID_PARAMS, mixedAutoId.getErrorCode());
                Assertions.assertTrue(mixedAutoId.getMessage().contains("all rows"));
                Assertions.assertTrue(mixedAutoId.getMessage().contains("id"));
            }

            // Full upsert includes the auto-ID field in its required input fields.
            JsonObject fullUpsertWithoutId = new JsonObject();
            fullUpsertWithoutId.add("vector", JsonUtils.toJsonTree(Arrays.asList(5.0f, 6.0f)));
            fullUpsertWithoutId.addProperty("tag", "missing-id");
            MilvusClientException missingAutoId = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.upsert(UpsertReq.builder()
                            .collectionName(autoIdCollection)
                            .data(Collections.singletonList(fullUpsertWithoutId))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, missingAutoId.getErrorCode());
            Assertions.assertTrue(missingAutoId.getMessage().contains("id"));

            // Partial upsert does not require non-primary fields that are absent from the row.
            JsonObject partialUpsert = new JsonObject();
            partialUpsert.addProperty("id", 100L);
            partialUpsert.add("vector", JsonUtils.toJsonTree(Arrays.asList(7.0f, 8.0f)));
            UpsertResp partialUpsertResp = client.upsert(UpsertReq.builder()
                    .collectionName(autoIdCollection)
                    .data(Collections.singletonList(partialUpsert))
                    .partialUpdate(true)
                    .build());
            Assertions.assertEquals(1, partialUpsertResp.getUpsertCnt());

            CreateCollectionReq.CollectionSchema validationSchema = CreateCollectionReq.CollectionSchema.builder()
                    .build();
            validationSchema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .build());
            validationSchema.addField(AddFieldReq.builder()
                    .fieldName("vector")
                    .dataType(DataType.FloatVector)
                    .dimension(2)
                    .build());
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            validationSchema.addField(AddFieldReq.builder()
                    .fieldName("text")
                    .dataType(DataType.VarChar)
                    .maxLength(100)
                    .enableAnalyzer(true)
                    .analyzerParams(analyzerParams)
                    .build());
            validationSchema.addField(AddFieldReq.builder()
                    .fieldName("sparse")
                    .dataType(DataType.SparseFloatVector)
                    .build());
            validationSchema.addField(AddFieldReq.builder()
                    .fieldName("metadata")
                    .dataType(DataType.Array)
                    .elementType(DataType.Struct)
                    .maxCapacity(10)
                    .addStructField(AddFieldReq.builder()
                            .fieldName("score")
                            .dataType(DataType.Float)
                            .build())
                    .build());
            validationSchema.addFunction(CreateCollectionReq.Function.builder()
                    .name("bm25")
                    .functionType(FunctionType.BM25)
                    .inputFieldNames(Collections.singletonList("text"))
                    .outputFieldNames(Collections.singletonList("sparse"))
                    .build());
            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(schemaCollection)
                    .collectionSchema(validationSchema)
                    .indexParams(Arrays.asList(
                            IndexParam.builder()
                                    .fieldName("vector")
                                    .indexType(IndexParam.IndexType.FLAT)
                                    .metricType(IndexParam.MetricType.L2)
                                    .build(),
                            IndexParam.builder()
                                    .fieldName("sparse")
                                    .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                                    .metricType(IndexParam.MetricType.BM25)
                                    .build()))
                    .build());
            client.loadCollection(LoadCollectionReq.builder()
                    .collectionName(schemaCollection)
                    .sync(true)
                    .build());
            DescribeCollectionResp validationDesc = client.describeCollection(DescribeCollectionReq.builder()
                    .collectionName(schemaCollection)
                    .build());

            JsonObject baseRow = new JsonObject();
            baseRow.addProperty("id", 1L);
            baseRow.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
            baseRow.addProperty("text", "base text");
            JsonObject metadata = new JsonObject();
            metadata.addProperty("score", 1.0f);
            JsonArray metadataArray = new JsonArray();
            metadataArray.add(metadata);
            baseRow.add("metadata", metadataArray);
            insertResp = client.insert(InsertReq.builder()
                    .collectionName(schemaCollection)
                    .data(Collections.singletonList(baseRow))
                    .build());
            Assertions.assertEquals(1, insertResp.getInsertCnt());

            // Function output fields are generated by Milvus and cannot be supplied by users.
            JsonObject functionOutputRow = baseRow.deepCopy();
            functionOutputRow.add("sparse", new JsonObject());
            MilvusClientException functionOutput = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.insert(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(functionOutputRow))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, functionOutput.getErrorCode());
            Assertions.assertTrue(functionOutput.getMessage().contains("sparse"));

            // Unknown fields are rejected when dynamic fields are disabled.
            JsonObject unknownFieldRow = baseRow.deepCopy();
            unknownFieldRow.addProperty("unknown", "value");
            MilvusClientException unknownField = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.insert(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(unknownFieldRow))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, unknownField.getErrorCode());
            Assertions.assertTrue(unknownField.getMessage().contains("unknown"));

            // Insert and full upsert reject an omitted struct field before sending the request.
            JsonObject fullWithoutStruct = new JsonObject();
            fullWithoutStruct.addProperty("id", 2L);
            fullWithoutStruct.add("vector", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
            fullWithoutStruct.addProperty("text", "without struct");
            MilvusClientException insertWithoutStruct = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.insert(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(fullWithoutStruct))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, insertWithoutStruct.getErrorCode());
            Assertions.assertTrue(insertWithoutStruct.getMessage().contains("metadata"));

            MilvusClientException fullUpsertWithoutStruct = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.upsert(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(fullWithoutStruct))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, fullUpsertWithoutStruct.getErrorCode());
            Assertions.assertTrue(fullUpsertWithoutStruct.getMessage().contains("metadata"));

            // Struct elements must contain exactly the configured subfields.
            JsonObject unexpectedStructFieldRow = baseRow.deepCopy();
            unexpectedStructFieldRow.getAsJsonArray("metadata").get(0).getAsJsonObject()
                    .addProperty("extra", "value");
            MilvusClientException unexpectedStructField = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.insert(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(unexpectedStructFieldRow))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, unexpectedStructField.getErrorCode());
            Assertions.assertTrue(unexpectedStructField.getMessage().contains("unexpected fields"));

            // Null struct subfields are rejected even if normal fields may support nullable values.
            JsonObject nullStructFieldRow = baseRow.deepCopy();
            nullStructFieldRow.getAsJsonArray("metadata").get(0).getAsJsonObject()
                    .add("score", JsonNull.INSTANCE);
            MilvusClientException nullStructField = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.insert(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(nullStructFieldRow))
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, nullStructField.getErrorCode());
            Assertions.assertTrue(nullStructField.getMessage().contains("cannot be null"));

            // A whole struct field can be omitted during partial upsert.
            JsonObject partialWithoutStruct = new JsonObject();
            partialWithoutStruct.addProperty("id", 1L);
            partialWithoutStruct.add("vector", JsonUtils.toJsonTree(Arrays.asList(9.0f, 10.0f)));
            io.milvus.grpc.UpsertRequest partialWithoutStructRequest =
                    new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(partialWithoutStruct))
                            .partialUpdate(true)
                            .build(), validationDesc);
            Assertions.assertTrue(partialWithoutStructRequest.getPartialUpdate());
            Assertions.assertEquals(2, partialWithoutStructRequest.getFieldsDataCount());
            Set<String> partialFieldNames = new HashSet<>();
            partialWithoutStructRequest.getFieldsDataList()
                    .forEach(fieldData -> partialFieldNames.add(fieldData.getFieldName()));
            Assertions.assertEquals(new HashSet<>(Arrays.asList("id", "vector")), partialFieldNames);

            // Every supplied field must have the same number of values in a partial-update batch.
            JsonObject firstPartialRow = new JsonObject();
            firstPartialRow.addProperty("id", 1L);
            firstPartialRow.add("vector", JsonUtils.toJsonTree(Arrays.asList(11.0f, 12.0f)));
            JsonObject secondPartialRow = new JsonObject();
            secondPartialRow.addProperty("id", 2L);
            MilvusClientException inconsistentFields = Assertions.assertThrows(MilvusClientException.class,
                    () -> client.upsert(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Arrays.asList(firstPartialRow, secondPartialRow))
                            .partialUpdate(true)
                            .build()));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, inconsistentFields.getErrorCode());

            // A common nonzero count is still invalid when it is shorter than the batch size.
            JsonObject populatedPartialRow = new JsonObject();
            populatedPartialRow.addProperty("id", 1L);
            populatedPartialRow.add("vector", JsonUtils.toJsonTree(Arrays.asList(13.0f, 14.0f)));
            JsonObject partialRowWithoutVector = new JsonObject();
            partialRowWithoutVector.addProperty("id", 2L);
            List<List<JsonObject>> shortFieldBatches = Arrays.asList(
                    Arrays.asList(populatedPartialRow, partialRowWithoutVector),
                    Arrays.asList(partialRowWithoutVector, populatedPartialRow));
            for (List<JsonObject> batch : shortFieldBatches) {
                MilvusClientException shortFieldCount = Assertions.assertThrows(MilvusClientException.class,
                        () -> client.upsert(UpsertReq.builder()
                                .collectionName(schemaCollection)
                                .data(batch)
                                .partialUpdate(true)
                                .build()));
                Assertions.assertEquals(ErrorCode.INVALID_PARAMS, shortFieldCount.getErrorCode());
                Assertions.assertTrue(shortFieldCount.getMessage().contains("number of values"));
            }

            // With dynamic fields enabled, upsert still rejects struct[subfield] storage syntax.
            validationDesc.getCollectionSchema().setEnableDynamicField(true);
            JsonObject fullStructSubField = baseRow.deepCopy();
            fullStructSubField.add("metadata[score]", JsonUtils.toJsonTree(Collections.singletonList(1.0f)));
            MilvusClientException fullStructSubFieldError = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(fullStructSubField))
                            .build(), validationDesc));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, fullStructSubFieldError.getErrorCode());
            Assertions.assertTrue(fullStructSubFieldError.getMessage().contains("top-level field"));

            JsonObject partialStructSubField = new JsonObject();
            partialStructSubField.addProperty("id", 1L);
            partialStructSubField.add("metadata[score]", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
            MilvusClientException partialStructSubFieldError = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(partialStructSubField))
                            .partialUpdate(true)
                            .build(), validationDesc));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, partialStructSubFieldError.getErrorCode());
            Assertions.assertTrue(partialStructSubFieldError.getMessage().contains("Partial struct update"));

            // Insert treats struct[subfield] as dynamic, and upsert does the same for a short name.
            JsonObject dynamicInsertRow = baseRow.deepCopy();
            dynamicInsertRow.add("metadata[score]", JsonUtils.toJsonTree(Collections.singletonList(1.0f)));
            io.milvus.grpc.InsertRequest dynamicInsertRequest =
                    new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(InsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(dynamicInsertRow))
                            .build(), validationDesc);
            Assertions.assertTrue(dynamicInsertRequest.getFieldsData(
                    dynamicInsertRequest.getFieldsDataCount() - 1).getIsDynamic());

            JsonObject dynamicShortNameRow = new JsonObject();
            dynamicShortNameRow.addProperty("id", 1L);
            dynamicShortNameRow.addProperty("score", "dynamic-value");
            io.milvus.grpc.UpsertRequest dynamicShortNameRequest =
                    new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(UpsertReq.builder()
                            .collectionName(schemaCollection)
                            .data(Collections.singletonList(dynamicShortNameRow))
                            .partialUpdate(true)
                            .build(), validationDesc);
            Assertions.assertEquals(2, dynamicShortNameRequest.getFieldsDataCount());
            Assertions.assertTrue(dynamicShortNameRequest.getFieldsData(1).getIsDynamic());
        } finally {
            if (client.hasCollection(HasCollectionReq.builder().collectionName(autoIdCollection).build())) {
                client.dropCollection(DropCollectionReq.builder().collectionName(autoIdCollection).build());
            }
            if (client.hasCollection(HasCollectionReq.builder().collectionName(schemaCollection).build())) {
                client.dropCollection(DropCollectionReq.builder().collectionName(schemaCollection).build());
            }
        }
    }

    @Test
    void testMultiThreadsInsert() {
        String randomCollectionName = generator.generate(10);
        int dim = 64;

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .maxLength(65535)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(dim)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("dataTime")
                .dataType(DataType.Int64)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        try {
            Random rand = new Random();
            List<Thread> threadList = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        List<JsonObject> rows = new ArrayList<>();
                        int cnt = rand.nextInt(100) + 100;
                        for (int j = 0; j < cnt; j++) {
                            JsonObject obj = new JsonObject();
                            obj.addProperty("id", String.format("%d", i * cnt + j));
                            List<Float> vector = utils.generateFloatVector(dim);
                            obj.add("vector", JsonUtils.toJsonTree(vector));
                            obj.addProperty("dataTime", System.currentTimeMillis());
                            rows.add(obj);
                        }

                        client.insert(InsertReq.builder()
                                .collectionName(randomCollectionName)
                                .data(rows)
                                .build());
                    }
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }
            System.out.println("Multi-thread insert done");

            QueryResp queryResp = client.query(QueryReq.builder()
                    .filter("")
                    .collectionName(randomCollectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println(queryResp.getQueryResults().get(0).getEntity().get("count(*)"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }

        try {
            Random rand = new Random();
            List<Thread> threadList = new ArrayList<>();
            for (int k = 0; k < 10; k++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        List<JsonObject> rows = new ArrayList<>();
                        int cnt = rand.nextInt(100) + 100;
                        for (int j = 0; j < cnt; j++) {
                            JsonObject obj = new JsonObject();
                            obj.addProperty("id", String.format("%d", i * cnt + j));
                            List<Float> vector = utils.generateFloatVector(dim);
                            obj.add("vector", JsonUtils.toJsonTree(vector));
                            obj.addProperty("dataTime", System.currentTimeMillis());
                            rows.add(obj);
                        }

                        UpsertReq upsertReq = UpsertReq.builder()
                                .collectionName(randomCollectionName)
                                .data(rows)
                                .build();
                        client.upsert(upsertReq);
                    }
                });
                t.start();
                threadList.add(t);
            }

            for (Thread t : threadList) {
                t.join();
            }
            System.out.println("Multi-thread upsert done");

            QueryResp queryResp = client.query(QueryReq.builder()
                    .filter("")
                    .collectionName(randomCollectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println(queryResp.getQueryResults().get(0).getEntity().get("count(*)"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }

}

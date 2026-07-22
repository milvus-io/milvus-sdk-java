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

package io.milvus.v2.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.DataNotMatchException;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

class DataUtilsTest {

    @Test
    void testInsertAllowsMissingAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(null, true, false);

        InsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        Assertions.assertEquals(Collections.singletonList("vector"), fieldNames(request.getFieldsDataList()));
    }

    @Test
    void testInsertAcceptsProvidedAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(1L, true, false);

        InsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        Assertions.assertEquals(Arrays.asList("vector", "id"), fieldNames(request.getFieldsDataList()));
    }

    @Test
    void testInsertRejectsMixedAutoIdPresenceRegardlessOfRowOrder() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject rowWithoutId = row(null, true, false);
        JsonObject rowWithId = row(1L, true, false);

        List<List<JsonObject>> batches = Arrays.asList(
                Arrays.asList(rowWithoutId, rowWithId),
                Arrays.asList(rowWithId, rowWithoutId));
        for (List<JsonObject> batch : batches) {
            MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                            InsertReq.builder().collectionName("test").data(batch).build(), collection));

            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
            Assertions.assertTrue(exception.getMessage().contains("all rows"));
            Assertions.assertTrue(exception.getMessage().contains("id"));
        }
    }

    @Test
    void testFullUpsertRequiresAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(null, true, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testPartialUpsertRequiresAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(null, true, false);

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("primary key"));
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testPartialUpsertRequiresNonAutoIdPrimaryKey() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(null, true, false);

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("primary key"));
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testRejectsFunctionOutputField() {
        DescribeCollectionResp collection = describeCollection(false, true, false);
        JsonObject row = row(1L, true, true);

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("embedding"));
    }

    @Test
    void testRejectsUnknownFieldWhenDynamicFieldIsDisabled() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(1L, true, false);
        row.addProperty("unknown", "value");

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }

    @Test
    void testFieldValidationErrorIsDataMismatch() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(1L, true, false);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f, 3.0f)));

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("dimension"));
    }

    @Test
    void testNumericConversionErrorIsDataMismatch() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(null, true, false);
        row.addProperty("id", "abc");

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertInstanceOf(NumberFormatException.class, exception.getCause());
    }

    @Test
    void testPartitionNameIsSerializedForPartitionKeySchema() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        collection.getCollectionSchema().getFieldSchemaList().get(0).setIsPartitionKey(true);
        JsonObject row = row(1L, true, false);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder()
                        .collectionName("test")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("partition", insert.getPartitionName());

        UpsertRequest upsert = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("partition", upsert.getPartitionName());
    }

    @Test
    void testNullRowIsNotDataMismatch() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        List<JsonObject> rows = Collections.singletonList((JsonObject) null);

        MilvusClientException insertException = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(rows).build(), collection));
        Assertions.assertFalse(insertException instanceof DataNotMatchException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, insertException.getErrorCode());
        Assertions.assertTrue(insertException.getMessage().contains("cannot be null"));

        MilvusClientException upsertException = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(rows).build(), collection));
        Assertions.assertFalse(upsertException instanceof DataNotMatchException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, upsertException.getErrorCode());
        Assertions.assertTrue(upsertException.getMessage().contains("cannot be null"));
    }

    @Test
    void testPartialUpsertAllowsOmittedStructField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList("id", "vector"), fieldNames(request.getFieldsDataList()));
    }

    @Test
    void testPartialUpsertRejectsInconsistentFieldCounts() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject firstRow = row(1L, true, false);
        JsonObject secondRow = row(2L, false, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Arrays.asList(firstRow, secondRow))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testPartialUpsertRejectsFieldCountsShorterThanBatchRegardlessOfRowOrder() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject populatedRow = row(1L, true, false);
        JsonObject rowWithoutVector = new JsonObject();
        rowWithoutVector.addProperty("id", 2L);

        List<List<JsonObject>> batches = Arrays.asList(
                Arrays.asList(populatedRow, rowWithoutVector),
                Arrays.asList(rowWithoutVector, populatedRow));
        for (List<JsonObject> batch : batches) {
            MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                            UpsertReq.builder()
                                    .collectionName("test")
                                    .data(batch)
                                    .partialUpdate(true)
                                    .build(),
                            collection));

            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
            Assertions.assertTrue(exception.getMessage().contains("number of values"));
        }
    }

    @Test
    void testInsertRejectsMissingStructField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("metadata"));
    }

    @Test
    void testFullUpsertRejectsMissingStructField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("metadata"));
    }

    @Test
    void testRejectsUnexpectedStructSubField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.addProperty("score", 1.0f);
        struct.addProperty("extra", "value");
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("unexpected fields"));
    }

    @Test
    void testRejectsNullStructSubFieldEvenWhenNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().getStructFields().get(0).getFields().get(0).setIsNullable(true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.add("score", JsonNull.INSTANCE);
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be null"));
    }

    @Test
    void testFullUpsertRejectsStructStorageSubFieldWithDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = row(1L, true, false);
        row.add("metadata[score]", JsonUtils.toJsonTree(Collections.singletonList(1.0f)));

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be used as a top-level field"));
    }

    @Test
    void testPartialUpsertRejectsStructStorageSubFieldWithDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("metadata[score]", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Partial struct update is unsupported"));
    }

    @Test
    void testInsertTreatsStructStorageSubFieldAsDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.addProperty("score", 1.0f);
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);
        row.add("metadata[score]", JsonUtils.toJsonTree(Collections.singletonList(1.0f)));

        InsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        assertFieldNamesIgnoringOrder(Arrays.asList("id", "vector", "metadata", Constant.DYNAMIC_FIELD_NAME),
                request.getFieldsDataList());
        Assertions.assertTrue(request.getFieldsDataList().stream()
                .anyMatch(io.milvus.grpc.FieldData::getIsDynamic));
    }

    @Test
    void testPartialUpsertTreatsShortStructSubFieldNameAsDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.addProperty("score", "dynamic-value");

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList("id", Constant.DYNAMIC_FIELD_NAME),
                fieldNames(request.getFieldsDataList()));
        Assertions.assertTrue(request.getFieldsData(1).getIsDynamic());
    }

    @Test
    void testConvertGrpcDeleteRequest() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("min_id", 10L);
        templateValues.put("tags", Arrays.asList("a", "b"));
        DeleteRequest request = new DataUtils().ConvertToGrpcDeleteRequest(DeleteReq.builder()
                .databaseName("db")
                .collectionName("collection")
                .partitionName("partition")
                .filter("id > {min_id} and tag in {tags}")
                .filterTemplateValues(templateValues)
                .build());

        Assertions.assertEquals("db", request.getDbName());
        Assertions.assertEquals("collection", request.getCollectionName());
        Assertions.assertEquals("partition", request.getPartitionName());
        Assertions.assertEquals("id > {min_id} and tag in {tags}", request.getExpr());
        Assertions.assertEquals(new HashSet<>(Arrays.asList("min_id", "tags")),
                request.getExprTemplateValuesMap().keySet());
    }

    @Test
    void testInsertAndUpsertPropagateDatabaseAndPartitionNames() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(1L, true, false);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder()
                        .databaseName("db")
                        .collectionName("collection")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("db", insert.getDbName());
        Assertions.assertEquals("partition", insert.getPartitionName());

        UpsertRequest upsert = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .databaseName("db")
                        .collectionName("collection")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("db", upsert.getDbName());
        Assertions.assertEquals("partition", upsert.getPartitionName());
    }

    @Test
    void testFieldOperationConversion() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = row(1L, true, false);
        row.add("values", JsonUtils.toJsonTree(Arrays.asList(1L, 2L)));
        List<UpsertReq.FieldPartialUpdateOp> operations = Arrays.asList(
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)
                        .build(),
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                        .build(),
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_REMOVE)
                        .build());

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .fieldOps(operations)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList(
                        FieldPartialUpdateOp.OpType.REPLACE,
                        FieldPartialUpdateOp.OpType.ARRAY_APPEND,
                        FieldPartialUpdateOp.OpType.ARRAY_REMOVE),
                request.getFieldOpsList().stream().map(FieldPartialUpdateOp::getOp).collect(Collectors.toList()));
    }

    @Test
    void testNonReplaceFieldOperationEnablesPartialUpdate() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));

        for (UpsertReq.FieldPartialUpdateOp.OpType opType : Arrays.asList(
                UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND,
                UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_REMOVE)) {
            UpsertReq upsertReq = UpsertReq.builder()
                    .collectionName("test")
                    .data(Collections.singletonList(row))
                    .fieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                            .fieldName("values")
                            .opType(opType)
                            .build()))
                    .build();
            Assertions.assertTrue(upsertReq.isPartialUpdate(), "Expected builder promotion for " + opType);

            UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                    upsertReq,
                    collection);

            Assertions.assertTrue(request.getPartialUpdate(), "Expected partial update for " + opType);
            Assertions.assertEquals(Arrays.asList("id", "values"), fieldNames(request.getFieldsDataList()));
        }
    }

    @Test
    void testReplaceOnlyFieldOperationFollowsPartialUpdateFlag() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        UpsertReq.FieldPartialUpdateOp replace = UpsertReq.FieldPartialUpdateOp.builder()
                .fieldName("values")
                .opType(UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)
                .build();

        JsonObject fullRow = row(1L, true, false);
        fullRow.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq fullUpsertReq = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(fullRow))
                .fieldOps(Collections.singletonList(replace))
                .build();
        Assertions.assertFalse(fullUpsertReq.isPartialUpdate());
        UpsertRequest fullRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                fullUpsertReq,
                collection);
        Assertions.assertFalse(fullRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "vector", "values"),
                fieldNames(fullRequest.getFieldsDataList()));

        JsonObject partialRow = new JsonObject();
        partialRow.addProperty("id", 1L);
        partialRow.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq partialUpsertReq = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(partialRow))
                .partialUpdate(true)
                .fieldOps(Collections.singletonList(replace))
                .build();
        Assertions.assertTrue(partialUpsertReq.isPartialUpdate());
        UpsertRequest partialRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                partialUpsertReq,
                collection);
        Assertions.assertTrue(partialRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "values"),
                fieldNames(partialRequest.getFieldsDataList()));
    }

    @Test
    void testPostBuildFieldOperationMutationPreservesPartialUpdateInvariant() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();

        request.setFieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                .fieldName("values")
                .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                .build()));
        Assertions.assertTrue(request.isPartialUpdate());

        request.setPartialUpdate(false);
        Assertions.assertTrue(request.isPartialUpdate());

        UpsertRequest grpcRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                request,
                collection);
        Assertions.assertTrue(grpcRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "values"), fieldNames(grpcRequest.getFieldsDataList()));
    }

    @Test
    void testRejectsInvalidFieldOperations() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(1L, true, false);

        List<UpsertReq.FieldPartialUpdateOp> invalidOperations = Arrays.asList(
                null,
                UpsertReq.FieldPartialUpdateOp.builder().fieldName("").build(),
                UpsertReq.FieldPartialUpdateOp.builder().fieldName("vector").opType(null).build());
        for (UpsertReq.FieldPartialUpdateOp operation : invalidOperations) {
            MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                            UpsertReq.builder()
                                    .collectionName("test")
                                    .data(Collections.singletonList(row))
                                    .fieldOps(Collections.singletonList(operation))
                                    .build(),
                            collection));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        }
    }

    @Test
    void testGenVectorArray() {
        List<List<List<Float>>> rows = Collections.singletonList(Arrays.asList(
                Arrays.asList(1.0f, 2.0f),
                Arrays.asList(3.0f, 4.0f)));

        VectorArray array = DataUtils.genVectorArray(io.milvus.grpc.DataType.FloatVector, rows, 2);

        Assertions.assertEquals(io.milvus.grpc.DataType.FloatVector, array.getElementType());
        Assertions.assertEquals(2, array.getDim());
        Assertions.assertEquals(1, array.getDataCount());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f),
                array.getData(0).getFloatVector().getDataList());
    }

    @Test
    void testInsertStructVectorSubField() {
        DescribeCollectionResp collection = describeCollectionWithStructVector();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        JsonArray metadata = new JsonArray();
        JsonObject first = new JsonObject();
        first.add("embedding", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        metadata.add(first);
        JsonObject second = new JsonObject();
        second.add("embedding", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
        metadata.add(second);
        row.add("metadata", metadata);

        InsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        FieldData metadataField = request.getFieldsData(1);
        Assertions.assertEquals("metadata", metadataField.getFieldName());
        FieldData embedding = metadataField.getStructArrays().getFields(0);
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, embedding.getType());
        Assertions.assertEquals(2, embedding.getVectors().getVectorArray().getDim());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f),
                embedding.getVectors().getVectorArray().getData(0).getFloatVector().getDataList());
    }

    @Test
    void testInsertBinarySparseJsonAndArrayFields() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).build());
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("binary").dataType(DataType.BinaryVector).dimension(16).build());
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("sparse").dataType(DataType.SparseFloatVector).build());
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("json").dataType(DataType.JSON).build());
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("values").dataType(DataType.Array).elementType(DataType.Int64).maxCapacity(4).build());
        DescribeCollectionResp collection = DescribeCollectionResp.builder()
                .collectionName("test").collectionSchema(schema).build();

        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(1L, 0.5f);
        JsonObject json = new JsonObject();
        json.addProperty("key", "value");
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("binary", JsonUtils.toJsonTree(new byte[]{1, 2}));
        row.add("sparse", JsonUtils.toJsonTree(sparse));
        row.add("json", json);
        row.add("values", JsonUtils.toJsonTree(Arrays.asList(10L, 20L)));

        InsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        Assertions.assertEquals(Arrays.asList("id", "binary", "sparse", "json", "values"),
                fieldNames(request.getFieldsDataList()));
        Assertions.assertEquals(2, request.getFieldsData(1).getVectors().getBinaryVector().size());
        Assertions.assertEquals(1, request.getFieldsData(2).getVectors().getSparseFloatVector().getContentsCount());
        Assertions.assertEquals(1, request.getFieldsData(3).getScalars().getJsonData().getDataCount());
        Assertions.assertEquals(1, request.getFieldsData(4).getScalars().getArrayData().getDataCount());
    }

    private static DescribeCollectionResp describeCollection(boolean autoId, boolean withFunctionOutput,
                                                               boolean withStructField) {
        CreateCollectionReq.FieldSchema id = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(autoId)
                .build();
        CreateCollectionReq.FieldSchema vector = CreateCollectionReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .build();

        CreateCollectionReq.CollectionSchema.CollectionSchemaBuilder schemaBuilder =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Arrays.asList(id, vector))
                        .enableDynamicField(false);

        if (withFunctionOutput) {
            CreateCollectionReq.FieldSchema embedding = CreateCollectionReq.FieldSchema.builder()
                    .name("embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(2)
                    .build();
            schemaBuilder.fieldSchemaList(Arrays.asList(id, vector, embedding));
            schemaBuilder.functionList(Collections.singletonList(
                    CreateCollectionReq.Function.builder()
                            .outputFieldNames(Collections.singletonList("embedding"))
                            .build()));
        }

        if (withStructField) {
            CreateCollectionReq.StructFieldSchema metadata = CreateCollectionReq.StructFieldSchema.builder()
                    .name("metadata")
                    .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                            .name("score")
                            .dataType(DataType.Float)
                            .build()))
                    .maxCapacity(10)
                    .build();
            schemaBuilder.structFields(Collections.singletonList(metadata));
        }

        return DescribeCollectionResp.builder()
                .collectionName("test")
                .collectionSchema(schemaBuilder.build())
                .build();
    }

    private static DescribeCollectionResp describeCollectionWithArrayField() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        List<CreateCollectionReq.FieldSchema> fields =
                new ArrayList<>(collection.getCollectionSchema().getFieldSchemaList());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("values")
                .dataType(DataType.Array)
                .elementType(DataType.Int64)
                .maxCapacity(10)
                .build());
        collection.getCollectionSchema().setFieldSchemaList(fields);
        return collection;
    }

    private static DescribeCollectionResp describeCollectionWithStructVector() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).build());
        schema.getStructFields().add(CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("embedding")
                        .dataType(DataType.FloatVector)
                        .dimension(2)
                        .build()))
                .maxCapacity(10)
                .build());
        return DescribeCollectionResp.builder().collectionName("test").collectionSchema(schema).build();
    }

    private static JsonObject row(Long id, boolean withVector, boolean withEmbedding) {
        JsonObject row = new JsonObject();
        if (id != null) {
            row.addProperty("id", id);
        }
        if (withVector) {
            row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        }
        if (withEmbedding) {
            row.add("embedding", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
        }
        return row;
    }

    private static List<String> fieldNames(List<io.milvus.grpc.FieldData> fieldsData) {
        return fieldsData.stream().map(io.milvus.grpc.FieldData::getFieldName).collect(Collectors.toList());
    }

    private static void assertFieldNamesIgnoringOrder(List<String> expected,
                                                      List<io.milvus.grpc.FieldData> fieldsData) {
        Assertions.assertEquals(expected.size(), fieldsData.size());
        Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(fieldNames(fieldsData)));
    }
}

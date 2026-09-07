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
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.SchemaUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import io.milvus.exception.ParamException;
import io.milvus.grpc.ArrayArray;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.IDs;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.StringArray;
import io.milvus.grpc.StructArrayField;
import io.milvus.grpc.StructArrayFieldSchema;
import io.milvus.grpc.VectorField;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.FieldType;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddCollectionStructFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tag("unit")
class TextDataTypeTest {

    @Test
    void convertsTextSchemaWithoutVarcharLengthConstraint() {
        CreateCollectionReq.FieldSchema textField = CreateCollectionReq.FieldSchema.builder()
                .name("body")
                .dataType(DataType.Text)
                .maxLength(1)
                .defaultValue("long default text")
                .enableAnalyzer(true)
                .build();

        FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(textField);

        Assertions.assertEquals(io.milvus.grpc.DataType.Text, grpcField.getDataType());
        Assertions.assertEquals("long default text", grpcField.getDefaultValue().getStringData());
        Assertions.assertTrue(grpcField.getTypeParamsList().stream()
                .noneMatch(pair -> pair.getKey().equals("max_length")));
        Assertions.assertEquals(DataType.Text, SchemaUtils.convertFromGrpcFieldSchema(grpcField).getDataType());
    }

    @Test
    void convertsTextArrayAndStructSubField() {
        CreateCollectionReq.FieldSchema textArray = CreateCollectionReq.FieldSchema.builder()
                .name("paragraphs")
                .dataType(DataType.Array)
                .elementType(DataType.Text)
                .maxCapacity(10)
                .maxLength(1)
                .build();

        FieldSchema grpcArray = SchemaUtils.convertToGrpcFieldSchema(textArray);
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, grpcArray.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.Text, grpcArray.getElementType());
        Assertions.assertTrue(grpcArray.getTypeParamsList().stream()
                .noneMatch(pair -> pair.getKey().equals("max_length")));

        CreateCollectionReq.StructFieldSchema structField = CreateCollectionReq.StructFieldSchema.builder()
                .name("chunks")
                .maxCapacity(20)
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("content")
                        .dataType(DataType.Text)
                        .build()))
                .build();

        StructArrayFieldSchema grpcStruct = SchemaUtils.convertToGrpcStructFieldSchema(structField);
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, grpcStruct.getFields(0).getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.Text, grpcStruct.getFields(0).getElementType());

        AddCollectionStructFieldReq addStructFieldReq = AddCollectionStructFieldReq.builder()
                .collectionName("collection")
                .fieldName("chunks")
                .maxCapacity(20)
                .addStructField(AddFieldReq.builder()
                        .fieldName("content")
                        .dataType(DataType.Text)
                        .build())
                .build();
        StructArrayFieldSchema grpcAddStruct = SchemaUtils.convertToGrpcStructFieldSchema(
                addStructFieldReq.toStructFieldSchema());
        Assertions.assertEquals(io.milvus.grpc.DataType.Text, grpcAddStruct.getFields(0).getElementType());
    }

    @Test
    void acceptsAndDecodesTextWithoutMaxLengthValidation() {
        String longText = String.join("", Collections.nCopies(1024, "text"));
        CreateCollectionReq.FieldSchema textField = CreateCollectionReq.FieldSchema.builder()
                .name("body")
                .dataType(DataType.Text)
                .maxLength(1)
                .build();

        Assertions.assertEquals(longText, DataUtils.checkFieldValue(textField, new JsonPrimitive(longText)));
        Assertions.assertThrows(ParamException.class,
                () -> DataUtils.checkFieldValue(textField, new JsonPrimitive(10)));

        ScalarField scalarField = ParamUtils.genScalarField(
                io.milvus.grpc.DataType.Text,
                io.milvus.grpc.DataType.None,
                Arrays.asList("first", "second"));
        FieldData fieldData = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Text)
                .setFieldName("body")
                .setScalars(scalarField)
                .build();

        FieldDataWrapper wrapper = new FieldDataWrapper(fieldData);
        Assertions.assertEquals(2, wrapper.getRowCount());
        Assertions.assertEquals(Arrays.asList("first", "second"), wrapper.getFieldData());
    }

    @Test
    void decodesArrayOfText() {
        ScalarField row = ScalarField.newBuilder()
                .setStringData(StringArray.newBuilder().addData("first").addData("second"))
                .build();
        FieldData fieldData = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Array)
                .setFieldName("paragraphs")
                .setScalars(ScalarField.newBuilder().setArrayData(ArrayArray.newBuilder()
                        .setElementType(io.milvus.grpc.DataType.Text)
                        .addData(row)))
                .build();

        Assertions.assertEquals(Collections.singletonList(Arrays.asList("first", "second")),
                new FieldDataWrapper(fieldData).getFieldData());
    }

    @Test
    void supportsTextInV1FieldAndArrayValidation() {
        String longText = String.join("", Collections.nCopies(1024, "text"));
        FieldType textField = FieldType.newBuilder()
                .withName("body")
                .withDataType(io.milvus.grpc.DataType.Text)
                .build();
        ParamUtils.checkFieldData(textField, Collections.singletonList(longText), false);

        FieldType textArray = FieldType.newBuilder()
                .withName("paragraphs")
                .withDataType(io.milvus.grpc.DataType.Array)
                .withElementType(io.milvus.grpc.DataType.Text)
                .withMaxCapacity(10)
                .build();
        ParamUtils.checkFieldData(textArray,
                Collections.singletonList(Arrays.asList(longText, "second")), false);
    }

    @Test
    void validatesEveryJsonTextArrayElement() {
        JsonArray valid = new JsonArray();
        valid.add("first");
        valid.add("second");
        Assertions.assertEquals(Arrays.asList("first", "second"),
                ParamUtils.convertJsonArray(valid, io.milvus.grpc.DataType.Text, "paragraphs"));

        JsonArray number = new JsonArray();
        number.add("first");
        number.add(10);
        Assertions.assertThrows(ParamException.class,
                () -> ParamUtils.convertJsonArray(number, io.milvus.grpc.DataType.Text, "paragraphs"));

        JsonArray bool = new JsonArray();
        bool.add("first");
        bool.add(true);
        Assertions.assertThrows(ParamException.class,
                () -> ParamUtils.convertJsonArray(bool, io.milvus.grpc.DataType.Text, "paragraphs"));

        JsonArray nullable = new JsonArray();
        nullable.add("first");
        nullable.add((String) null);
        Assertions.assertThrows(ParamException.class,
                () -> ParamUtils.convertJsonArray(nullable, io.milvus.grpc.DataType.Text, "paragraphs"));
    }

    @Test
    void decodesTextFromQueryAndSearchResults() {
        FieldData textData = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Text)
                .setFieldName("body")
                .setScalars(ScalarField.newBuilder().setStringData(
                        StringArray.newBuilder().addData("first").addData("second")))
                .build();

        QueryResults queryResults = QueryResults.newBuilder()
                .addOutputFields("body")
                .addFieldsData(textData)
                .build();
        List<QueryResultsWrapper.RowRecord> queryRows = new QueryResultsWrapper(queryResults).getRowRecords();
        Assertions.assertEquals("first", queryRows.get(0).get("body"));
        Assertions.assertEquals("second", queryRows.get(1).get("body"));

        SearchResultData searchResultData = SearchResultData.newBuilder()
                .setNumQueries(1)
                .setTopK(2)
                .addTopks(2)
                .addScores(1.0f)
                .addScores(0.5f)
                .setIds(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(1L).addData(2L)))
                .setPrimaryFieldName("id")
                .addOutputFields("body")
                .addFieldsData(textData)
                .build();
        List<QueryResultsWrapper.RowRecord> searchRows = new SearchResultsWrapper(searchResultData).getRowRecords(0);
        Assertions.assertEquals("first", searchRows.get(0).get("body"));
        Assertions.assertEquals("second", searchRows.get(1).get("body"));
    }

    @Test
    void decodesTextStructSubField() {
        ScalarField contentRow = ScalarField.newBuilder()
                .setStringData(StringArray.newBuilder().addData("first").addData("second"))
                .build();
        FieldData contentField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Array)
                .setFieldName("content")
                .setScalars(ScalarField.newBuilder().setArrayData(ArrayArray.newBuilder()
                        .setElementType(io.milvus.grpc.DataType.Text)
                        .addData(contentRow)))
                .build();
        FieldData chunksField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.ArrayOfStruct)
                .setFieldName("chunks")
                .setStructArrays(StructArrayField.newBuilder().addFields(contentField))
                .build();

        List<Map<String, Object>> expectedStructs = Arrays.asList(
                Collections.singletonMap("content", "first"),
                Collections.singletonMap("content", "second"));
        Assertions.assertEquals(Collections.singletonList(expectedStructs),
                new FieldDataWrapper(chunksField).getFieldData());
    }

    @Test
    void decodesNullableStructNullRow() {
        ScalarField scoreRow0 = ScalarField.newBuilder()
                .setFloatData(io.milvus.grpc.FloatArray.newBuilder().addData(1.0f).build())
                .build();
        ScalarField scoreRow1 = ScalarField.newBuilder().build();
        FieldData scoreField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Array)
                .setFieldName("score")
                .setScalars(ScalarField.newBuilder().setArrayData(ArrayArray.newBuilder()
                        .setElementType(io.milvus.grpc.DataType.Float)
                        .addData(scoreRow0)
                        .addData(scoreRow1)))
                .addValidData(true)
                .addValidData(false)
                .build();
        FieldData metadataField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.ArrayOfStruct)
                .setFieldName("metadata")
                .setStructArrays(StructArrayField.newBuilder().addFields(scoreField))
                .build();

        List<?> data = new FieldDataWrapper(metadataField).getFieldData();
        Assertions.assertEquals(2, data.size());
        Assertions.assertEquals(
                Collections.singletonList(Collections.singletonMap("score", 1.0f)), data.get(0));
        Assertions.assertNull(data.get(1));
    }

    @Test
    void decodesNullableStructVectorNullRow() {
        VectorField vecRow0 = VectorField.newBuilder()
                .setDim(2)
                .setFloatVector(io.milvus.grpc.FloatArray.newBuilder().addData(1.0f).addData(2.0f).build())
                .build();
        VectorField vecRow1 = VectorField.newBuilder().build();
        FieldData embField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.ArrayOfVector)
                .setFieldName("embedding")
                .setVectors(VectorField.newBuilder()
                        .setVectorArray(io.milvus.grpc.VectorArray.newBuilder()
                                .setElementType(io.milvus.grpc.DataType.FloatVector)
                                .setDim(2)
                                .addData(vecRow0)
                                .addData(vecRow1)))
                .addValidData(true)
                .addValidData(false)
                .build();
        FieldData metadataField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.ArrayOfStruct)
                .setFieldName("metadata")
                .setStructArrays(StructArrayField.newBuilder().addFields(embField))
                .build();

        List<?> data = new FieldDataWrapper(metadataField).getFieldData();
        Assertions.assertEquals(2, data.size());
        Assertions.assertEquals(Collections.singletonList(
                Collections.singletonMap("embedding", Arrays.asList(1.0f, 2.0f))), data.get(0));
        Assertions.assertNull(data.get(1));
    }

    @Test
    void decodesCompactedNullableStructPayload() {
        ScalarField scoreRow0 = ScalarField.newBuilder()
                .setFloatData(io.milvus.grpc.FloatArray.newBuilder().addData(1.0f).build())
                .build();
        FieldData scoreField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.Array)
                .setFieldName("score")
                .setScalars(ScalarField.newBuilder().setArrayData(ArrayArray.newBuilder()
                        .setElementType(io.milvus.grpc.DataType.Float)
                        .addData(scoreRow0)))
                .addValidData(true)
                .addValidData(false)
                .build();
        FieldData metadataField = FieldData.newBuilder()
                .setType(io.milvus.grpc.DataType.ArrayOfStruct)
                .setFieldName("metadata")
                .setStructArrays(StructArrayField.newBuilder().addFields(scoreField))
                .build();

        List<?> data = new FieldDataWrapper(metadataField).getFieldData();
        Assertions.assertEquals(2, data.size());
        Assertions.assertEquals(
                Collections.singletonList(Collections.singletonMap("score", 1.0f)), data.get(0));
        Assertions.assertNull(data.get(1));
    }
}

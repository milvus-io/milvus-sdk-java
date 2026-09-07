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

package io.milvus.unit.v1.response;

import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.ScalarField;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.basic.RowRecordWrapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RowRecordWrapperTest {

    private static class TestRowRecordWrapper extends RowRecordWrapper {
        private final List<FieldData> fields;
        private final List<String> outputFields;

        TestRowRecordWrapper(List<FieldData> fields, List<String> outputFields) {
            this.fields = fields;
            this.outputFields = outputFields;
        }

        @Override
        public List<QueryResultsWrapper.RowRecord> getRowRecords() {
            List<QueryResultsWrapper.RowRecord> records = new ArrayList<>();
            long rowCount = fields.isEmpty() ? 0 : new FieldDataWrapper(fields.get(0)).getRowCount();
            for (int i = 0; i < rowCount; ++i) {
                records.add(buildRowRecord(new QueryResultsWrapper.RowRecord(), i));
            }
            return records;
        }

        QueryResultsWrapper.RowRecord buildAt(long index) {
            return buildRowRecord(new QueryResultsWrapper.RowRecord(), index);
        }

        @Override
        protected List<FieldData> getFieldDataList() {
            return fields;
        }

        @Override
        protected List<String> getOutputFields() {
            return outputFields;
        }
    }

    private FieldData int64Field(String name, Long... values) {
        LongArray.Builder builder = LongArray.newBuilder();
        for (Long value : values) {
            builder.addData(value);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder().setLongData(builder.build()).build())
                .build();
    }

    private FieldData jsonField(String name, boolean dynamic, String... contents) {
        JSONArray.Builder builder = JSONArray.newBuilder();
        for (String content : contents) {
            builder.addData(ByteString.copyFromUtf8(content));
        }
        FieldData.Builder fieldBuilder = FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.JSON)
                .setScalars(ScalarField.newBuilder().setJsonData(builder.build()).build());
        if (dynamic) {
            fieldBuilder.setIsDynamic(true);
        }
        return fieldBuilder.build();
    }

    @Test
    void testScalarRowRecord() {
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(int64Field("id", 1L, 2L, 3L)), Collections.emptyList());
        List<QueryResultsWrapper.RowRecord> records = wrapper.getRowRecords();
        assertEquals(3, records.size());
        assertEquals(1L, records.get(0).get("id"));
        assertEquals(2L, records.get(1).get("id"));
        assertEquals(3L, records.get(2).get("id"));
    }

    @Test
    void testBuildRowRecordIndexOutOfRange() {
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(int64Field("id", 1L)), Collections.emptyList());
        assertThrows(ParamException.class, () -> wrapper.buildAt(5));
        assertThrows(ParamException.class, () -> wrapper.buildAt(-1));
    }

    @Test
    void testGetDynamicWrapper() {
        FieldData meta = jsonField("meta", true, "{\"k1\":1}");
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(meta), Collections.emptyList());
        FieldDataWrapper dynamic = wrapper.getDynamicWrapper();
        assertTrue(dynamic.isJsonField());
        assertTrue(dynamic.isDynamicField());
        assertEquals(1L, dynamic.getRowCount());
        assertSame(dynamic, wrapper.getDynamicWrapper());
    }

    @Test
    void testGetDynamicWrapperMissing() {
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(int64Field("id", 1L)), Collections.emptyList());
        assertThrows(ParamException.class, wrapper::getDynamicWrapper);
    }

    @Test
    void testNonDynamicJsonField() {
        FieldData extra = jsonField("extra", false, "{\"x\":1}");
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(extra), Collections.emptyList());
        QueryResultsWrapper.RowRecord record = wrapper.buildAt(0);
        assertTrue(record.get("extra") instanceof JsonObject);
    }

    @Test
    void testDynamicFieldMergeWithOutputFieldFilter() {
        FieldData id = int64Field("id", 1L);
        FieldData meta = jsonField("meta", true, "{\"k1\":1,\"k2\":\"v\"}");
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                java.util.Arrays.asList(id, meta), java.util.Arrays.asList("id", "k2"));
        QueryResultsWrapper.RowRecord record = wrapper.buildAt(0);
        assertEquals(1L, record.get("id"));
        assertEquals("v", record.get("k2"));
        assertNull(record.get("k1"));
    }

    @Test
    void testDynamicFieldMergeWithoutFilter() {
        FieldData id = int64Field("id", 1L);
        FieldData meta = jsonField("meta", true, "{\"k1\":1,\"k2\":\"v\"}");
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                java.util.Arrays.asList(id, meta), Collections.emptyList());
        QueryResultsWrapper.RowRecord record = wrapper.buildAt(0);
        assertEquals(1L, record.get("k1"));
        assertEquals("v", record.get("k2"));
    }

    @Test
    void testDynamicFieldNonDictThrows() {
        FieldData meta = jsonField("meta", true, "123");
        TestRowRecordWrapper wrapper = new TestRowRecordWrapper(
                Collections.singletonList(meta), Collections.emptyList());
        assertThrows(ParamException.class, () -> wrapper.buildAt(0));
    }
}

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

package io.milvus.response;

import com.google.gson.*;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;

import io.milvus.response.basic.RowRecordWrapper;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;

/**
 * Utility class to wrap response of <code>query</code> interface.
 */
public class QueryResultsWrapper extends RowRecordWrapper {
    private final QueryResults results;

    public QueryResultsWrapper(@NonNull QueryResults results) {
        this.results = results;
    }

    /**
     * Gets {@link FieldDataWrapper} for a field.
     * Throws {@link ParamException} if the field doesn't exist.
     *
     * @param fieldName field name to get output data
     * @return {@link FieldDataWrapper}
     */
    public FieldDataWrapper getFieldWrapper(@NonNull String fieldName) throws ParamException {
        List<FieldData> fields = results.getFieldsDataList();
        for (FieldData field : fields) {
            if (fieldName.compareTo(field.getFieldName()) == 0) {
                return new FieldDataWrapper(field);
            }
        }

        throw new ParamException("The field name doesn't exist");
    }

    /**
     * Gets row records list from query result.
     *
     * @return <code>List<RowRecord></code> a row records list of the query result
     */
    @Override
    public List<QueryResultsWrapper.RowRecord> getRowRecords() {
        long rowCount = getRowCount();
        List<QueryResultsWrapper.RowRecord> records = new ArrayList<>();
        for (long i = 0; i < rowCount; i++) {
            QueryResultsWrapper.RowRecord record = buildRowRecord(i);
            records.add(record);
        }

        return records;
    }

    /**
     * Gets a row record from result.
     *  Throws {@link ParamException} if the index is illegal.
     *
     * @return <code>RowRecord</code> a row record of the result
     */
    protected QueryResultsWrapper.RowRecord buildRowRecord(long index) {
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        buildRowRecord(record, index);
        return record;
    }

    /**
     * Gets the row count of the result.
     *
     * @return <code>long</code> row count of the result
     */
    public long getRowCount() {
        List<FieldData> fields = results.getFieldsDataList();
        for (FieldData field : fields) {
            FieldDataWrapper wrapper = new FieldDataWrapper(field);
            return wrapper.getRowCount();
        }

        return 0L;
    }

    @Override
    protected List<FieldData> getFieldDataList() {
        return results.getFieldsDataList();
    }

    protected List<String> getOutputFields() {
        return results.getOutputFieldsList();
    }

    /**
     * Internal-use class to wrap response of <code>query</code> interface.
     */
    @Getter
    public static final class RowRecord {
        Map<String, Object> fieldValues = new HashMap<>();

        public RowRecord() {
        }

        public boolean put(String keyName, Object obj) {
            if (fieldValues.containsKey(keyName)) {
                return false;
            }
            fieldValues.put(keyName, obj);

            return true;
        }

        /**
         * Get a value by a key name. If the key name is a field name, return the value of this field.
         * If the key name is in dynamic field, return the value from the dynamic field.
         * Throws {@link ParamException} if the key name doesn't exist.
         *
         * @return {@link FieldDataWrapper}
         */
        public Object get(String keyName) throws ParamException {
            if (fieldValues.isEmpty()) {
                throw new ParamException("This record is empty");
            }

            Object obj = fieldValues.get(keyName);
            if (obj == null) {
                // find the value from dynamic field
                Object meta = fieldValues.get(Constant.DYNAMIC_FIELD_NAME);
                if (meta != null) {
                    JsonObject jsonMata = (JsonObject)meta;
                    Object innerObj = jsonMata.get(keyName);
                    if (innerObj != null) {
                        return innerObj;
                    }
                }
                throw new ParamException("The key name is not found");
            }

            return obj;
        }

        /**
         * Constructs a <code>String</code> by {@link QueryResultsWrapper.RowRecord} instance.
         *
         * @return <code>String</code>
         */
        @Override
        public String toString() {
            List<String> pairs = new ArrayList<>();
            fieldValues.forEach((keyName, fieldValue) -> {
                pairs.add(keyName + ":" + fieldValue.toString());
            });
            return pairs.toString();
        }
    }
}

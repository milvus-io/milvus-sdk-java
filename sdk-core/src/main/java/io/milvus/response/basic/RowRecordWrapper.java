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

package io.milvus.response.basic;

import com.google.gson.*;
import io.milvus.exception.ParamException;
import io.milvus.grpc.FieldData;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RowRecordWrapper {
    // a cache for output fields
    private ConcurrentHashMap<String, FieldDataWrapper> outputFieldsData = new ConcurrentHashMap<>();
    // a cache for output dynamic field names
    private List<String> dynamicFieldNames = null;

    public abstract List<QueryResultsWrapper.RowRecord> getRowRecords();

    protected FieldDataWrapper getFieldWrapperInternal(FieldData field) {
        if (outputFieldsData.containsKey(field.getFieldName())) {
            return outputFieldsData.get(field.getFieldName());
        }

        FieldDataWrapper wrapper = new FieldDataWrapper(field);
        outputFieldsData.put(field.getFieldName(), wrapper);
        return wrapper;
    }

    /**
     * Get the dynamic field. Only available when a collection's dynamic field is enabled.
     * Throws {@link ParamException} if the dynamic field doesn't exist.
     *
     * @return {@link FieldDataWrapper}
     */
    public FieldDataWrapper getDynamicWrapper() throws ParamException {
        List<FieldData> fields = getFieldDataList();
        for (FieldData field : fields) {
            if (field.getIsDynamic()) {
                return getFieldWrapperInternal(field);
            }
        }

        throw new ParamException("The dynamic field doesn't exist");
    }

    /**
     * Gets a row record from result.
     *  Throws {@link ParamException} if the index is illegal.
     *
     * @return <code>RowRecord</code> a row record of the result
     */
    protected QueryResultsWrapper.RowRecord buildRowRecord(QueryResultsWrapper.RowRecord record, long index) {
        List<String> dynamicFields = getDynamicFieldNames();
        List<FieldData> fieldsData = getFieldDataList();
        for (FieldData field : fieldsData) {
            FieldDataWrapper wrapper = getFieldWrapperInternal(field);
            if (index < 0 || index >= wrapper.getRowCount()) {
                throw new ParamException("Index out of range");
            }
            Object value = wrapper.valueByIdx((int)index);
            if (wrapper.isJsonField()) {
                JsonElement jsonValue = FieldDataWrapper.ParseJSONObject(value);
                if (!field.getIsDynamic()) {
                    record.put(field.getFieldName(), jsonValue);
                    continue;
                }

                // dynamic field, the value must be a dict
                if (!(jsonValue instanceof JsonObject)) {
                    throw new ParamException("The content of dynamic field is not a JSON dict");
                }

                JsonObject jsonDict = (JsonObject)jsonValue;
                // the outputFields of QueryRequest/SearchRequest contains a "$meta"
                // put all key/value pairs of "$meta" into record
                // else pick some key/value pairs according to the dynamicFields
                for (String key: jsonDict.keySet()) {
                    if (dynamicFields.isEmpty() || dynamicFields.contains(key)) {
                        record.put(key, FieldDataWrapper.ValueOfJSONElement(jsonDict.get(key)));
                    }
                }
            } else {
                record.put(field.getFieldName(), value);
            }
        }

        return record;
    }

    private List<String> getDynamicFieldNames() {
        if (dynamicFieldNames != null) {
            return dynamicFieldNames;
        }

        dynamicFieldNames = new ArrayList<>();
        // find out dynamic field names
        List<FieldData> fieldsData = getFieldDataList();
        String dynamicFieldName = null;
        List<String> fieldNames = new ArrayList<>();
        for (FieldData field : fieldsData) {
            if (!fieldNames.contains(field.getFieldName())) {
                fieldNames.add(field.getFieldName());
            }
            if (field.getIsDynamic()) {
                dynamicFieldName = field.getFieldName();
            }
        }

        List<String> outputNames = getOutputFields();
        for (String name : outputNames) {
            if (name.equals(dynamicFieldName)) {
                dynamicFieldNames.clear();
                break;
            }
            if (!fieldNames.contains(name)) {
                dynamicFieldNames.add(name);
            }
        }
        return dynamicFieldNames;
    }

    protected abstract List<FieldData> getFieldDataList();
    protected abstract List<String> getOutputFields();
}

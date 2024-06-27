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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RowRecordWrapper {
    // a cache for output fields
    private ConcurrentHashMap<String, FieldDataWrapper> outputFieldsData = new ConcurrentHashMap<>();

    protected FieldDataWrapper getFieldWrapperInternal(FieldData field) {
        if (outputFieldsData.containsKey(field.getFieldName())) {
            return outputFieldsData.get(field.getFieldName());
        }

        FieldDataWrapper wrapper = new FieldDataWrapper(field);
        outputFieldsData.put(field.getFieldName(), wrapper);
        return wrapper;
    }

    public abstract List<QueryResultsWrapper.RowRecord> getRowRecords();

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
        for (String outputKey : getOutputFields()) {
            boolean isField = false;
            for (FieldData field : getFieldDataList()) {
                if (outputKey.equals(field.getFieldName())) {
                    FieldDataWrapper wrapper = getFieldWrapperInternal(field);
                    if (index < 0 || index >= wrapper.getRowCount()) {
                        throw new ParamException("Index out of range");
                    }
                    Object value = wrapper.valueByIdx((int)index);
                    if (wrapper.isJsonField()) {
                        JsonElement jsonField = FieldDataWrapper.ParseJSONObject(value);
                        if (wrapper.isDynamicField() && jsonField instanceof JsonObject) {
                            JsonObject jsonObj = (JsonObject) jsonField;
                            for (String key: jsonObj.keySet()) {
                                record.put(key, FieldDataWrapper.ValueOfJSONElement(jsonObj.get(key)));
                            }
                        } else {
                            record.put(field.getFieldName(), jsonField);
                        }
                    } else {
                        record.put(field.getFieldName(), value);
                    }
                    isField = true;
                    break;
                }
            }

            // if the output field is not a field name, fetch it from dynamic field
            if (!isField) {
                FieldDataWrapper dynamicField = getDynamicWrapper();
                Object obj = dynamicField.get((int)index, outputKey);
                if (obj != null) {
                    record.put(outputKey, obj);
                }
            }
        }
        return record;
    }

    protected abstract List<FieldData> getFieldDataList();
    protected abstract List<String> getOutputFields();

}

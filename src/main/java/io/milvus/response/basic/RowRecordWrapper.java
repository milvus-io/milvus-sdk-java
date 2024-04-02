package io.milvus.response.basic;

import com.alibaba.fastjson.JSONObject;
import io.milvus.exception.ParamException;
import io.milvus.grpc.FieldData;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.List;

public abstract class RowRecordWrapper {

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
                return new FieldDataWrapper(field);
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
                    FieldDataWrapper wrapper = new FieldDataWrapper(field);
                    if (index < 0 || index >= wrapper.getRowCount()) {
                        throw new ParamException("Index out of range");
                    }
                    Object value = wrapper.valueByIdx((int)index);
                    if (wrapper.isJsonField()) {
                        JSONObject jsonField = FieldDataWrapper.ParseJSONObject(value);
                        if (wrapper.isDynamicField()) {
                            for (String key: jsonField.keySet()) {
                                record.put(key, jsonField.get(key));
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

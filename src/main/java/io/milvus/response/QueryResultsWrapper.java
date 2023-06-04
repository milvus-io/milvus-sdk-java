package io.milvus.response;

import com.alibaba.fastjson.JSONObject;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;

import io.milvus.param.dml.InsertParam;
import kotlin.text.UStringsKt;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;

/**
 * Utility class to wrap response of <code>query</code> interface.
 */
public class QueryResultsWrapper {
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
     * Get the dynamic field. Only available when a collection's dynamic field is enabled.
     * Throws {@link ParamException} if the dynamic field doesn't exist.
     *
     * @return {@link FieldDataWrapper}
     */
    public FieldDataWrapper getDynamicWrapper() throws ParamException {
        List<FieldData> fields = results.getFieldsDataList();
        for (FieldData field : fields) {
            if (field.getIsDynamic()) {
                return new FieldDataWrapper(field);
            }
        }

        throw new ParamException("The dynamic field doesn't exist");
    }

    /**
     * Gets the row count of a query result.
     *
     * @return <code>long</code> row count of the query result
     */
    public long getRowCount() {
        List<FieldData> fields = results.getFieldsDataList();
        for (FieldData field : fields) {
            FieldDataWrapper wrapper = new FieldDataWrapper(field);
            return wrapper.getRowCount();
        }

        return 0L;
    }

    /**
     * Gets a row record from query result.
     *  Throws {@link ParamException} if the index is illegal.
     *
     * @return <code>RowRecord</code> a row record of the query result
     */
    public RowRecord getRowRecord(long index) throws ParamException {
        List<String> outputFields = results.getOutputFieldsList();
        List<FieldData> fields = results.getFieldsDataList();

        RowRecord record = new RowRecord();
        for (String outputKey : outputFields) {
            boolean isField = false;
            for (FieldData field : fields) {
                if (outputKey.equals(field.getFieldName())) {
                    FieldDataWrapper wrapper = new FieldDataWrapper(field);
                    if (index < 0 || index >= wrapper.getRowCount()) {
                        throw new ParamException("Index out of range");
                    }
                    Object value = wrapper.valueByIdx((int)index);
                    if (wrapper.isJsonField()) {
                        record.put(field.getFieldName(), JSONObject.parseObject(new String((byte[])value)));
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
                if (dynamicField != null) {
                    Object obj = dynamicField.get((int)index, outputKey);
                    if (obj != null) {
                        record.put(outputKey, obj);
                    }
                }
            }
        }

        return record;
    }

    /**
     * Gets row records list from query result.
     *
     * @return <code>List<RowRecord></code> a row records list of the query result
     */
    public List<RowRecord> getRowRecords() {
        long rowCount = getRowCount();
        List<RowRecord> records = new ArrayList<>();
        for (long i = 0; i < rowCount; i++) {
            RowRecord record = getRowRecord(i);
            records.add(record);
        }

        return records;
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
                Object meta = fieldValues.get("$meta");
                if (meta != null) {
                    JSONObject jsonMata = (JSONObject)meta;
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

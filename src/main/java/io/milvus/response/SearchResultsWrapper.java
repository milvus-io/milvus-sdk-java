package io.milvus.response;

import com.alibaba.fastjson.JSONObject;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to wrap response of <code>search</code> interface.
 */
public class SearchResultsWrapper {
    private final SearchResultData results;

    public SearchResultsWrapper(@NonNull SearchResultData results) {
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
     * Gets data for an output field which is specified by search request.
     * Throws {@link ParamException} if the field doesn't exist.
     * Throws {@link ParamException} if the indexOfTarget is illegal.
     *
     * @param fieldName field name to get output data
     * @param indexOfTarget which target vector the field data belongs to
     * @return {@link FieldDataWrapper}
     */
    public List<?> getFieldData(@NonNull String fieldName, int indexOfTarget) {
        FieldDataWrapper wrapper = null;
        for (int i = 0; i < results.getFieldsDataCount(); ++i) {
            FieldData data = results.getFieldsData(i);
            if (fieldName.compareTo(data.getFieldName()) == 0) {
                wrapper = new FieldDataWrapper(data);
            }
        }

        if (wrapper == null) {
            throw new ParamException("Illegal field name: " + fieldName);
        }

        Position position = getOffsetByIndex(indexOfTarget);
        long offset = position.getOffset();
        long k = position.getK();

        List<?> allData = wrapper.getFieldData();
        if (offset + k > allData.size()) {
            throw new IllegalResponseException("Field data row count is wrong");
        }

        return allData.subList((int)offset, (int)offset + (int)k);
    }

    /**
     * Gets ID-score pairs returned by search interface.
     * Throws {@link ParamException} if the indexOfTarget is illegal.
     * Throws {@link IllegalResponseException} if the returned results is illegal.
     *
     * @param indexOfTarget which target vector the result belongs to
     * @return List of IDScore, ID-score pairs returned by search interface
     */
    public List<IDScore> getIDScore(int indexOfTarget) throws ParamException, IllegalResponseException {
        Position position = getOffsetByIndex(indexOfTarget);

        long offset = position.getOffset();
        long k = position.getK();
        if (offset + k > results.getScoresCount()) {
            throw new IllegalResponseException("Result scores count is wrong");
        }

        List<IDScore> idScores = new ArrayList<>();

        // set id and distance
        IDs ids = results.getIds();
        if (ids.hasIntId()) {
            LongArray longIDs = ids.getIntId();
            if (offset + k > longIDs.getDataCount()) {
                throw new IllegalResponseException("Result ids count is wrong");
            }

            for (int n = 0; n < k; ++n) {
                idScores.add(new IDScore("", longIDs.getData((int)offset + n), results.getScores((int)offset + n)));
            }
        } else if (ids.hasStrId()) {
            StringArray strIDs = ids.getStrId();
            if (offset + k > strIDs.getDataCount()) {
                throw new IllegalResponseException("Result ids count is wrong");
            }

            for (int n = 0; n < k; ++n) {
                idScores.add(new IDScore(strIDs.getData((int)offset + n), 0, results.getScores((int)offset + n)));
            }
        } else {
            throw new IllegalResponseException("Result ids is illegal");
        }

        // set output fields
        List<String> outputFields = results.getOutputFieldsList();
        List<FieldData> fields = results.getFieldsDataList();
        if (fields.isEmpty()) {
            return idScores;
        }

        for (String outputKey : outputFields) {
            boolean isField = false;
            FieldDataWrapper dynamicField = null;
            for (FieldData field : fields) {
                if (field.getIsDynamic()) {
                    dynamicField = new FieldDataWrapper(field);
                }
                if (outputKey.equals(field.getFieldName())) {
                    FieldDataWrapper wrapper = new FieldDataWrapper(field);
                    for (int n = 0; n < k; ++n) {
                        if ((offset + n) >= wrapper.getRowCount()) {
                            throw new ParamException("Illegal values length of output fields");
                        }

                        Object value = wrapper.valueByIdx((int)offset + n);
                        if (wrapper.isJsonField()) {
                            idScores.get(n).put(field.getFieldName(), JSONObject.parseObject(new String((byte[])value)));
                        } else {
                            idScores.get(n).put(field.getFieldName(), value);
                        }
                    }

                    isField = true;
                    break;
                }
            }

            // if the output field is not a field name, fetch it from dynamic field
            if (!isField && dynamicField != null) {
                for (int n = 0; n < k; ++n) {
                    Object obj = dynamicField.get((int)offset + n, outputKey);
                    if (obj != null) {
                        idScores.get(n).put(outputKey, obj);
                    }
                }
            }
        }
        return idScores;
    }

    @Getter
    private static final class Position {
        private final long offset;
        private final long k;

        public Position(long offset, long k) {
            this.offset = offset;
            this.k = k;
        }
    }
    private Position getOffsetByIndex(int indexOfTarget) {
        List<Long> kList = results.getTopksList();

        // if the server didn't return separate topK, use same topK value
        if (kList.isEmpty()) {
            kList = new ArrayList<>();
            for (long i = 0; i < results.getNumQueries(); ++i) {
                kList.add(results.getTopK());
            }
        }

        if (indexOfTarget < 0 || indexOfTarget >= kList.size()) {
            throw new ParamException("Illegal index of target: " + indexOfTarget);
        }

        long offset = 0;
        for (int i = 0; i < indexOfTarget; ++i) {
            offset += kList.get(i);
        }

        long k = kList.get(indexOfTarget);
        return new Position(offset, k);
    }

    /**
     * Internal-use class to wrap response of <code>search</code> interface.
     */
    @Getter
    public static final class IDScore {
        private final String strID;
        private final long longID;
        private final float score;
        Map<String, Object> fieldValues = new HashMap<>();

        public IDScore(String strID, long longID, float score) {
            this.strID = strID;
            this.longID = longID;
            this.score = score;
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
            }

            return obj;
        }

        @Override
        public String toString() {
            List<String> pairs = new ArrayList<>();
            fieldValues.forEach((keyName, fieldValue) -> {
                pairs.add(keyName + ":" + fieldValue.toString());
            });

            if (strID.isEmpty()) {
                return "(ID: " + getLongID() + " Score: " + getScore() + " OutputFields: " + pairs + ")";
            } else {
                return "(ID: '" + getStrID() + "' Score: " + getScore()+ " OutputFields: " + pairs + ")";
            }
        }
    }
}

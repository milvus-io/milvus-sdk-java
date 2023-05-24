package io.milvus.param;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.DescCollResponseWrapper;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility functions for param classes
 */
public class ParamUtils {
    public static HashMap<DataType, String> getTypeErrorMsg() {
        final HashMap<DataType, String> typeErrMsg = new HashMap<>();
        typeErrMsg.put(DataType.None, "Type mismatch for field '%s': the field type is illegal");
        typeErrMsg.put(DataType.Bool, "Type mismatch for field '%s': Bool field value type must be Boolean");
        typeErrMsg.put(DataType.Int8, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer");
        typeErrMsg.put(DataType.Int16, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer");
        typeErrMsg.put(DataType.Int32, "Type mismatch for field '%s': Int32/Int16/Int8 field value type must be Short or Integer");
        typeErrMsg.put(DataType.Int64, "Type mismatch for field '%s': Int64 field value type must be Long");
        typeErrMsg.put(DataType.Float, "Type mismatch for field '%s': Float field value type must be Float");
        typeErrMsg.put(DataType.Double, "Type mismatch for field '%s': Double field value type must be Double");
        typeErrMsg.put(DataType.String, "Type mismatch for field '%s': String field value type must be String");
        typeErrMsg.put(DataType.VarChar, "Type mismatch for field '%s': VarChar field value type must be String");
        typeErrMsg.put(DataType.FloatVector, "Type mismatch for field '%s': Float vector field's value type must be List<Float>");
        typeErrMsg.put(DataType.BinaryVector, "Type mismatch for field '%s': Binary vector field's value type must be ByteBuffer");
        return typeErrMsg;
    }

    private static void checkFieldData(FieldType fieldSchema, InsertParam.Field fieldData) {
        List<?> values = fieldData.getValues();
        checkFieldData(fieldSchema, values);
    }

    private static void checkFieldData(FieldType fieldSchema, List<?> values) {
        HashMap<DataType, String> errMsgs = getTypeErrorMsg();
        DataType dataType = fieldSchema.getDataType();

        switch (dataType) {
            case FloatVector: {
                int dim = fieldSchema.getDimension();
                for (int i = 0; i < values.size(); ++i) {
                    // is List<> ?
                    Object value  = values.get(i);
                    if (!(value instanceof List)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                    // is List<Float> ?
                    List<?> temp = (List<?>)value;
                    for (Object v : temp) {
                        if (!(v instanceof Float)) {
                            throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                        }
                    }

                    // check dimension
                    if (temp.size() != dim) {
                        String msg = "Incorrect dimension for field '%s': the no.%d vector's dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), i, temp.size(), dim));
                    }
                }
            }
            break;
            case BinaryVector: {
                int dim = fieldSchema.getDimension();
                for (int i = 0; i < values.size(); ++i) {
                    Object value  = values.get(i);
                    // is ByteBuffer?
                    if (!(value instanceof ByteBuffer)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }

                    // check dimension
                    ByteBuffer v = (ByteBuffer)value;
                    if (v.position()*8 != dim) {
                        String msg = "Incorrect dimension for field '%s': the no.%d vector's dimension: %d is not equal to field's dimension: %d";
                        throw new ParamException(String.format(msg, fieldSchema.getName(), i, v.position()*8, dim));
                    }
                }
            }
            break;
            case Int64:
                for (Object value : values) {
                    if (!(value instanceof Long)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Int32:
            case Int16:
            case Int8:
                for (Object value : values) {
                    if (!(value instanceof Short) && !(value instanceof Integer)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Bool:
                for (Object value : values) {
                    if (!(value instanceof Boolean)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Float:
                for (Object value : values) {
                    if (!(value instanceof Float)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Double:
                for (Object value : values) {
                    if (!(value instanceof Double)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case VarChar:
            case String:
                for (Object value : values) {
                    if (!(value instanceof String)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case JSON:
                for (Object value : values) {
                    if (!(value instanceof ByteString)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Checks if a string is empty or null.
     * Throws {@link ParamException} if the string is empty of null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullEmptyString(String target, String name) throws ParamException {
        if (target == null || StringUtils.isBlank(target)) {
            throw new ParamException(name + " cannot be null or empty");
        }
    }

    /**
     * Checks if a string is  null.
     * Throws {@link ParamException} if the string is null.
     *
     * @param target target string
     * @param name a name to describe this string
     */
    public static void CheckNullString(String target, String name) throws ParamException {
        if (target == null) {
            throw new ParamException(name + " cannot be null");
        }
    }

    /**
     * Checks if a metric is for float vector.
     *
     * @param metric metric type
     */
    public static boolean IsFloatMetric(MetricType metric) {
        return metric == MetricType.L2 || metric == MetricType.IP;
    }

    /**
     * Checks if a metric is for binary vector.
     *
     * @param metric metric type
     */
    public static boolean IsBinaryMetric(MetricType metric) {
        return !IsFloatMetric(metric);
    }

    /**
     * Checks if an index type is for vector.
     *
     * @param idx index type
     */
    public static boolean IsVectorIndex(IndexType idx) {
        return idx != IndexType.INVALID && idx != IndexType.TRIE;
    }

    public static InsertRequest convertInsertParam(@NonNull InsertParam requestParam,
                                                   @NonNull DescCollResponseWrapper wrapper) {
        String collectionName = requestParam.getCollectionName();
        String partitionName = requestParam.getPartitionName();

        // gen insert request
        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
        InsertRequest.Builder insertBuilder = InsertRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(partitionName)
                .setBase(msgBase)
                .setNumRows(requestParam.getRowCount());
        if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
            insertBuilder.setDbName(requestParam.getDatabaseName());
        }
        fillFieldsData(requestParam, wrapper, insertBuilder);

        // gen request
        return insertBuilder.build();
    }

    private static void fillFieldsData(InsertParam requestParam, DescCollResponseWrapper wrapper, InsertRequest.Builder insertBuilder) {
        List<InsertParam.Field> columnFields = requestParam.getFields();
        List<JSONObject> rowFields = requestParam.getRows();

        if (CollectionUtils.isNotEmpty(columnFields)) {
            checkAndSetColumnData(wrapper.getFields(), insertBuilder, columnFields);
        } else {
            checkAndSetRowData(wrapper, insertBuilder, rowFields);
        }
    }

    private static void checkAndSetColumnData(List<FieldType> fieldTypes, InsertRequest.Builder insertBuilder, List<InsertParam.Field> fields) {
        // gen fieldData
        // make sure the field order must be consisted with collection schema
        for (FieldType fieldType : fieldTypes) {
            boolean found = false;
            for (InsertParam.Field field : fields) {
                if (field.getName().equals(fieldType.getName())) {
                    if (fieldType.isAutoID()) {
                        String msg = "The primary key: " + fieldType.getName() + " is auto generated, no need to input.";
                        throw new ParamException(msg);
                    }
                    checkFieldData(fieldType, field);

                    found = true;
                    insertBuilder.addFieldsData(genFieldData(field.getName(), fieldType.getDataType(), field.getValues()));
                    break;
                }

            }
            if (!found && !fieldType.isAutoID()) {
                String msg = "The field: " + fieldType.getName() + " is not provided.";
                throw new ParamException(msg);
            }
        }
    }

    private static void checkAndSetRowData(DescCollResponseWrapper wrapper, InsertRequest.Builder insertBuilder, List<JSONObject> rows) {
        List<FieldType> fieldTypes = wrapper.getFields();

        Map<String, InsertDataInfo> nameInsertInfo = new HashMap<>();
        InsertDataInfo insertDynamicDataInfo = InsertDataInfo.builder().dataType(DataType.JSON).data(new LinkedList<>()).build();
        for (JSONObject row : rows) {
            for (FieldType fieldType : fieldTypes) {
                String fieldName = fieldType.getName();
                InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, InsertDataInfo.builder()
                        .fieldName(fieldName).dataType(fieldType.getDataType()).data(new LinkedList<>()).build());

                // check jsonField
                if (fieldType.getDataType() == DataType.JSON) {
                    JSONObject jsonField = parseJsonData(row, fieldType);
                    insertDataInfo.getData().add(jsonField);
                    nameInsertInfo.put(fieldName, insertDataInfo);
                } else {
                    // check normalField
                    Object rowFieldData = row.get(fieldName);
                    if (rowFieldData != null) {
                        if (fieldType.isAutoID()) {
                            String msg = "The primary key: " + fieldName + " is auto generated, no need to input.";
                            throw new ParamException(msg);
                        }
                        checkFieldData(fieldType, Lists.newArrayList(rowFieldData));

                        insertDataInfo.getData().add(rowFieldData);
                        nameInsertInfo.put(fieldName, insertDataInfo);
                    } else {
                        // check if autoId
                        if (!fieldType.isAutoID()) {
                            String msg = "The field: " + fieldType.getName() + " is not provided.";
                            throw new ParamException(msg);
                        }
                    }
                }
            }

            // deal with dynamicField
            if (wrapper.getEnableDynamicField()) {
                JSONObject dynamicField = new JSONObject();
                for (String rowFieldName : row.keySet()) {
                    Pair<String, String> outerJsonField = parseData(rowFieldName);
                    if (!nameInsertInfo.containsKey(rowFieldName) && outerJsonField == null) {
                        dynamicField.put(rowFieldName, row.get(rowFieldName));
                    }
                }
                insertDynamicDataInfo.getData().add(dynamicField);
            }

            for (String rowFieldName : row.keySet()) {
                Pair<String, String> outerJsonField = parseData(rowFieldName);
                if (outerJsonField != null  && !nameInsertInfo.containsKey(outerJsonField.getKey())) {
                    String msg = "The field: " + outerJsonField.getKey() + " not exists, no need to provided.";
                    throw new ParamException(msg);
                }
            }
        }

        for (String fieldNameKey : nameInsertInfo.keySet()) {
            InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
            insertBuilder.addFieldsData(genFieldData(insertDataInfo.getFieldName(), insertDataInfo.getDataType(), insertDataInfo.getData()));
        }
        if (wrapper.getEnableDynamicField()) {
            insertBuilder.addFieldsData(genFieldData(insertDynamicDataInfo.getFieldName(), insertDynamicDataInfo.getDataType(), insertDynamicDataInfo.getData(), Boolean.TRUE));
        }
    }

    private static JSONObject parseJsonData(JSONObject row, FieldType fieldType) {
        JSONObject jsonField = new JSONObject();
        for (String rowFieldName : row.keySet()) {
            Pair<String, String> outerJsonField = parseData(rowFieldName);
            if (outerJsonField != null && fieldType.getName().equals(outerJsonField.getKey())) {
                jsonField.put(outerJsonField.getValue(), row.get(rowFieldName));
            }
        }
        return jsonField;
    }

    public static Pair<String, String> parseData(String data) {
        String pattern = "(\\w+)\\[\"(\\w+)\"\\]";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(data);
        if (matcher.matches()) {
            return Pair.of(matcher.group(1), matcher.group(2));
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static SearchRequest convertSearchParam(@NonNull SearchParam requestParam) throws ParamException {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setDbName("")
                .setCollectionName(requestParam.getCollectionName());
        if (!requestParam.getPartitionNames().isEmpty()) {
            requestParam.getPartitionNames().forEach(builder::addPartitionNames);
        }

        // prepare target vectors
        // TODO: check target vector dimension(use DescribeCollection get schema to compare)
        PlaceholderType plType = PlaceholderType.None;
        List<?> vectors = requestParam.getVectors();
        List<ByteString> byteStrings = new ArrayList<>();
        for (Object vector : vectors) {
            if (vector instanceof List) {
                plType = PlaceholderType.FloatVector;
                List<Float> list = (List<Float>) vector;
                ByteBuffer buf = ByteBuffer.allocate(Float.BYTES * list.size());
                buf.order(ByteOrder.LITTLE_ENDIAN);
                list.forEach(buf::putFloat);

                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else if (vector instanceof ByteBuffer) {
                plType = PlaceholderType.BinaryVector;
                ByteBuffer buf = (ByteBuffer) vector;
                byte[] array = buf.array();
                ByteString bs = ByteString.copyFrom(array);
                byteStrings.add(bs);
            } else {
                String msg = "Search target vector type is illegal(Only allow List<Float> or ByteBuffer)";
                throw new ParamException(msg);
            }
        }

        PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                .setTag(Constant.VECTOR_TAG)
                .setType(plType);
        byteStrings.forEach(pldBuilder::addValues);

        PlaceholderValue plv = pldBuilder.build();
        PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                .addPlaceholders(plv)
                .build();

        ByteString byteStr = placeholderGroup.toByteString();
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(requestParam.getNQ());

        // search parameters
        builder.addSearchParams(
                KeyValuePair.newBuilder()
                        .setKey(Constant.VECTOR_FIELD)
                        .setValue(requestParam.getVectorFieldName())
                        .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(requestParam.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.METRIC_TYPE)
                                .setValue(requestParam.getMetricType())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.ROUND_DECIMAL)
                                .setValue(String.valueOf(requestParam.getRoundDecimal()))
                                .build());

        if (null != requestParam.getParams() && !requestParam.getParams().isEmpty()) {
            try {
            Map<String, Object> paramMap = JacksonUtils.fromJson(requestParam.getParams(),Map.class);
            String offset = paramMap.getOrDefault(Constant.OFFSET, 0).toString();
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.OFFSET)
                            .setValue(offset)
                            .build());
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.PARAMS)
                            .setValue(requestParam.getParams())
                            .build());
            } catch (IllegalArgumentException e) {
                throw new ParamException(e.getMessage() + e.getCause().getMessage());
            }
        }

        if (!requestParam.getOutFields().isEmpty()) {
            requestParam.getOutFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (requestParam.getExpr() != null && !requestParam.getExpr().isEmpty()) {
            builder.setDsl(requestParam.getExpr());
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(requestParam.getConsistencyLevel(),
                requestParam.getGuaranteeTimestamp(), requestParam.getGracefulTime());
        builder.setTravelTimestamp(requestParam.getTravelTimestamp());
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        return builder.build();
    }

    public static QueryRequest convertQueryParam(@NonNull QueryParam requestParam) {
        long guaranteeTimestamp = getGuaranteeTimestamp(requestParam.getConsistencyLevel(),
                requestParam.getGuaranteeTimestamp(), requestParam.getGracefulTime());
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                .setCollectionName(requestParam.getCollectionName())
                .addAllPartitionNames(requestParam.getPartitionNames())
                .addAllOutputFields(requestParam.getOutFields())
                .setExpr(requestParam.getExpr())
                .setTravelTimestamp(requestParam.getTravelTimestamp())
                .setGuaranteeTimestamp(guaranteeTimestamp);

        // set offset and limit value.
        // directly pass the two values, the server will verify them.
        long offset = requestParam.getOffset();
        if (offset > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey("offset")
                    .setValue(String.valueOf(offset))
                    .build());
        }

        long limit = requestParam.getLimit();
        if (limit > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey("limit")
                    .setValue(String.valueOf(limit))
                    .build());
        }

        return builder.build();
    }

    private static long getGuaranteeTimestamp(ConsistencyLevelEnum consistencyLevel,
                                              long guaranteeTimestamp, Long gracefulTime){
        if(consistencyLevel == null){
            return guaranteeTimestamp;
        }
        switch (consistencyLevel){
            case STRONG:
                guaranteeTimestamp = 0L;
                break;
            case BOUNDED:
                guaranteeTimestamp = (new Date()).getTime() - gracefulTime;
                break;
            case EVENTUALLY:
                guaranteeTimestamp = 1L;
                break;
        }
        return guaranteeTimestamp;
    }


    private static final Set<DataType> vectorDataType = new HashSet<DataType>() {{
        add(DataType.FloatVector);
        add(DataType.BinaryVector);
    }};

    @SuppressWarnings("unchecked")
    private static FieldData genFieldData(String fieldName, DataType dataType, List<?> objects) {
        return genFieldData(fieldName, dataType, objects, Boolean.FALSE);
    }

    @SuppressWarnings("unchecked")
    private static FieldData genFieldData(String fieldName, DataType dataType, List<?> objects, boolean isDynamic) {
        if (objects == null) {
            throw new ParamException("Cannot generate FieldData from null object");
        }
        FieldData.Builder builder = FieldData.newBuilder();
        if (vectorDataType.contains(dataType)) {
            if (dataType == DataType.FloatVector) {
                List<Float> floats = new ArrayList<>();
                // each object is List<Float>
                for (Object object : objects) {
                    if (object instanceof List) {
                        List<Float> list = (List<Float>) object;
                        floats.addAll(list);
                    } else {
                        throw new ParamException("The type of FloatVector must be List<Float>");
                    }
                }

                int dim = floats.size() / objects.size();
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
                return builder.setFieldName(fieldName).setType(DataType.FloatVector).setVectors(vectorField).build();
            } else if (dataType == DataType.BinaryVector) {
                ByteBuffer totalBuf = null;
                int dim = 0;
                // each object is ByteBuffer
                for (Object object : objects) {
                    ByteBuffer buf = (ByteBuffer) object;
                    if (totalBuf == null) {
                        totalBuf = ByteBuffer.allocate(buf.position() * objects.size());
                        totalBuf.put(buf.array());
                        dim = buf.position() * 8;
                    } else {
                        totalBuf.put(buf.array());
                    }
                }

                assert totalBuf != null;
                ByteString byteString = ByteString.copyFrom(totalBuf.array());
                VectorField vectorField = VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
                return builder.setFieldName(fieldName).setType(DataType.BinaryVector).setVectors(vectorField).build();
            }
        } else {
            switch (dataType) {
                case None:
                case UNRECOGNIZED:
                    throw new ParamException("Cannot support this dataType:" + dataType);
                case Int64: {
                    List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                    LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                    ScalarField scalarField = ScalarField.newBuilder().setLongData(longArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Int32:
                case Int16:
                case Int8: {
                    List<Integer> integers = objects.stream().map(p -> p instanceof Short ? ((Short) p).intValue() : (Integer) p).collect(Collectors.toList());
                    IntArray intArray = IntArray.newBuilder().addAllData(integers).build();
                    ScalarField scalarField = ScalarField.newBuilder().setIntData(intArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Bool: {
                    List<Boolean> booleans = objects.stream().map(p -> (Boolean) p).collect(Collectors.toList());
                    BoolArray boolArray = BoolArray.newBuilder().addAllData(booleans).build();
                    ScalarField scalarField = ScalarField.newBuilder().setBoolData(boolArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Float: {
                    List<Float> floats = objects.stream().map(p -> (Float) p).collect(Collectors.toList());
                    FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                    ScalarField scalarField = ScalarField.newBuilder().setFloatData(floatArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case Double: {
                    List<Double> doubles = objects.stream().map(p -> (Double) p).collect(Collectors.toList());
                    DoubleArray doubleArray = DoubleArray.newBuilder().addAllData(doubles).build();
                    ScalarField scalarField = ScalarField.newBuilder().setDoubleData(doubleArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case String:
                case VarChar: {
                    List<String> strings = objects.stream().map(p -> (String) p).collect(Collectors.toList());
                    StringArray stringArray = StringArray.newBuilder().addAllData(strings).build();
                    ScalarField scalarField = ScalarField.newBuilder().setStringData(stringArray).build();
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
                case JSON: {
                    List<ByteString> byteStrings = objects.stream().map(p -> ByteString.copyFromUtf8(((JSONObject) p).toJSONString()))
                            .collect(Collectors.toList());
                    JSONArray jsonArray = JSONArray.newBuilder().addAllData(byteStrings).build();
                    ScalarField scalarField = ScalarField.newBuilder().setJsonData(jsonArray).build();
                    if (isDynamic) {
                        return builder.setType(dataType).setScalars(scalarField).setIsDynamic(true).build();
                    }
                    return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
                }
            }
        }

        return null;
    }

    /**
     * Convert a grpc field schema to client field schema
     *
     * @param field FieldSchema object
     * @return {@link FieldType} schema of the field
     */
    public static FieldType ConvertField(@NonNull FieldSchema field) {
        FieldType.Builder builder = FieldType.newBuilder()
                .withName(field.getName())
                .withDescription(field.getDescription())
                .withPrimaryKey(field.getIsPrimaryKey())
                .withAutoID(field.getAutoID())
                .withDataType(field.getDataType());

        if (field.getIsDynamic()) {
            builder.withIsDynamic(true);
        }

        List<KeyValuePair> keyValuePairs = field.getTypeParamsList();
        keyValuePairs.forEach((kv) -> builder.addTypeParam(kv.getKey(), kv.getValue()));

        return builder.build();
    }

    /**
     * Convert a client field schema to grpc field schema
     *
     * @param field {@link FieldType} object
     * @return {@link FieldSchema} schema of the field
     */
    public static FieldSchema ConvertField(@NonNull FieldType field) {
        FieldSchema.Builder builder = FieldSchema.newBuilder()
                .setIsPrimaryKey(field.isPrimaryKey())
                .setAutoID(field.isAutoID())
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setDataType(field.getDataType());
        Map<String, String> params = field.getTypeParams();
        params.forEach((key, value) -> builder.addTypeParams(KeyValuePair.newBuilder()
                .setKey(key).setValue(value).build()));

        return builder.build();
    }

    @Builder
    @Getter
    public static class InsertDataInfo {
        private String fieldName;
        private DataType dataType;
        private LinkedList<Object> data;
    }
}

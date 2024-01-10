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
import io.milvus.param.dml.UpsertParam;
import io.milvus.response.DescCollResponseWrapper;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
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
        checkFieldData(fieldSchema, values, false);
    }

    private static void checkFieldData(FieldType fieldSchema, List<?> values, boolean verifyElementType) {
        HashMap<DataType, String> errMsgs = getTypeErrorMsg();
        DataType dataType = verifyElementType ? fieldSchema.getElementType() : fieldSchema.getDataType();

        if (verifyElementType && values.size() > fieldSchema.getMaxCapacity()) {
            throw new ParamException(String.format("Array field '%s' length: %d exceeds max capacity: %d",
                    fieldSchema.getName(), values.size(), fieldSchema.getMaxCapacity()));
        }

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
                    if (!(value instanceof JSONObject)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }
                }
                break;
            case Array:
                for (Object value : values) {
                    if (!(value instanceof List)) {
                        throw new ParamException(String.format(errMsgs.get(dataType), fieldSchema.getName()));
                    }

                    List<?> temp = (List<?>)value;
                    checkFieldData(fieldSchema, temp, true);
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
        return metric == MetricType.L2 || metric == MetricType.IP || metric == MetricType.COSINE;
    }

    /**
     * Checks if a metric is for binary vector.
     *
     * @param metric metric type
     */
    public static boolean IsBinaryMetric(MetricType metric) {
        return metric != MetricType.INVALID && !IsFloatMetric(metric);
    }

    /**
     * Checks if an index type is for vector field.
     *
     * @param idx index type
     */
    public static boolean IsVectorIndex(IndexType idx) {
        return idx != IndexType.INVALID && idx != IndexType.None && idx.getCode() < IndexType.TRIE.getCode();
    }

    /**
     * Checks if an index type is matched with data type.
     *
     * @param indexType index type
     * @param dataType data type
     */
    public static boolean VerifyIndexType(IndexType indexType, DataType dataType) {
        // if user not specify the indexType, auto select by kernal
        if (indexType == IndexType.None) {
            return true;
        } else if (dataType == DataType.FloatVector) {
            return (IsVectorIndex(indexType) && (indexType != IndexType.BIN_FLAT) && (indexType != IndexType.BIN_IVF_FLAT));
        } else if (dataType == DataType.BinaryVector) {
            return indexType == IndexType.BIN_FLAT || indexType == IndexType.BIN_IVF_FLAT;
        } else if (dataType == DataType.VarChar) {
            return indexType == IndexType.TRIE;
        } else {
            return indexType == IndexType.STL_SORT;
        }
    }

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;

        public InsertBuilderWrapper(@NonNull InsertParam requestParam,
                                    DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getRowCount());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                insertBuilder.setDbName(requestParam.getDatabaseName());
            }
            fillFieldsData(requestParam, wrapper);
        }

        public InsertBuilderWrapper(@NonNull UpsertParam requestParam,
                                    DescCollResponseWrapper wrapper) {
            String collectionName = requestParam.getCollectionName();

            // currently, not allow to upsert for collection whose primary key is auto-generated
            FieldType pk = wrapper.getPrimaryField();
            if (pk.isAutoID()) {
                throw new ParamException(String.format("Upsert don't support autoID==True, collection: %s",
                        requestParam.getCollectionName()));
            }

            // generate upsert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getRowCount());
            if (StringUtils.isNotEmpty(requestParam.getDatabaseName())) {
                upsertBuilder.setDbName(requestParam.getDatabaseName());
            }
            fillFieldsData(requestParam, wrapper);
        }

        private void addFieldsData(io.milvus.grpc.FieldData value) {
            if (insertBuilder != null) {
                insertBuilder.addFieldsData(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.addFieldsData(value);
            }
        }

        private void setPartitionName(String value) {
            if (insertBuilder != null) {
                insertBuilder.setPartitionName(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.setPartitionName(value);
            }
        }

        private void fillFieldsData(InsertParam requestParam, DescCollResponseWrapper wrapper) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            boolean isPartitionKeyEnabled = false;
            for (FieldType fieldType : wrapper.getFields()) {
                if (fieldType.isPartitionKey()) {
                    isPartitionKeyEnabled = true;
                    break;
                }
            }
            if (isPartitionKeyEnabled) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new ParamException(msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<InsertParam.Field> columnFields = requestParam.getFields();
            List<JSONObject> rowFields = requestParam.getRows();

            if (CollectionUtils.isNotEmpty(columnFields)) {
                checkAndSetColumnData(wrapper, columnFields);
            } else {
                checkAndSetRowData(wrapper, rowFields);
            }
        }

        private void checkAndSetColumnData(DescCollResponseWrapper wrapper, List<InsertParam.Field> fields) {
            List<FieldType> fieldTypes = wrapper.getFields();

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
                        this.addFieldsData(genFieldData(fieldType, field.getValues()));
                        break;
                    }

                }
                if (!found && !fieldType.isAutoID()) {
                    String msg = "The field: " + fieldType.getName() + " is not provided.";
                    throw new ParamException(msg);
                }
            }

            // deal with dynamicField
            if (wrapper.getEnableDynamicField()) {
                for (InsertParam.Field field : fields) {
                    if (field.getName().equals(Constant.DYNAMIC_FIELD_NAME)) {
                        FieldType dynamicType = FieldType.newBuilder()
                                .withName(Constant.DYNAMIC_FIELD_NAME)
                                .withDataType(DataType.JSON)
                                .withIsDynamic(true)
                                .build();
                        checkFieldData(dynamicType, field);
                        this.addFieldsData(genFieldData(dynamicType, field.getValues(), true));
                        break;
                    }
                }
            }
        }

        private void checkAndSetRowData(DescCollResponseWrapper wrapper, List<JSONObject> rows) {
            List<FieldType> fieldTypes = wrapper.getFields();

            Map<String, InsertDataInfo> nameInsertInfo = new HashMap<>();
            InsertDataInfo insertDynamicDataInfo = InsertDataInfo.builder().fieldType(
                    FieldType.newBuilder()
                            .withName(Constant.DYNAMIC_FIELD_NAME)
                            .withDataType(DataType.JSON)
                            .withIsDynamic(true)
                            .build())
                    .data(new LinkedList<>()).build();
            for (JSONObject row : rows) {
                for (FieldType fieldType : fieldTypes) {
                    String fieldName = fieldType.getName();
                    InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, InsertDataInfo.builder()
                            .fieldType(fieldType).data(new LinkedList<>()).build());

                    // check normalField
                    Object rowFieldData = row.get(fieldName);
                    if (rowFieldData != null) {
                        if (fieldType.isAutoID()) {
                            String msg = "The primary key: " + fieldName + " is auto generated, no need to input.";
                            throw new ParamException(msg);
                        }
                        checkFieldData(fieldType, Lists.newArrayList(rowFieldData), false);

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

                // deal with dynamicField
                if (wrapper.getEnableDynamicField()) {
                    JSONObject dynamicField = new JSONObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!nameInsertInfo.containsKey(rowFieldName)) {
                            dynamicField.put(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.getData().add(dynamicField);
                }
            }

            for (String fieldNameKey : nameInsertInfo.keySet()) {
                InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
                this.addFieldsData(genFieldData(insertDataInfo.getFieldType(), insertDataInfo.getData()));
            }
            if (wrapper.getEnableDynamicField()) {
                this.addFieldsData(genFieldData(insertDynamicDataInfo.getFieldType(), insertDynamicDataInfo.getData(), Boolean.TRUE));
            }
        }

        public InsertRequest buildInsertRequest() {
            if (insertBuilder != null) {
                return insertBuilder.build();
            }
            throw new ParamException("Unable to build insert request since no input");
        }

        public UpsertRequest buildUpsertRequest() {
            if (upsertBuilder != null) {
                return upsertBuilder.build();
            }
            throw new ParamException("Unable to build upsert request since no input");
        }
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
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.IGNORE_GROWING)
                                .setValue(String.valueOf(requestParam.isIgnoreGrowing()))
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

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (requestParam.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode());
        }

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

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (requestParam.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(requestParam.getConsistencyLevel().getCode());
        }

        // set offset and limit value.
        // directly pass the two values, the server will verify them.
        long offset = requestParam.getOffset();
        if (offset > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.OFFSET)
                    .setValue(String.valueOf(offset))
                    .build());
        }

        long limit = requestParam.getLimit();
        if (limit > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.LIMIT)
                    .setValue(String.valueOf(limit))
                    .build());
        }

        // ignore growing
        builder.addQueryParams(KeyValuePair.newBuilder()
                .setKey(Constant.IGNORE_GROWING)
                .setValue(String.valueOf(requestParam.isIgnoreGrowing()))
                .build());

        return builder.build();
    }

    private static long getGuaranteeTimestamp(ConsistencyLevelEnum consistencyLevel,
                                              long guaranteeTimestamp, Long gracefulTime){
        if(consistencyLevel == null){
            return 1L;
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

    private static FieldData genFieldData(FieldType fieldType, List<?> objects) {
        return genFieldData(fieldType, objects, Boolean.FALSE);
    }

    @SuppressWarnings("unchecked")
    private static FieldData genFieldData(FieldType fieldType, List<?> objects, boolean isDynamic) {
        if (objects == null) {
            throw new ParamException("Cannot generate FieldData from null object");
        }
        DataType dataType = fieldType.getDataType();
        String fieldName = fieldType.getName();
        FieldData.Builder builder = FieldData.newBuilder();
        if (vectorDataType.contains(dataType)) {
            VectorField vectorField = genVectorField(dataType, objects);
            return builder.setFieldName(fieldName).setType(dataType).setVectors(vectorField).build();
        } else {
            ScalarField scalarField = genScalarField(fieldType, objects);
            if (isDynamic) {
                return builder.setType(dataType).setScalars(scalarField).setIsDynamic(true).build();
            }
            return builder.setFieldName(fieldName).setType(dataType).setScalars(scalarField).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static VectorField genVectorField(DataType dataType, List<?> objects) {
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
            return VectorField.newBuilder().setDim(dim).setFloatVector(floatArray).build();
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
            return VectorField.newBuilder().setDim(dim).setBinaryVector(byteString).build();
        }

        throw new ParamException("Illegal vector dataType:" + dataType);
    }

    private static ScalarField genScalarField(FieldType fieldType, List<?> objects) {
        if (fieldType.getDataType() == DataType.Array) {
            ArrayArray.Builder builder = ArrayArray.newBuilder();
            for (Object object : objects) {
                List<?> temp = (List<?>)object;
                ScalarField arrayField = genScalarField(fieldType.getElementType(), temp);
                builder.addData(arrayField);
            }

            return ScalarField.newBuilder().setArrayData(builder.build()).build();
        } else {
            return genScalarField(fieldType.getDataType(), objects);
        }
    }

    private static ScalarField genScalarField(DataType dataType, List<?> objects) {
        switch (dataType) {
            case None:
            case UNRECOGNIZED:
                throw new ParamException("Cannot support this dataType:" + dataType);
            case Int64: {
                List<Long> longs = objects.stream().map(p -> (Long) p).collect(Collectors.toList());
                LongArray longArray = LongArray.newBuilder().addAllData(longs).build();
                return ScalarField.newBuilder().setLongData(longArray).build();
            }
            case Int32:
            case Int16:
            case Int8: {
                List<Integer> integers = objects.stream().map(p -> p instanceof Short ? ((Short) p).intValue() : (Integer) p).collect(Collectors.toList());
                IntArray intArray = IntArray.newBuilder().addAllData(integers).build();
                return ScalarField.newBuilder().setIntData(intArray).build();
            }
            case Bool: {
                List<Boolean> booleans = objects.stream().map(p -> (Boolean) p).collect(Collectors.toList());
                BoolArray boolArray = BoolArray.newBuilder().addAllData(booleans).build();
                return ScalarField.newBuilder().setBoolData(boolArray).build();
            }
            case Float: {
                List<Float> floats = objects.stream().map(p -> (Float) p).collect(Collectors.toList());
                FloatArray floatArray = FloatArray.newBuilder().addAllData(floats).build();
                return ScalarField.newBuilder().setFloatData(floatArray).build();
            }
            case Double: {
                List<Double> doubles = objects.stream().map(p -> (Double) p).collect(Collectors.toList());
                DoubleArray doubleArray = DoubleArray.newBuilder().addAllData(doubles).build();
                return ScalarField.newBuilder().setDoubleData(doubleArray).build();
            }
            case String:
            case VarChar: {
                List<String> strings = objects.stream().map(p -> (String) p).collect(Collectors.toList());
                StringArray stringArray = StringArray.newBuilder().addAllData(strings).build();
                return ScalarField.newBuilder().setStringData(stringArray).build();
            }
            case JSON: {
                List<ByteString> byteStrings = objects.stream().map(p -> ByteString.copyFromUtf8(((JSONObject) p).toJSONString()))
                        .collect(Collectors.toList());
                JSONArray jsonArray = JSONArray.newBuilder().addAllData(byteStrings).build();
                return ScalarField.newBuilder().setJsonData(jsonArray).build();
            }
            default:
                throw new ParamException("Illegal scalar dataType:" + dataType);
        }
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
                .withPartitionKey(field.getIsPartitionKey())
                .withAutoID(field.getAutoID())
                .withDataType(field.getDataType())
                .withElementType(field.getElementType())
                .withIsDynamic(field.getIsDynamic());

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
                .setName(field.getName())
                .setDescription(field.getDescription())
                .setIsPrimaryKey(field.isPrimaryKey())
                .setIsPartitionKey(field.isPartitionKey())
                .setAutoID(field.isAutoID())
                .setDataType(field.getDataType())
                .setElementType(field.getElementType())
                .setIsDynamic(field.isDynamic());

        // assemble typeParams for CollectionSchema
        List<KeyValuePair> typeParamsList = AssembleKvPair(field.getTypeParams());
        if (CollectionUtils.isNotEmpty(typeParamsList)) {
            typeParamsList.forEach(builder::addTypeParams);
        }

        return builder.build();
    }

    public static List<KeyValuePair> AssembleKvPair(Map<String, String> sourceMap) {
        List<KeyValuePair> result = new ArrayList<>();

        if (MapUtils.isNotEmpty(sourceMap)) {
            sourceMap.forEach((key, value) -> {
                KeyValuePair kv = KeyValuePair.newBuilder()
                        .setKey(key)
                        .setValue(value).build();
                result.add(kv);
            });
        }
        return result;
    }

    @Builder
    @Getter
    public static class InsertDataInfo {
        private final FieldType fieldType;
        private final LinkedList<Object> data;
    }
}

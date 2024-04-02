package io.milvus.v2.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class DataUtils {
    private InsertRequest.Builder insertBuilder;
    private UpsertRequest.Builder upsertBuilder;
    private static final Set<DataType> vectorDataType = new HashSet<DataType>() {{
        add(DataType.FloatVector);
        add(DataType.BinaryVector);
    }};

    public InsertRequest convertGrpcInsertRequest(@NonNull InsertReq requestParam,
                                                  DescCollResponseWrapper wrapper) {
        String collectionName = requestParam.getCollectionName();

        // generate insert request builder
        MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
        insertBuilder = InsertRequest.newBuilder()
                .setCollectionName(collectionName)
                .setBase(msgBase)
                .setNumRows(requestParam.getData().size());
        upsertBuilder = null;
        fillFieldsData(requestParam, wrapper);
        return insertBuilder.build();
    }
    public UpsertRequest convertGrpcUpsertRequest(@NonNull UpsertReq requestParam,
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
                .setNumRows(requestParam.getData().size());
        insertBuilder = null;
        fillFieldsData(requestParam, wrapper);
        return upsertBuilder.build();
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

    private void fillFieldsData(UpsertReq requestParam, DescCollResponseWrapper wrapper) {
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
        List<JSONObject> rowFields = requestParam.getData();

        checkAndSetRowData(wrapper, rowFields);

    }

    private void fillFieldsData(InsertReq requestParam, DescCollResponseWrapper wrapper) {
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
        List<JSONObject> rowFields = requestParam.getData();

        checkAndSetRowData(wrapper, rowFields);

    }

    private void checkAndSetRowData(DescCollResponseWrapper wrapper, List<JSONObject> rows) {
        List<FieldType> fieldTypes = wrapper.getFields();

        Map<String, ParamUtils.InsertDataInfo> nameInsertInfo = new HashMap<>();
        ParamUtils.InsertDataInfo insertDynamicDataInfo = ParamUtils.InsertDataInfo.builder().fieldType(
                        FieldType.newBuilder()
                                .withName(Constant.DYNAMIC_FIELD_NAME)
                                .withDataType(DataType.JSON)
                                .withIsDynamic(true)
                                .build())
                .data(new LinkedList<>()).build();
        for (JSONObject row : rows) {
            for (FieldType fieldType : fieldTypes) {
                String fieldName = fieldType.getName();
                ParamUtils.InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, ParamUtils.InsertDataInfo.builder()
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
            ParamUtils.InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
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
        typeErrMsg.put(DataType.Float16Vector, "Type mismatch for field '%s': Float16 vector field's value type must be ByteBuffer");
        typeErrMsg.put(DataType.BFloat16Vector, "Type mismatch for field '%s': BFloat16 vector field's value type must be ByteBuffer");
        typeErrMsg.put(DataType.SparseFloatVector, "Type mismatch for field '%s': SparseFloatVector vector field's value type must be SortedMap");
        return typeErrMsg;
    }
}

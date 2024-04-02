package io.milvus.response;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ProtocolStringList;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.exception.IllegalResponseException;

import io.milvus.param.ParamUtils;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import static io.milvus.grpc.DataType.JSON;

/**
 * Utility class to wrap response of <code>query/search</code> interface.
 */
public class FieldDataWrapper {
    private final FieldData fieldData;

    public FieldDataWrapper(@NonNull FieldData fieldData) {
        this.fieldData = fieldData;
    }

    public boolean isVectorField() {
        return ParamUtils.isVectorDataType(fieldData.getType());
    }

    public boolean isJsonField() {
        return fieldData.getType() == JSON;
    }

    public boolean isDynamicField() {
        return fieldData.getType() == JSON && fieldData.getIsDynamic();
    }

    /**
     * Gets the dimension value of a vector field.
     * Throw {@link IllegalResponseException} if the field is not a vector filed.
     *
     * @return <code>int</code> dimension of the vector field
     */
    public int getDim() throws IllegalResponseException {
        if (!isVectorField()) {
            throw new IllegalResponseException("Not a vector field");
        }
        return (int) fieldData.getVectors().getDim();
    }

    // this method returns bytes size of each vector according to vector type
    private int checkDim(DataType dt, ByteString data, int dim) {
        if (dt == DataType.BinaryVector) {
            if ((data.size()*8) % dim != 0) {
                String msg = String.format("Returned binary vector data array size %d doesn't match dimension %d",
                        data.size(), dim);
                throw new IllegalResponseException(msg);
            }
            return dim/8;
        } else if (dt == DataType.Float16Vector || dt == DataType.BFloat16Vector) {
            if (data.size() % (dim*2) != 0) {
                String msg = String.format("Returned float16 vector data array size %d doesn't match dimension %d",
                        data.size(), dim);
                throw new IllegalResponseException(msg);
            }
            return dim*2;
        }

        return 0;
    }

    /**
     * Gets the row count of a field.
     * * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>long</code> row count of the field
     */
    public long getRowCount() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    String msg = String.format("Returned float vector data array size %d doesn't match dimension %d",
                            data.size(), dim);
                    throw new IllegalResponseException(msg);
                }

                return data.size()/dim;
            }
            case BinaryVector: {
                // for binary vector, each dimension is one bit, each byte is 8 dim
                int dim = getDim();
                ByteString data = fieldData.getVectors().getBinaryVector();
                int bytePerVec = checkDim(dt, data, dim);

                return data.size()/bytePerVec;
            }
            case Float16Vector:
            case BFloat16Vector: {
                // for float16 vector, each dimension 2 bytes
                int dim = getDim();
                ByteString data = (dt == DataType.Float16Vector) ?
                        fieldData.getVectors().getFloat16Vector() : fieldData.getVectors().getBfloat16Vector();
                int bytePerVec = checkDim(dt, data, dim);

                return data.size()/bytePerVec;
            }
            case SparseFloatVector: {
                // for sparse vector, each content is a vector
                return fieldData.getVectors().getSparseFloatVector().getContentsCount();
            }
            case Int64:
                return fieldData.getScalars().getLongData().getDataCount();
            case Int32:
            case Int16:
            case Int8:
                return fieldData.getScalars().getIntData().getDataCount();
            case Bool:
                return fieldData.getScalars().getBoolData().getDataCount();
            case Float:
                return fieldData.getScalars().getFloatData().getDataCount();
            case Double:
                return fieldData.getScalars().getDoubleData().getDataCount();
            case VarChar:
            case String:
                return fieldData.getScalars().getStringData().getDataCount();
            case JSON:
                return fieldData.getScalars().getJsonData().getDataCount();
            case Array:
                return fieldData.getScalars().getArrayData().getDataCount();
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Returns the field data according to its type:
     *      FloatVector field returns List of List Float,
     *      BinaryVector/Float16Vector/BFloat16Vector fields return List of ByteBuffer
     *      SparseFloatVector field returns List of SortedMap[Long, Float]
     *      Int64 field returns List of Long
     *      Int32/Int16/Int8 fields return List of Integer
     *      Bool field returns List of Boolean
     *      Float field returns List of Float
     *      Double field returns List of Double
     *      Varchar field returns List of String
     *      Array field returns List of List
     *      JSON field returns List of String;
     *      etc.
     *
     * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>List</code>
     */
    public List<?> getFieldData() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    String msg = String.format("Returned float vector data array size %d doesn't match dimension %d",
                            data.size(), dim);
                    throw new IllegalResponseException(msg);
                }

                List<List<Float>> packData = new ArrayList<>();
                int count = data.size() / dim;
                for (int i = 0; i < count; ++i) {
                    packData.add(data.subList(i * dim, (i + 1) * dim));
                }
                return packData;
            }
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector: {
                int dim = getDim();
                ByteString data = null;
                if (dt == DataType.BinaryVector) {
                    data = fieldData.getVectors().getBinaryVector();
                } else if (dt == DataType.Float16Vector) {
                    data = fieldData.getVectors().getFloat16Vector();
                } else {
                    data = fieldData.getVectors().getBfloat16Vector();
                }

                int bytePerVec = checkDim(dt, data, dim);
                int count = data.size()/bytePerVec;
                List<ByteBuffer> packData = new ArrayList<>();
                for (int i = 0; i < count; ++i) {
                    ByteBuffer bf = ByteBuffer.allocate(bytePerVec);
                    bf.put(data.substring(i * bytePerVec, (i + 1) * bytePerVec).toByteArray());
                    packData.add(bf);
                }
                return packData;
            }
            case SparseFloatVector: {
                // in Java sdk, each sparse vector is pairs of long+float
                // in server side, each sparse vector is stored as uint+float (8 bytes)
                // don't use sparseArray.getDim() because the dim is the max index of each rows
                SparseFloatArray sparseArray = fieldData.getVectors().getSparseFloatVector();
                List<SortedMap<Long, Float>> packData = new ArrayList<>();
                for (int i = 0; i < sparseArray.getContentsCount(); ++i) {
                    ByteString bs = sparseArray.getContents(i);
                    ByteBuffer bf = ByteBuffer.wrap(bs.toByteArray());
                    bf.order(ByteOrder.LITTLE_ENDIAN);
                    SortedMap<Long, Float> sparse = new TreeMap<>();
                    long num = bf.limit()/8; // each uint+float pair is 8 bytes
                    for (long j = 0; j < num; j++) {
                        // here we convert an uint 4-bytes to a long value
                        ByteBuffer pBuf = ByteBuffer.allocate(Long.BYTES);
                        pBuf.order(ByteOrder.LITTLE_ENDIAN);
                        int offset = 8*(int)j;
                        byte[] aa = bf.array();
                        for (int k = offset; k < offset + 4; k++) {
                            pBuf.put(aa[k]); // fill the first 4 bytes with the unit bytes
                        }
                        pBuf.putInt(0); // fill the last 4 bytes to zero
                        pBuf.rewind(); // reset position to head
                        long k = pBuf.getLong(); // this is the long value converted from the uint

                        // here we get the float value as normal
                        bf.position(offset+4); // position offsets 4 bytes since they were converted to long
                        float v = bf.getFloat(); // this is the float value
                        sparse.put(k, v);
                    }
                    packData.add(sparse);
                }
                return packData;
            }
            case Array:
                List<List<?>> array = new ArrayList<>();
                ArrayArray arrArray = fieldData.getScalars().getArrayData();
                for (int i = 0; i < arrArray.getDataCount(); i++) {
                    ScalarField scalar = arrArray.getData(i);
                    array.add(getScalarData(arrArray.getElementType(), scalar));
                }
                return array;
            case Int64:
            case Int32:
            case Int16:
            case Int8:
            case Bool:
            case Float:
            case Double:
            case VarChar:
            case String:
            case JSON:
                return getScalarData(dt, fieldData.getScalars());
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    private List<?> getScalarData(DataType dt, ScalarField scalar) {
        switch (dt) {
            case Int64:
                return scalar.getLongData().getDataList();
            case Int32:
            case Int16:
            case Int8:
                return scalar.getIntData().getDataList();
            case Bool:
                return scalar.getBoolData().getDataList();
            case Float:
                return scalar.getFloatData().getDataList();
            case Double:
                return scalar.getDoubleData().getDataList();
            case VarChar:
            case String:
                ProtocolStringList protoStrList = scalar.getStringData().getDataList();
                return protoStrList.subList(0, protoStrList.size());
            case JSON:
                List<ByteString> dataList = scalar.getJsonData().getDataList();
                return dataList.stream().map(ByteString::toStringUtf8).collect(Collectors.toList());
            default:
                return new ArrayList<>();
        }
    }

    public Integer getAsInt(int index, String paramName) throws IllegalResponseException {
        if (isJsonField()) {
            String result = getAsString(index, paramName);
            return result == null ? null : Integer.parseInt(result);
        }
        throw new IllegalResponseException("Only JSON type support this operation");
    }

    public String getAsString(int index, String paramName) throws IllegalResponseException {
        if (isJsonField()) {
            JSONObject jsonObject = parseObjectData(index);
            return jsonObject.getString(paramName);
        }
        throw new IllegalResponseException("Only JSON type support this operation");
    }

    public Boolean getAsBool(int index, String paramName) throws IllegalResponseException {
        if (isJsonField()) {
            String result = getAsString(index, paramName);
            return result == null ? null : Boolean.parseBoolean(result);
        }
        throw new IllegalResponseException("Only JSON type support this operation");
    }

    public Double getAsDouble(int index, String paramName) throws IllegalResponseException {
        if (isJsonField()) {
            String result = getAsString(index, paramName);
            return result == null ? null : Double.parseDouble(result);
        }
        throw new IllegalResponseException("Only JSON type support this operation");
    }

    public Object get(int index, String paramName) throws IllegalResponseException {
        if (isJsonField()) {
            JSONObject jsonObject = parseObjectData(index);
            return jsonObject.get(paramName);
        }
        throw new IllegalResponseException("Only JSON type support this operation");
    }

    public Object valueByIdx(int index) throws ParamException {
        List<?> data = getFieldData();
        if (index < 0 || index >= data.size()) {
            throw new ParamException(String.format("Value index %d out of range %d", index, data.size()));
        }
        return data.get(index);
    }

    private JSONObject parseObjectData(int index) {
        Object object = valueByIdx(index);
        return ParseJSONObject(object);
    }

    public static JSONObject ParseJSONObject(Object object) {
        if (object instanceof String) {
            return JSONObject.parseObject((String)object);
        } else if (object instanceof byte[]) {
            return JSONObject.parseObject(new String((byte[]) object));
        } else {
            throw new IllegalResponseException("Illegal type value for JSON parser");
        }
    }
}

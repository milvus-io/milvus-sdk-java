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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import io.milvus.exception.IllegalResponseException;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.ParamUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

import static io.milvus.grpc.DataType.JSON;

/**
 * Utility class to wrap response of <code>query/search</code> interface.
 */
public class FieldDataWrapper {
    private final FieldData fieldData;
    private List<?> cacheData = null;

    public FieldDataWrapper(FieldData fieldData) {
        if (fieldData == null) {
            throw new IllegalArgumentException("FieldData cannot be null");
        }
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
        return getDimInternal(fieldData.getVectors());
    }

    private int getDimInternal(VectorField vector) {
        return (int) vector.getDim();
    }

    // this method returns bytes size of each vector according to vector type
    // for binary vector, each dimension is one bit, each byte is 8 dim
    // for int8 vector, each dimension is ony byte, each byte is one dim
    // for float16 vector, each dimension 2 bytes
    private int checkDim(DataType dt, ByteString data, int dim) {
        if (dt == DataType.BinaryVector) {
            if ((data.size() * 8) % dim != 0) {
                String msg = String.format("Returned binary vector data array size %d doesn't match dimension %d",
                        data.size(), dim);
                throw new IllegalResponseException(msg);
            }
            return dim / 8;
        } else if (dt == DataType.Float16Vector || dt == DataType.BFloat16Vector) {
            if (data.size() % (dim * 2) != 0) {
                String msg = String.format("Returned float16 vector data array size %d doesn't match dimension %d",
                        data.size(), dim);
                throw new IllegalResponseException(msg);
            }
            return dim * 2;
        } else if (dt == DataType.Int8Vector) {
            if (data.size() % dim != 0) {
                String msg = String.format("Returned int8 vector data array size %d doesn't match dimension %d",
                        data.size(), dim);
                throw new IllegalResponseException(msg);
            }
            return dim;
        }

        return 0;
    }

    private ByteString getVectorBytes(VectorField vd, DataType dt) {
        ByteString data;
        if (dt == DataType.BinaryVector) {
            data = vd.getBinaryVector();
        } else if (dt == DataType.Float16Vector) {
            data = vd.getFloat16Vector();
        } else if (dt == DataType.BFloat16Vector) {
            data = vd.getBfloat16Vector();
        } else if (dt == DataType.Int8Vector) {
            data = vd.getInt8Vector();
        } else {
            String msg = String.format("Unsupported data type %s returned by FieldData", dt.name());
            throw new IllegalResponseException(msg);
        }
        return data;
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

                return data.size() / dim;
            }
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector:
            case Int8Vector: {
                int dim = getDim();
                ByteString data = getVectorBytes(fieldData.getVectors(), dt);
                int bytePerVec = checkDim(dt, data, dim);

                return data.size() / bytePerVec;
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
            case Timestamptz:
                return fieldData.getScalars().getStringData().getDataCount();
            case Geometry:
                return fieldData.getScalars().getGeometryWktData().getDataCount();
            case JSON:
                return fieldData.getScalars().getJsonData().getDataCount();
            case Array:
                return fieldData.getScalars().getArrayData().getDataCount();
            case ArrayOfStruct: {
                List<FieldData> structData = fieldData.getStructArrays().getFieldsList();
                for (FieldData fd : structData) {
                    if (fd.getType() == DataType.Array) {
                        return fd.getScalars().getArrayData().getDataCount();
                    } else if (fd.getType() == DataType.ArrayOfVector) {
                        FieldDataWrapper tempWrapper = new FieldDataWrapper(fd);
                        return tempWrapper.getRowCount();
                    }
                }
            }
            case ArrayOfVector: {
                return fieldData.getVectors().getVectorArray().getDataCount();
            }
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Returns the field data according to its type:
     * FloatVector field returns List of List Float,
     * BinaryVector/Float16Vector/BFloat16Vector fields return List of ByteBuffer
     * SparseFloatVector field returns List of SortedMap[Long, Float]
     * Int64 field returns List of Long
     * Int32/Int16/Int8 fields return List of Integer
     * Bool field returns List of Boolean
     * Float field returns List of Float
     * Double field returns List of Double
     * Varchar field returns List of String
     * Array field returns List of List
     * JSON field returns List of String;
     * Struct field returns List of List<Map<String, Object>>
     * etc.
     * <p>
     * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>List</code>
     */
    public List<?> getFieldData() throws IllegalResponseException {
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = getFieldDataInternal();
        return cacheData;
    }

    private List<?> getFieldDataInternal() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector:
            case BinaryVector:
            case Float16Vector:
            case BFloat16Vector:
            case Int8Vector:
            case SparseFloatVector:
                return getVectorData(dt, fieldData.getVectors());
            case Array:
            case Int64:
            case Int32:
            case Int16:
            case Int8:
            case Bool:
            case Float:
            case Double:
            case VarChar:
            case String:
            case Geometry:
            case Timestamptz:
            case JSON:
                return getScalarData(dt, fieldData.getScalars(), fieldData.getValidDataList());
            case ArrayOfStruct:
                return getStructData(fieldData.getStructArrays(), fieldData.getFieldName());
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    private List<?> setNoneData(List<?> data, List<Boolean> validData) {
        if (validData != null && validData.size() == data.size()) {
            List<?> newData = new ArrayList<>(data); // copy the list since the data is come from grpc is not mutable
            for (int i = 0; i < validData.size(); i++) {
                if (validData.get(i) == Boolean.FALSE) {
                    newData.set(i, null);
                }
            }
            return newData;
        }
        return data;
    }

    private List<?> getVectorData(DataType dt, VectorField vector) {
        switch (dt) {
            case FloatVector: {
                int dim = getDimInternal(vector);
                List<Float> data = vector.getFloatVector().getDataList();
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
            case BFloat16Vector:
            case Int8Vector: {
                int dim = getDimInternal(vector);
                ByteString data = getVectorBytes(vector, dt);
                int bytePerVec = checkDim(dt, data, dim);
                int count = data.size() / bytePerVec;
                List<ByteBuffer> packData = new ArrayList<>();
                for (int i = 0; i < count; ++i) {
                    ByteBuffer bf = ByteBuffer.allocate(bytePerVec);
                    // binary vector doesn't care endian since each byte is independent
                    // fp16/bf16/int8 vector is sensitive to endian because each dim occupies 1~2 bytes,
                    // milvus server stores fp16/bf16/int8 vector as little endian
                    bf.order(ByteOrder.LITTLE_ENDIAN);
                    bf.put(data.substring(i * bytePerVec, (i + 1) * bytePerVec).toByteArray());
                    packData.add(bf);
                }
                return packData;
            }
            case SparseFloatVector: {
                // in Java sdk, each sparse vector is pairs of long+float
                // in server side, each sparse vector is stored as uint+float (8 bytes)
                // don't use sparseArray.getDim() because the dim is the max index of each rows
                SparseFloatArray sparseArray = vector.getSparseFloatVector();
                List<SortedMap<Long, Float>> packData = new ArrayList<>();
                for (int i = 0; i < sparseArray.getContentsCount(); ++i) {
                    ByteString bs = sparseArray.getContents(i);
                    ByteBuffer bf = ByteBuffer.wrap(bs.toByteArray());
                    SortedMap<Long, Float> sparse = ParamUtils.decodeSparseFloatVector(bf);
                    packData.add(sparse);
                }
                return packData;
            }
            default:
                return new ArrayList<>();
        }
    }

    private List<?> getScalarData(DataType dt, ScalarField scalar, List<Boolean> validData) {
        switch (dt) {
            case Int64:
                return setNoneData(scalar.getLongData().getDataList(), validData);
            case Int32:
            case Int16:
            case Int8:
                return setNoneData(scalar.getIntData().getDataList(), validData);
            case Bool:
                return setNoneData(scalar.getBoolData().getDataList(), validData);
            case Float:
                return setNoneData(scalar.getFloatData().getDataList(), validData);
            case Double:
                return setNoneData(scalar.getDoubleData().getDataList(), validData);
            case VarChar:
            case String:
            case Timestamptz: {
                ProtocolStringList protoStrList = scalar.getStringData().getDataList();
                return setNoneData(protoStrList.subList(0, protoStrList.size()), validData);
            }
            case Geometry: {
                ProtocolStringList protoGeoList = scalar.getGeometryWktData().getDataList();
                return setNoneData(protoGeoList.subList(0, protoGeoList.size()), validData);
            }
            case JSON: {
                List<ByteString> dataList = scalar.getJsonData().getDataList();
                return dataList.stream().map(ByteString::toStringUtf8).collect(Collectors.toList());
            }
            case Array: {
                List<List<?>> array = new ArrayList<>();
                ArrayArray arrArray = scalar.getArrayData();
                boolean nullable = validData != null && validData.size() == arrArray.getDataCount();
                for (int i = 0; i < arrArray.getDataCount(); i++) {
                    if (nullable && validData.get(i) == Boolean.FALSE) {
                        array.add(null);
                    } else {
                        ScalarField rowData = arrArray.getData(i);
                        array.add(getScalarData(arrArray.getElementType(), rowData, null));
                    }
                }
                return array;
            }
            default:
                return new ArrayList<>();
        }
    }

    private List<?> getStructData(StructArrayField field, String fieldName) {
        List<List<Map<String, Object>>> packData = new ArrayList<>();
        if (field.getFieldsCount() == 0) {
            return packData;
        }

        // read column data from FieldData
        // for a struct with two sub-fields "int" and "emb", search with nq=2, topk=3
        // the column data is like this:
        //  {
        //     "int": [[x1, x2], [x1, x2, x3], [x1], [x1, x2], [x1, x2, x3], [x1]],
        //     "emb": [[emb1, emb2], [emb1, emb2, emb3], [emb1], [emb1m emb2], [emb1, emb2, emb3], [emb1]],
        //  }
        Map<String, List<List<?>>> columnsData = new HashMap<>();
        int rowCount = 0;
        for (FieldData fd : field.getFieldsList()) {
            List<List<?>> column = new ArrayList<>();
            if (fd.getType() == DataType.Array) {
                column = (List<List<?>>) getScalarData(fd.getType(), fd.getScalars(), fd.getValidDataList());
                columnsData.put(fd.getFieldName(), column);
                rowCount = column.size();
            } else if (fd.getType() == DataType.ArrayOfVector) {
                VectorArray vecArr = fd.getVectors().getVectorArray();
                for (VectorField vf : vecArr.getDataList()) {
                    List<?> vector = getVectorData(vecArr.getElementType(), vf);
                    column.add(vector);
                }
                rowCount = column.size();
                columnsData.put(fd.getFieldName(), column);
            } else {
                throw new IllegalResponseException("Unsupported data type returned by StructArrayField");
            }
        }

        // convert column data into struct list, eventually, the packData is like this:
        //   [
        //      [{x1, emb1}, {x2, emb2}],
        //      [{x1, emb1}, {x2, emb2}, {x3, emb3}],
        //      [{x1, emb1}],
        //      [{x1, emb1}, {x2, emb2}],
        //      [{x1, emb1}, {x2, emb2}, {x3, emb3}],
        //      [{x1, emb1}]
        //   ]
        for (int i = 0; i < rowCount; i++) {
            int elementCount = 0;
            Map<String, List<?>> rowColumn = new HashMap<>();
            for (String key : columnsData.keySet()) {
                List<?> val = columnsData.get(key).get(i);
                rowColumn.put(key, val);
                elementCount = val.size();
            }

            List<Map<String, Object>> structs = new ArrayList<>();
            for (int k = 0; k < elementCount; k++) {
                Map<String, Object> struct = new HashMap<>();
                int finalK = k;
                rowColumn.forEach((key, val) -> struct.put(key, val.get(finalK)));
                structs.add(struct);
            }
            packData.add(structs);
        }
        return packData;
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
            JsonElement jsonElement = parseObjectData(index);
            if (jsonElement instanceof JsonObject) {
                return ((JsonObject) jsonElement).get(paramName).getAsString();
            } else {
                throw new IllegalResponseException("The JSON element is not a dict");
            }
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

    /**
     * Gets a field's value by field name.
     *
     * @param index     which row
     * @param paramName which field
     * @return returns Long for integer value, returns Double for decimal value,
     * returns String for string value, returns JsonElement for JSON object and Array.
     */
    public Object get(int index, String paramName) throws IllegalResponseException {
        if (!isJsonField()) {
            throw new IllegalResponseException("Only JSON type support this operation");
        }

        JsonElement jsonElement = parseObjectData(index);
        if (!(jsonElement instanceof JsonObject)) {
            throw new IllegalResponseException("The JSON element is not a dict");
        }

        JsonElement element = ((JsonObject) jsonElement).get(paramName);
        return ValueOfJSONElement(element);
    }

    public Object valueByIdx(int index) throws ParamException {
        List<?> data = getFieldData();
        if (index < 0 || index >= data.size()) {
            throw new ParamException(String.format("Value index %d out of range %d", index, data.size()));
        }
        return data.get(index);
    }

    private JsonElement parseObjectData(int index) {
        Object object = valueByIdx(index);
        return ParseJSONObject(object);
    }

    public static JsonElement ParseJSONObject(Object object) {
        if (object == null) {
            throw new IllegalResponseException("Object cannot be null");
        }
        if (object instanceof String) {
            return JsonParser.parseString((String) object);
        } else if (object instanceof byte[]) {
            return JsonParser.parseString(new String((byte[]) object));
        } else {
            throw new IllegalResponseException("Illegal type value for JSON parser");
        }
    }

    public static Object ValueOfJSONElement(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = (JsonPrimitive) element;
            if (primitive.isString()) {
                return primitive.getAsString();
            } else if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else if (primitive.isNumber()) {
                if (primitive.getAsBigDecimal().scale() == 0) {
                    return primitive.getAsLong();
                } else {
                    return primitive.getAsDouble();
                }
            }
        }
        return element;
    }
}

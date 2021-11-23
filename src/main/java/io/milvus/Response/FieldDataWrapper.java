package io.milvus.Response;

import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.exception.IllegalResponseException;

import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

/**
 * Util class to wrap response of <code>query/search</code> interface.
 */
public class FieldDataWrapper {
    private final FieldData fieldData;

    public FieldDataWrapper(@NonNull FieldData fieldData) {
        this.fieldData = fieldData;
    }

    public boolean isVectorField() {
        return fieldData.getType() == DataType.FloatVector || fieldData.getType() == DataType.BinaryVector;
    }

    /**
     * Get dimension of a vector field.
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

    /**
     * Get row count of a field.
     * * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>long</code> row count of the field
     */
    public long getRowCount() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                System.out.println(fieldData.getVectors().getFloatVector().getDataCount());
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned float vector field data array size doesn't match dimension");
                }

                return data.size()/dim;
            }
            case BinaryVector: {
                int dim = getDim();
                ByteString data = fieldData.getVectors().getBinaryVector();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned binary vector field data array size doesn't match dimension");
                }

                return data.size()/dim;
            }
            case Int64:
                return fieldData.getScalars().getLongData().getDataList().size();
            case Int32:
            case Int16:
            case Int8:
                return fieldData.getScalars().getIntData().getDataList().size();
            case Bool:
                return fieldData.getScalars().getBoolData().getDataList().size();
            case Float:
                return fieldData.getScalars().getFloatData().getDataList().size();
            case Double:
                return fieldData.getScalars().getDoubleData().getDataList().size();
            case String:
                return fieldData.getScalars().getStringData().getDataList().size();
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Return field data according to its type:
     *      float vector field return List<List<Float>>
     *      binary vector field return List<ByteBuffer>
     *      int64 field return List<Long>
     *      boolean field return List<Boolean>
     *
     * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>List<?></?></code>
     */
    public List<?> getFieldData() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                System.out.println(fieldData.getVectors().getFloatVector().getDataCount());
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned float vector field data array size doesn't match dimension");
                }

                List<List<Float>> packData = new ArrayList<>();
                int count = data.size() / dim;
                for (int i = 0; i < count; ++i) {
                    packData.add(data.subList(i * dim, (i + 1) * dim));
                }
                return packData;
            }
            case BinaryVector: {
                int dim = getDim();
                ByteString data = fieldData.getVectors().getBinaryVector();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned binary vector field data array size doesn't match dimension");
                }

                List<ByteBuffer> packData = new ArrayList<>();
                int count = data.size() / dim;
                for (int i = 0; i < count; ++i) {
                    ByteBuffer bf = ByteBuffer.allocate(dim);
                    bf.put(data.substring(i * dim, (i + 1) * dim).toByteArray());
                    packData.add(bf);
                }
                return packData;
            }
            case Int64:
                return fieldData.getScalars().getLongData().getDataList();
            case Int32:
            case Int16:
            case Int8:
                return fieldData.getScalars().getIntData().getDataList();
            case Bool:
                return fieldData.getScalars().getBoolData().getDataList();
            case Float:
                return fieldData.getScalars().getFloatData().getDataList();
            case Double:
                return fieldData.getScalars().getDoubleData().getDataList();
            case String:
                return fieldData.getScalars().getStringData().getDataList();
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }
}

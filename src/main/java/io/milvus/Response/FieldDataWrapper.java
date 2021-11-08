package io.milvus.Response;

import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.exception.IllegalResponseException;

import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

public class FieldDataWrapper {
    private final FieldData fieldData;

    public FieldDataWrapper(@NonNull FieldData fieldData) {
        this.fieldData = fieldData;
    }

    public boolean isVectorField() {
        return fieldData.getType() == DataType.FloatVector || fieldData.getType() == DataType.BinaryVector;
    }

    public int getDim() {
        return (int) fieldData.getVectors().getDim();
    }

    // the return type is determined by field type:
    // float vector field return List<List<Float>>
    // binary vector field return List<ByteBuffer>
    // int64 field return List<Long>
    // boolean field return List<Boolean>
    public List<?> getFieldData() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        int dim = getDim();
        switch (dt) {
            case FloatVector: {
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
                ByteString data = fieldData.getVectors().getBinaryVector();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned binary vector field data array size doesn't match dimension");
                }

                List<ByteBuffer> packData = new ArrayList<>();
                int count = data.size() / dim;
                for (int i = 0; i < count; ++i) {
                    packData.add(data.substring(i * dim, (i + 1) * dim).asReadOnlyByteBuffer());
                }
                return packData;
            }
            case Int64:
            case Int32:
            case Int16:
                return fieldData.getScalars().getLongData().getDataList();
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

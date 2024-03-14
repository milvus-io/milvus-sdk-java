package io.milvus.bulkwriter.common.clientenum;

import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;

public enum TypeSize {
    BOOL(DataType.Bool, 1),
    INT8(DataType.Int8, 1),
    INT16(DataType.Int16, 2),
    INT32(DataType.Int32, 4),
    INT64(DataType.Int64, 8),
    FLOAT(DataType.Float, 4),
    DOUBLE(DataType.Double, 8),

    ;
    private DataType dataType;
    private Integer size;

    TypeSize(DataType dataType, Integer size) {
        this.dataType = dataType;
        this.size = size;
    }

    public static boolean contains(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return true;
            }
        }
        return false;
    }

    public static Integer getSize(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return typeSize.size;
            }
        }
        throw new ParamException("TypeSize not contains this dataType: " + dataType);
    }

}

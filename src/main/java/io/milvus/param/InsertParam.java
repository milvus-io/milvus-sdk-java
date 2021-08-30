package io.milvus.param;

import io.milvus.grpc.DataType;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * fieldNames,dataTypes, fieldValues' order must be consistent.
 * explain fieldValues:
 *    if dataType is scalar: ? is basic type, like Integer,Long...
 *    if dataType is FloatVector: ? is List<Float>
 */
public class InsertParam {
    private final String collectionName;
    private final String partitionName;
    //for check collectionFields
    private final int fieldNum;
    // field's name
    private final List<String> fieldNames;
    // field's dataType
    private final List<DataType> dataTypes;
    // field's values
    private final List<List<?>> fieldValues;

    private InsertParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.fieldNum = builder.fieldNum;
        this.fieldNames = builder.fieldNames;
        this.dataTypes = builder.dataTypes;
        this.fieldValues = builder.fieldValues;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getFieldNum() {
        return fieldNum;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public List<List<?>> getFieldValues() {
        return fieldValues;
    }

    public static class Builder {
        private final String collectionName;
        private String partitionName = "_default";
        private int fieldNum;
        private List<String> fieldNames;
        private List<DataType> dataTypes;
        private List<List<?>> fieldValues;

        private Builder(@Nonnull String collectionName) {
            this.collectionName = collectionName;
        }

        public static Builder nweBuilder(@Nonnull String collectionName) {
            return new Builder(collectionName);
        }

        public Builder setPartitionName(@Nonnull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder setFieldNum(int fieldNum) {
            this.fieldNum = fieldNum;
            return this;
        }

        public Builder setFieldNames(@Nonnull List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setDataTypes(@Nonnull List<DataType> dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public Builder setFieldValues(@Nonnull List<List<?>> fieldValues) {
            this.fieldValues = fieldValues;
            return this;
        }

        public InsertParam build() {
            return new InsertParam(this);
        }

    }
}

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

package io.milvus.param.dml;

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

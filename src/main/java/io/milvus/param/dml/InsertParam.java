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

import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * InsertParam.Field.values:
 * if dataType is scalar: values is List<Integer>, List<Long>...
 * if dataType is FloatVector: values is List<List<Float>>
 * if dataType is BinaryVector: values is List<ByteBuffer>
 */
public class InsertParam {
    private final List<Field> fields;

    private final String collectionName;
    private final String partitionName;
    private final int row_count;

    private InsertParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.fields = builder.fields;
        this.row_count = builder.row_count;
    }

    public List<Field> getFields() {
        return fields;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getRowCount() {
        return row_count;
    }

    public static class Builder {
        private String collectionName;
        private String partitionName = "_default";
        private List<InsertParam.Field> fields;
        private int row_count;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withPartitionName(@Nonnull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder withFields(@Nonnull List<InsertParam.Field> fields) {
            this.fields = fields;
            return this;
        }

        public InsertParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (fields.isEmpty()) {
                throw new ParamException("Fields cannot be empty");
            }

            for (InsertParam.Field field : fields) {
                if (field == null) {
                    throw new ParamException("Field cannot be null");
                }

                ParamUtils.CheckNullEmptyString(field.getName(), "Field name");

                if (field.getValues() == null || field.getValues().isEmpty()) {
                    throw new ParamException("Field value cannot be empty");
                }
            }

            // check row count
            int count = fields.get(0).getValues().size();
            for (InsertParam.Field field : fields) {
                if (field.getValues().size() != count) {
                    throw new ParamException("Row count of fields must be equal");
                }
            }
            this.row_count = count;

            if (count == 0) {
                throw new ParamException("Row count is zero");
            }

            // check value type and vector dimension
            for (InsertParam.Field field : fields) {
                List<?> values = field.getValues();
                if (field.getType() == DataType.FloatVector) {
                    if (!(values.get(0) instanceof List)) {
                        throw new ParamException("Float vector field's value must be Lst<Float>");
                    }
                    List first = (List) values.get(0);
                    if (!(first.get(0) instanceof Float)) {
                        throw new ParamException("Float vector field's value must be Lst<Float>");
                    }

                    int dim = first.size();
                    for (int i = 1; i < values.size(); ++i) {
                        List temp = (List) values.get(i);
                        if (dim != temp.size()) {
                            throw new ParamException("Vector dimension must be equal");
                        }
                    }
                } else if (field.getType() == DataType.BinaryVector) {
                    if (!(values.get(0) instanceof ByteBuffer)) {
                        throw new ParamException("Binary vector field's value must be ByteBuffer");
                    }

                    ByteBuffer first = (ByteBuffer) values.get(0);
                    int dim = first.position();
                    for (int i = 1; i < values.size(); ++i) {
                        ByteBuffer temp = (ByteBuffer) values.get(i);
                        if (dim != temp.position()) {
                            throw new ParamException("Vector dimension must be equal");
                        }
                    }
                }
            }

            return new InsertParam(this);
        }
    }

    public static class Field {
        private final String name;
        private final DataType type;
        private final List<?> values;

        public Field(String name, DataType type, List<?> values) {
            this.name = name;
            this.type = type;
            this.values = values;
        }

        public String getName() {
            return name;
        }

        public DataType getType() {
            return type;
        }

        public List<?> getValues() {
            return values;
        }
    }
}

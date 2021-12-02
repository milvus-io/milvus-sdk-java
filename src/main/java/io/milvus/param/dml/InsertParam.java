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

import lombok.Getter;
import lombok.NonNull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Parameters for <code>insert</code> interface.
 */
@Getter
public class InsertParam {
    private final List<Field> fields;

    private final String collectionName;
    private final String partitionName;
    private final int rowCount;

    private InsertParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.fields = builder.fields;
        this.rowCount = builder.rowCount;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>InsertParam</code> class.
     */
    public static class Builder {
        private String collectionName;
        private String partitionName = "_default";
        private List<InsertParam.Field> fields;
        private int rowCount;

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Set partition name (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the data to insert. The field list cannot be empty.
         * @see InsertParam.Field
         *
         * @param fields insert data
         * @return <code>Builder</code>
         */
        public Builder withFields(@NonNull List<InsertParam.Field> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Verifies parameters and creates a new <code>InsertParam</code> instance.
         *
         * @return <code>InsertParam</code>
         */
        @java.lang.SuppressWarnings("unchecked")
        public InsertParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (fields.isEmpty()) {
                throw new ParamException("Fields cannot be empty");
            }

            for (InsertParam.Field field : fields) {
                if (field == null) {
                    throw new ParamException("Field cannot be null." +
                            " If the field is auto-id, just ignore it from withFields()");
                }

                ParamUtils.CheckNullEmptyString(field.getName(), "Field name");

                if (field.getValues() == null || field.getValues().isEmpty()) {
                    throw new ParamException("Field value cannot be empty." +
                            " If the field is auto-id, just ignore it from withFields()");
                }
            }

            // check row count
            int count = fields.get(0).getValues().size();
            for (InsertParam.Field field : fields) {
                if (field.getValues().size() != count) {
                    throw new ParamException("Row count of fields must be equal");
                }
            }
            this.rowCount = count;

            if (count == 0) {
                throw new ParamException("Row count is zero");
            }

            // check value type and vector dimension
            for (InsertParam.Field field : fields) {
                List<?> values = field.getValues();
                if (field.getType() == DataType.FloatVector) {
                    for (Object obj : values) {
                        if (!(obj instanceof List)) {
                            throw new ParamException("Float vector field's value must be Lst<Float>");
                        }

                        List<?> temp = (List<?>)obj;
                        for (Object v : temp) {
                            if (!(v instanceof Float)) {
                                throw new ParamException("Float vector's value type must be Float");
                            }
                        }
                    }

                    List<Float> first = (List<Float>) values.get(0);
                    int dim = first.size();
                    for (int i = 1; i < values.size(); ++i) {
                        List<Float> temp = (List<Float>) values.get(i);
                        if (dim != temp.size()) {
                            throw new ParamException("Vector dimension must be equal");
                        }
                    }
                } else if (field.getType() == DataType.BinaryVector) {
                    for (Object obj : values) {
                        if (!(obj instanceof ByteBuffer)) {
                            throw new ParamException("Binary vector field's type must be ByteBuffer");
                        }
                    }

                    ByteBuffer first = (ByteBuffer) values.get(0);
                    int dim = first.position();
                    for (int i = 1; i < values.size(); ++i) {
                        ByteBuffer temp = (ByteBuffer) values.get(i);
                        if (dim != temp.position()) {
                            throw new ParamException("Vector dimension must be equal");
                        }
                    }
                } else if (field.getType() == DataType.Int64) {
                    for (Object obj : values) {
                        if (!(obj instanceof Long)) {
                            throw new ParamException("Int64 field value type must be Long");
                        }
                    }
                } else if (field.getType() == DataType.Int32 || field.getType() == DataType.Int16
                        || field.getType() == DataType.Int8 ) {
                    for (Object obj : values) {
                        if (!(obj instanceof Integer) && !(obj instanceof Short)) {
                            throw new ParamException("Int32/Int16/Int8 field value type must be Integer or Short");
                        }
                    }
                } else if (field.getType() == DataType.Float) {
                    for (Object obj : values) {
                        if (!(obj instanceof Float)) {
                            throw new ParamException("Float field value type must be Float");
                        }
                    }
                } else if (field.getType() == DataType.Double) {
                    for (Object obj : values) {
                        if (!(obj instanceof Double)) {
                            throw new ParamException("Double field value type must be Double");
                        }
                    }
                } else if (field.getType() == DataType.Bool) {
                    for (Object obj : values) {
                        if (!(obj instanceof Boolean)) {
                            throw new ParamException("Bool field value type must be Boolean");
                        }
                    }
                } else if (field.getType() == DataType.String) {
                    for (Object obj : values) {
                        if (!(obj instanceof String)) {
                            throw new ParamException("String field value type must be String");
                        }
                    }
                }
            }

            return new InsertParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by <code>InsertParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "InsertParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", row_count=" + rowCount +
                '}';
    }

    /**
     * Internal class for insert data.
     * if dataType is scalar(bool/int/float/double): values is List<Integer>, List<Long>...
     * if dataType is FloatVector: values is List<List<Float>>
     * if dataType is BinaryVector: values is List<ByteBuffer>
     */
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

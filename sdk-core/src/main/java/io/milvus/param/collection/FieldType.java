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

package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for a collection field.
 *
 * @see CreateCollectionParam
 */
public class FieldType {
    private final String name;
    private final boolean primaryKey;
    private final String description;
    private final DataType dataType;
    private final Map<String, String> typeParams;
    private final boolean autoID;
    private final boolean partitionKey;
    private final boolean clusteringKey;
    private final boolean isDynamic;
    private final DataType elementType;
    private final boolean nullable;
    private final Object defaultValue;

    private FieldType(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.name = builder.name;
        this.primaryKey = builder.primaryKey;
        this.description = builder.description;
        this.dataType = builder.dataType;
        this.typeParams = builder.typeParams;
        this.autoID = builder.autoID;
        this.partitionKey = builder.partitionKey;
        this.clusteringKey = builder.clusteringKey;
        this.isDynamic = builder.isDynamic;
        this.elementType = builder.elementType;
        this.nullable = builder.nullable;
        this.defaultValue = builder.defaultValue;
    }

    // Getter methods to replace @Getter annotation
    public String getName() {
        return name;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String getDescription() {
        return description;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    public boolean isAutoID() {
        return autoID;
    }

    public boolean isPartitionKey() {
        return partitionKey;
    }

    public boolean isClusteringKey() {
        return clusteringKey;
    }

    public boolean isDynamic() {
        return isDynamic;
    }

    public DataType getElementType() {
        return elementType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public int getDimension() {
        if (typeParams.containsKey(Constant.VECTOR_DIM)) {
            return Integer.parseInt(typeParams.get(Constant.VECTOR_DIM));
        }

        return 0;
    }

    public int getMaxLength() {
        if (typeParams.containsKey(Constant.VARCHAR_MAX_LENGTH)) {
            return Integer.parseInt(typeParams.get(Constant.VARCHAR_MAX_LENGTH));
        }

        return 0;
    }

    public int getMaxCapacity() {
        if (typeParams.containsKey(Constant.ARRAY_MAX_CAPACITY)) {
            return Integer.parseInt(typeParams.get(Constant.ARRAY_MAX_CAPACITY));
        }

        return 0;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "FieldType{" +
                "name='" + name + '\'' +
                ", primaryKey=" + primaryKey +
                ", description='" + description + '\'' +
                ", dataType=" + dataType +
                ", typeParams=" + typeParams +
                ", autoID=" + autoID +
                ", partitionKey=" + partitionKey +
                ", clusteringKey=" + clusteringKey +
                ", isDynamic=" + isDynamic +
                ", elementType=" + elementType +
                ", nullable=" + nullable +
                ", defaultValue=" + defaultValue +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link FieldType} class.
     */
    public static final class Builder {
        private String name;
        private boolean primaryKey = false;
        private String description = "";
        private DataType dataType;
        private final Map<String, String> typeParams = new HashMap<>();
        private boolean autoID = false;
        private boolean partitionKey = false;
        private boolean clusteringKey = false;
        private boolean isDynamic = false;
        private DataType elementType = DataType.None; // only for Array type field
        private boolean nullable = false; // only for scalar fields(not include Array fields)
        private Object defaultValue = null; // only for scalar fields
        private boolean enableDefaultValue = false; // a flag to pass the default value to server or not

        private Builder() {
        }

        public Builder withName(String name) {
            if (name == null) {
                throw new IllegalArgumentException("name cannot be null");
            }
            this.name = name;
            return this;
        }

        /**
         * Sets the isDynamic of a field.
         *
         * @param isDynamic of a field
         * @return <code>Builder</code>
         */
        public Builder withIsDynamic(boolean isDynamic) {
            this.isDynamic = isDynamic;
            return this;
        }

        /**
         * Sets the field as the primary key field.
         * Note that the current release of Milvus only support <code>Long</code> data type as primary key.
         *
         * @param primaryKey true is primary key, false is not
         * @return <code>Builder</code>
         */
        public Builder withPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        /**
         * Sets the field description. The description can be empty. The default is "".
         *
         * @param description description of the field
         * @return <code>Builder</code>
         */
        public Builder withDescription(String description) {
            if (description == null) {
                throw new IllegalArgumentException("description cannot be null");
            }
            this.description = description;
            return this;
        }

        /**
         * Sets the data type for the field.
         *
         * @param dataType data type of the field
         * @return <code>Builder</code>
         */
        public Builder withDataType(DataType dataType) {
            if (dataType == null) {
                throw new IllegalArgumentException("dataType cannot be null");
            }
            this.dataType = dataType;
            return this;
        }

        /**
         * Sets the element type for Array type field.
         *
         * @param elementType element type of the Array type field
         * @return <code>Builder</code>
         */
        public Builder withElementType(DataType elementType) {
            if (elementType == null) {
                throw new IllegalArgumentException("elementType cannot be null");
            }
            this.elementType = elementType;
            return this;
        }

        /**
         * Adds a parameter pair for the field.
         *
         * @param key   parameter key
         * @param value parameter value
         * @return <code>Builder</code>
         */
        public Builder addTypeParam(String key, String value) {
            if (key == null) {
                throw new IllegalArgumentException("key cannot be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            this.typeParams.put(key, value);
            return this;
        }

        /**
         * Sets more parameters for the field.
         *
         * @param typeParams parameters of the field
         * @return <code>Builder</code>
         */
        public Builder withTypeParams(Map<String, String> typeParams) {
            if (typeParams == null) {
                throw new IllegalArgumentException("typeParams cannot be null");
            }
            typeParams.forEach(this.typeParams::put);
            return this;
        }

        /**
         * Sets the dimension of a vector field. Dimension value must be greater than zero.
         *
         * @param dimension dimension of the field
         * @return <code>Builder</code>
         */
        public Builder withDimension(Integer dimension) {
            if (dimension == null) {
                throw new IllegalArgumentException("dimension cannot be null");
            }
            this.typeParams.put(Constant.VECTOR_DIM, dimension.toString());
            return this;
        }

        /**
         * Sets the max length of a varchar field. The value must be greater than zero.
         *
         * @param maxLength max length of a varchar field
         * @return <code>Builder</code>
         */
        public Builder withMaxLength(Integer maxLength) {
            if (maxLength == null) {
                throw new IllegalArgumentException("maxLength cannot be null");
            }
            this.typeParams.put(Constant.VARCHAR_MAX_LENGTH, maxLength.toString());
            return this;
        }

        /**
         * Sets the max capacity of an array field. The value must be greater than zero.
         * The valid capacity value range is [1, 4096]
         *
         * @param maxCapacity max capacity of an array field
         * @return <code>Builder</code>
         */
        public Builder withMaxCapacity(Integer maxCapacity) {
            if (maxCapacity == null) {
                throw new IllegalArgumentException("maxCapacity cannot be null");
            }
            if (maxCapacity <= 0 || maxCapacity > 4096) {
                throw new ParamException("Array field max capacity value must be within range [1, 4096]");
            }
            this.typeParams.put(Constant.ARRAY_MAX_CAPACITY, maxCapacity.toString());
            return this;
        }

        /**
         * Enables auto-id function for the field. Note that the auto-id function can only be enabled on primary key field.
         * If auto-id function is enabled, Milvus will automatically generate unique ID for each entity,
         * thus you do not need to provide values for the primary key field when inserting.
         * <p>
         * If auto-id is disabled, you need to provide values for the primary key field when inserting.
         *
         * @param autoID true enable auto-id, false disable auto-id
         * @return <code>Builder</code>
         */
        public Builder withAutoID(boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        /**
         * Sets the field to be partition key.
         * A partition key field's values are hashed and distributed to different logic partitions.
         * Only int64 and varchar type field can be partition key.
         * Primary key field cannot be partition key.
         *
         * @param partitionKey true is partition key, false is not
         * @return <code>Builder</code>
         */
        public Builder withPartitionKey(boolean partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        /**
         * Sets this field is nullable or not.
         * Primary key field, vector fields, Array fields cannot be nullable.
         * <p>
         * 1. if the field is nullable, user can input JsonNull/JsonObject(for row-based insert), or input null/object(for column-based insert)
         * 1) if user input JsonNull, this value is replaced by default value
         * 2) if user input JsonObject, infer this value by type
         * 2. if the field is not nullable, user can input JsonNull/JsonObject(for row-based insert), or input null/object(for column-based insert)
         * 1) if user input JsonNull, and default value is null, throw error
         * 2) if user input JsonNull, and default value is not null, this value is replaced by default value
         * 3) if user input JsonObject, infer this value by type
         *
         * @param nullable true is nullable, false is not
         * @return <code>Builder</code>
         */
        public Builder withNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        /**
         * Sets default value of this field.
         * If nullable is false, the default value cannot be null. If nullable is true, the default value can be null.
         * Only scalar fields(not include Array field) support default value.
         * The default value type must obey the following rule:
         * - Boolean for Bool fields
         * - Short for Int8/Int16 fields
         * - Short/Integer for Int32 fields
         * - Short/Integer/Long for Int64 fields
         * - Float for Float fields
         * - Double for Double fields
         * - String for Varchar fields
         * - JsonObject for JSON fields
         * <p>
         * For JSON field, you can use JsonNull.INSTANCE as default value. For other scalar fields, you can use null as default value.
         *
         * @param obj the default value
         * @return <code>Builder</code>
         */
        public Builder withDefaultValue(Object obj) {
            this.defaultValue = obj;
            this.enableDefaultValue = true;
            return this;
        }

        /**
         * Set the field to be a clustering key.
         * A clustering key can notify milvus to trigger a clustering compaction to redistribute entities among segments
         * in a collection based on the values in the clustering key field. And one global index called PartitionStats
         * is generated to store the mapping relationship between segments and clustering key values. Once receiving
         * a search/query request that carries a clustering key value, it quickly finds out a search scope from
         * the PartitionStats which significantly improving search performance.
         * Only scalar fields(except Array/JSON) can be clustering key.
         * Only one clustering key is allowed in one collection.
         *
         * @param clusteringKey true is clustering key, false is not
         * @return <code>Builder</code>
         */
        public Builder withClusteringKey(boolean clusteringKey) {
            this.clusteringKey = clusteringKey;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link FieldType} instance.
         *
         * @return {@link FieldType}
         */
        public FieldType build() throws ParamException {
            ParamUtils.CheckNullEmptyString(name, "Field name");

            if (dataType == null || dataType == DataType.None || dataType == DataType.UNRECOGNIZED) {
                throw new ParamException("Field data type is illegal");
            }

            if (dataType == DataType.String) {
                throw new ParamException("String type is not supported, use Varchar instead");
            }

            // SparseVector has no dimension, other vector types must have dimension
            if (ParamUtils.isVectorDataType(dataType) && dataType != DataType.SparseFloatVector) {
                if (!typeParams.containsKey(Constant.VECTOR_DIM)) {
                    throw new ParamException("Vector field dimension must be specified");
                }

                try {
                    int dim = Integer.parseInt(typeParams.get(Constant.VECTOR_DIM));
                    if (dim <= 0) {
                        throw new ParamException("Vector field dimension must be larger than zero");
                    }
                } catch (NumberFormatException e) {
                    throw new ParamException("Vector field dimension must be an integer number");
                }
            }

            if (dataType == DataType.VarChar) {
                if (!typeParams.containsKey(Constant.VARCHAR_MAX_LENGTH)) {
                    throw new ParamException("Varchar field max length must be specified");
                }

                try {
                    int len = Integer.parseInt(typeParams.get(Constant.VARCHAR_MAX_LENGTH));
                    if (len <= 0) {
                        throw new ParamException("Varchar field max length must be larger than zero");
                    }
                } catch (NumberFormatException e) {
                    throw new ParamException("Varchar field max length must be an integer number");
                }
            }

            // verify partition key
            if (partitionKey) {
                if (primaryKey) {
                    throw new ParamException("Primary key field can not be partition key");
                }
                if (dataType != DataType.Int64 && dataType != DataType.VarChar) {
                    throw new ParamException("Only Int64 and Varchar type field can be partition key");
                }
            }

            // verify element type for Array field
            if (dataType == DataType.Array) {
                if (elementType == DataType.String) {
                    throw new ParamException("String type is not supported, use Varchar instead");
                }
                if (elementType == DataType.None || elementType == DataType.Array
                        || elementType == DataType.JSON || ParamUtils.isVectorDataType(elementType)
                        || elementType == DataType.UNRECOGNIZED) {
                    throw new ParamException("Unsupported element type");
                }

                if (!this.typeParams.containsKey(Constant.ARRAY_MAX_CAPACITY)) {
                    throw new ParamException("Array field max capacity must be specified");
                }
                if (elementType == DataType.VarChar && !this.typeParams.containsKey(Constant.VARCHAR_MAX_LENGTH)) {
                    throw new ParamException("Varchar array field max length must be specified");
                }
            }

            // check the input here to pop error messages earlier
            if (enableDefaultValue && defaultValue == null && !nullable) {
                String msg = String.format("Default value cannot be null for field '%s' that is defined as nullable == false.", name);
                throw new ParamException(msg);
            }

            return new FieldType(this);
        }
    }

}

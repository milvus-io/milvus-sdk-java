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

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for a collection field.
 * @see CreateCollectionParam
 */
@Getter
@ToString
public class FieldType {
    private final String name;
    private final boolean primaryKey;
    private final String description;
    private final DataType dataType;
    private final Map<String,String> typeParams;
    private final boolean autoID;
    private final boolean partitionKey;
    private final boolean isDynamic;
    private final DataType elementType;

    private FieldType(@NonNull Builder builder){
        this.name = builder.name;
        this.primaryKey = builder.primaryKey;
        this.description = builder.description;
        this.dataType = builder.dataType;
        this.typeParams = builder.typeParams;
        this.autoID = builder.autoID;
        this.partitionKey = builder.partitionKey;
        this.isDynamic = builder.isDynamic;
        this.elementType = builder.elementType;
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
        private final Map<String,String> typeParams = new HashMap<>();
        private boolean autoID = false;
        private boolean partitionKey = false;
        private boolean isDynamic = false;
        private DataType elementType = DataType.None; // only for Array type field

        private Builder() {
        }

        public Builder withName(@NonNull String name) {
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
        public Builder withDescription(@NonNull String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the data type for the field.
         *
         * @param dataType data type of the field
         * @return <code>Builder</code>
         */
        public Builder withDataType(@NonNull DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        /**
         * Sets the element type for Array type field.
         *
         * @param elementType element type of the Array type field
         * @return <code>Builder</code>
         */
        public Builder withElementType(@NonNull DataType elementType) {
            this.elementType = elementType;
            return this;
        }

        /**
         * Adds a parameter pair for the field.
         *
         * @param key parameter key
         * @param value parameter value
         * @return <code>Builder</code>
         */
        public Builder addTypeParam(@NonNull String key, @NonNull String value) {
            this.typeParams.put(key, value);
            return this;
        }

        /**
         * Sets more parameters for the field.
         *
         * @param typeParams parameters of the field
         * @return <code>Builder</code>
         */
        public Builder withTypeParams(@NonNull Map<String, String> typeParams) {
            typeParams.forEach(this.typeParams::put);
            return this;
        }

        /**
         * Sets the dimension of a vector field. Dimension value must be greater than zero.
         *
         * @param dimension dimension of the field
         * @return <code>Builder</code>
         */
        public Builder withDimension(@NonNull Integer dimension) {
            this.typeParams.put(Constant.VECTOR_DIM, dimension.toString());
            return this;
        }

        /**
         * Sets the max length of a varchar field. The value must be greater than zero.
         *
         * @param maxLength max length of a varchar field
         * @return <code>Builder</code>
         */
        public Builder withMaxLength(@NonNull Integer maxLength) {
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
        public Builder withMaxCapacity(@NonNull Integer maxCapacity) {
            if (maxCapacity <= 0 || maxCapacity >= 4096) {
                throw new ParamException("Array field max capacity value must be within range [1, 4096]");
            }
            this.typeParams.put(Constant.ARRAY_MAX_CAPACITY, maxCapacity.toString());
            return this;
        }

        /**
         * Enables auto-id function for the field. Note that the auto-id function can only be enabled on primary key field.
         * If auto-id function is enabled, Milvus will automatically generate unique ID for each entity,
         * thus you do not need to provide values for the primary key field when inserting.
         *
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

            return new FieldType(this);
        }
    }

}

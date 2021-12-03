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
import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for a collection field.
 * @see CreateCollectionParam
 */
@Getter
public class FieldType {
    private final String name;
    private final boolean primaryKey;
    private final String description;
    private final DataType dataType;
    private final Map<String,String> typeParams;
    private final boolean autoID;

    private FieldType(@NonNull Builder builder){
        this.name = builder.name;
        this.primaryKey = builder.primaryKey;
        this.description = builder.description;
        this.dataType = builder.dataType;
        this.typeParams = builder.typeParams;
        this.autoID = builder.autoID;
    }

    public int getDimension() {
        if (typeParams.containsKey(Constant.VECTOR_DIM)) {
            return Integer.valueOf(typeParams.get(Constant.VECTOR_DIM));
        }

        return 0;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>FieldType</code> class.
     */
    public static final class Builder {
        private String name;
        private boolean primaryKey = false;
        private String description = "";
        private DataType dataType;
        private final Map<String,String> typeParams = new HashMap<>();
        private boolean autoID = false;

        private Builder() {
        }

        public Builder withName(@NonNull String name) {
            this.name = name;
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
         * Verifies parameters and creates a new <code>FieldType</code> instance.
         *
         * @return <code>FieldType</code>
         */
        public FieldType build() throws ParamException {
            ParamUtils.CheckNullEmptyString(name, "Field name");

            if (dataType == null || dataType == DataType.None) {
                throw new ParamException("Field data type is illegal");
            }

            if (dataType == DataType.FloatVector || dataType == DataType.BinaryVector) {
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

            return new FieldType(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>FieldType</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "FieldType{" +
                "name='" + name + '\'' +
                ", type='" + dataType.name() + '\'' +
                ", primaryKey=" + primaryKey +
                ", autoID=" + autoID +
                ", params=" + typeParams.toString() +
                '}';
    }
}

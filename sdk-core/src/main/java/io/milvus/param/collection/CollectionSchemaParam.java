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

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>createCollection</code> interface.
 */
public class CollectionSchemaParam {
    private final List<FieldType> fieldTypes;
    private final boolean enableDynamicField;

    private CollectionSchemaParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.fieldTypes = builder.fieldTypes;
        this.enableDynamicField = builder.enableDynamicField;
    }

    public List<FieldType> getFieldTypes() {
        return fieldTypes;
    }

    public boolean isEnableDynamicField() {
        return enableDynamicField;
    }

    @Override
    public String toString() {
        return "CollectionSchemaParam{" +
                "fieldTypes=" + fieldTypes +
                ", enableDynamicField=" + enableDynamicField +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CollectionSchemaParam} class.
     */
    public static final class Builder {
        private final List<FieldType> fieldTypes = new ArrayList<>();
        private boolean enableDynamicField;

        private Builder() {
        }

        /**
         * Sets the collection if enableDynamicField.
         *
         * @param enableDynamicField enableDynamicField of the collection
         * @return <code>Builder</code>
         */
        public Builder withEnableDynamicField(boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
            return this;
        }

        /**
         * Sets the fieldTypes of the schema. The fieldTypes cannot be empty or null.
         * @see FieldType
         *
         * @param fieldTypes a <code>List</code> of {@link FieldType}
         * @return <code>Builder</code>
         */
        public Builder withFieldTypes(List<FieldType> fieldTypes) {
            if (fieldTypes == null) {
                throw new IllegalArgumentException("fieldTypes cannot be null");
            }
            this.fieldTypes.addAll(fieldTypes);
            return this;
        }

        /**
         * Adds a field.
         * @see FieldType
         *
         * @param fieldType a {@link FieldType} object
         * @return <code>Builder</code>
         */
        public Builder addFieldType(FieldType fieldType) {
            if (fieldType == null) {
                throw new IllegalArgumentException("fieldType cannot be null");
            }
            this.fieldTypes.add(fieldType);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CollectionSchemaParam} instance.
         *
         * @return {@link CollectionSchemaParam}
         */
        public CollectionSchemaParam build() throws ParamException {
            if (fieldTypes.isEmpty()) {
                throw new ParamException("Field numbers must be larger than 0");
            }

            boolean hasPartitionKey = false;
            for (FieldType fieldType : fieldTypes) {
                if (fieldType == null) {
                    throw new ParamException("Collection field cannot be null");
                }

                if (fieldType.isPartitionKey()) {
                    if (hasPartitionKey) {
                        throw new ParamException("Only one partition key field is allowed in a collection");
                    }
                    hasPartitionKey = true;
                }
            }

            return new CollectionSchemaParam(this);
        }
    }
}

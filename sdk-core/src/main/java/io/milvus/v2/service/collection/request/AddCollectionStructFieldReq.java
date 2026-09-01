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

package io.milvus.v2.service.collection.request;

import io.milvus.exception.ParamException;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for the {@code addCollectionStructField} API.
 */
public class AddCollectionStructFieldReq {
    private String collectionName;
    private String databaseName;
    private String fieldName;
    private String description;
    private Integer maxCapacity;
    private Boolean nullable;
    private List<FieldSchema> structFields;
    private Map<String, String> typeParams;

    private AddCollectionStructFieldReq(AddCollectionStructFieldReqBuilder builder) {
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.fieldName = builder.fieldName;
        this.description = builder.description;
        this.maxCapacity = builder.maxCapacity;
        this.nullable = builder.nullable;
        this.structFields = builder.structFields;
        this.typeParams = builder.typeParams;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the name of the struct field to add.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the name of the struct field to add.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Returns the description of the struct field.
     *
     * @return the field description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the struct field.
     *
     * @param description the field description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the maximum number of elements the struct field can hold.
     *
     * @return the max capacity
     */
    public Integer getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * Sets the maximum number of elements the struct field can hold.
     *
     * @param maxCapacity the max capacity
     */
    public void setMaxCapacity(Integer maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    /**
     * Returns whether the struct field is nullable.
     *
     * @return {@code true} if the field is nullable
     */
    public Boolean getNullable() {
        return nullable;
    }

    /**
     * Sets whether the struct field is nullable.
     *
     * @param nullable {@code true} to make the field nullable
     */
    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * Returns the sub-fields of the struct field.
     *
     * @return the struct sub-fields
     */
    public List<FieldSchema> getStructFields() {
        return structFields;
    }

    /**
     * Sets the sub-fields of the struct field.
     *
     * @param structFields the struct sub-fields
     */
    public void setStructFields(List<FieldSchema> structFields) {
        this.structFields = structFields;
    }

    /**
     * Returns the type parameters of the struct field.
     *
     * @return the type parameters
     */
    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    /**
     * Sets the type parameters of the struct field.
     *
     * @param typeParams the type parameters
     */
    public void setTypeParams(Map<String, String> typeParams) {
        this.typeParams = typeParams;
    }

    /**
     * Converts this request into a {@link CreateCollectionReq.StructFieldSchema}.
     *
     * @return the struct field schema
     * @throws ParamException if the field is not nullable or the schema conversion fails
     */
    public CreateCollectionReq.StructFieldSchema toStructFieldSchema() {
        if (Boolean.FALSE.equals(nullable)) {
            throw new ParamException("Adding struct field to existing collection requires nullable=true");
        }

        AddFieldReq addFieldReq = AddFieldReq.builder()
                .fieldName(fieldName)
                .description(description)
                .maxCapacity(maxCapacity)
                .structFields(structFields)
                .build();
        CreateCollectionReq.StructFieldSchema structFieldSchema = SchemaUtils.convertFieldReqToStructFieldSchema(addFieldReq);
        structFieldSchema.setNullable(Boolean.TRUE);
        structFieldSchema.setTypeParams(typeParams);
        return structFieldSchema;
    }

    @Override
    public String toString() {
        return "AddCollectionStructFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", description='" + description + '\'' +
                ", maxCapacity=" + maxCapacity +
                ", nullable=" + nullable +
                ", structFields=" + structFields +
                ", typeParams=" + typeParams +
                '}';
    }

    /**
     * Creates a new builder for {@link AddCollectionStructFieldReq}.
     *
     * @return the builder
     */
    public static AddCollectionStructFieldReqBuilder builder() {
        return new AddCollectionStructFieldReqBuilder();
    }

    public static class AddCollectionStructFieldReqBuilder {
        private String collectionName = "";
        private String databaseName = "";
        private String fieldName = "";
        private String description = "";
        private Integer maxCapacity;
        private Boolean nullable = Boolean.TRUE;
        private List<FieldSchema> structFields = new ArrayList<>();
        private Map<String, String> typeParams = new HashMap<>();

        private AddCollectionStructFieldReqBuilder() {
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the name of the struct field to add.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the description of the struct field.
         *
         * @param description the field description
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the maximum number of elements the struct field can hold.
         *
         * @param maxCapacity the max capacity
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder maxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        /**
         * Sets whether the struct field is nullable.
         *
         * @param nullable {@code true} to make the field nullable
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder nullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        /**
         * Sets the sub-fields of the struct field.
         *
         * @param structFields the struct sub-fields
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder structFields(List<FieldSchema> structFields) {
            this.structFields = structFields;
            return this;
        }

        /**
         * Converts the given field request into a {@link FieldSchema} and appends it
         * to the struct sub-fields.
         *
         * @param addFieldReq the field request
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder addStructField(AddFieldReq addFieldReq) {
            if (this.structFields == null) {
                this.structFields = new ArrayList<>();
            }
            this.structFields.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        /**
         * Sets the type parameters of the struct field.
         *
         * @param typeParams the type parameters
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder typeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
            return this;
        }

        /**
         * Adds a single type parameter to the struct field.
         *
         * @param key the parameter key
         * @param value the parameter value
         * @return this builder
         */
        public AddCollectionStructFieldReqBuilder typeParam(String key, String value) {
            if (this.typeParams == null) {
                this.typeParams = new HashMap<>();
            }
            this.typeParams.put(key, value);
            return this;
        }

        /**
         * Builds an {@link AddCollectionStructFieldReq} with the configured parameters.
         *
         * @return the request
         */
        public AddCollectionStructFieldReq build() {
            return new AddCollectionStructFieldReq(this);
        }
    }
}

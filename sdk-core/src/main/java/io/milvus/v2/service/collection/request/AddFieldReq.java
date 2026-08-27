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

import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Request parameters for adding a field to a collection schema, used by the
 * {@code addField} schema builder and by the {@code addCollectionField} API.
 */
public class AddFieldReq {
    private String fieldName;
    private String description;
    private DataType dataType;
    private Integer maxLength;
    private Boolean isPrimaryKey;
    private Boolean isPartitionKey;
    private Boolean isClusteringKey;
    private Boolean autoID;
    private Integer dimension;
    private DataType elementType;
    private Integer maxCapacity;
    private Boolean isNullable; // only for scalar fields(not include Array fields)
    private Object defaultValue; // only for scalar fields
    private boolean enableDefaultValue; // a flag to pass the default value to server or not
    private Boolean enableAnalyzer; // for BM25 tokenizer
    private Map<String, Object> analyzerParams; // for BM25 tokenizer
    private Boolean enableMatch; // for BM25 keyword search

    // If a specific field, such as maxLength, has been specified, it will override the corresponding key's value in typeParams.
    private Map<String, String> typeParams;
    private Map<String, Object> multiAnalyzerParams; // for multi‑language analyzers

    private String externalField; // external field name mapping

    private List<FieldSchema> structFields;

    AddFieldReq(AddFieldReqBuilder<?> builder) {
        this.fieldName = builder.fieldName;
        this.description = builder.description != null ? builder.description : "";
        this.dataType = builder.dataType;
        this.maxLength = builder.maxLength != null ? builder.maxLength : 65535;
        this.isPrimaryKey = builder.isPrimaryKey != null ? builder.isPrimaryKey : Boolean.FALSE;
        this.isPartitionKey = builder.isPartitionKey != null ? builder.isPartitionKey : Boolean.FALSE;
        this.isClusteringKey = builder.isClusteringKey != null ? builder.isClusteringKey : Boolean.FALSE;
        this.autoID = builder.autoID != null ? builder.autoID : Boolean.FALSE;
        this.dimension = builder.dimension;
        this.elementType = builder.elementType;
        this.maxCapacity = builder.maxCapacity;
        this.isNullable = builder.isNullable != null ? builder.isNullable : Boolean.FALSE;
        this.defaultValue = builder.defaultValue;
        this.enableDefaultValue = builder.enableDefaultValue;
        this.enableAnalyzer = builder.enableAnalyzer;
        this.analyzerParams = builder.analyzerParams;
        this.enableMatch = builder.enableMatch;
        this.typeParams = builder.typeParams;
        this.multiAnalyzerParams = builder.multiAnalyzerParams;
        this.externalField = builder.externalField != null ? builder.externalField : "";
        this.structFields = builder.structFields != null ? builder.structFields : new ArrayList<>();
    }

    /**
     * Creates a new builder for {@link AddFieldReq}.
     *
     * @return the builder
     */
    public static AddFieldReqBuilder<?> builder() {
        return new AddFieldReqBuilder<>();
    }

    // Getters
    /**
     * Returns the field name.
     *
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the field description.
     *
     * @return the field description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the data type of the field.
     *
     * @return the data type
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * Returns the maximum length allowed for the field.
     *
     * @return the max length
     */
    public Integer getMaxLength() {
        return maxLength;
    }

    /**
     * Returns whether the field is the primary key.
     *
     * @return {@code true} if the field is the primary key
     */
    public Boolean getIsPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     * Returns whether the field is a partition key.
     *
     * @return {@code true} if the field is a partition key
     */
    public Boolean getIsPartitionKey() {
        return isPartitionKey;
    }

    /**
     * Returns whether the field is a clustering key.
     *
     * @return {@code true} if the field is a clustering key
     */
    public Boolean getIsClusteringKey() {
        return isClusteringKey;
    }

    /**
     * Returns whether the field has auto-generated IDs.
     *
     * @return {@code true} if auto ID is enabled
     */
    public Boolean getAutoID() {
        return autoID;
    }

    /**
     * Returns the dimension of the vector field.
     *
     * @return the vector dimension
     */
    public Integer getDimension() {
        return dimension;
    }

    /**
     * Returns the element type of an Array field.
     *
     * @return the element type
     */
    public DataType getElementType() {
        return elementType;
    }

    /**
     * Returns the maximum number of elements an Array field can hold.
     *
     * @return the max capacity
     */
    public Integer getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * Returns whether the scalar field is nullable.
     *
     * @return {@code true} if the field is nullable
     */
    public Boolean getIsNullable() {
        return isNullable;
    }

    /**
     * Returns the default value of the scalar field.
     *
     * @return the default value
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * Returns whether the default value should be passed to the server.
     *
     * @return {@code true} if the default value is enabled
     */
    public boolean isEnableDefaultValue() {
        return enableDefaultValue;
    }

    /**
     * Returns whether the BM25 analyzer is enabled for this field.
     *
     * @return {@code true} if the analyzer is enabled
     */
    public Boolean getEnableAnalyzer() {
        return enableAnalyzer;
    }

    /**
     * Returns the analyzer parameters for the BM25 tokenizer.
     *
     * @return the analyzer parameters
     */
    public Map<String, Object> getAnalyzerParams() {
        return analyzerParams;
    }

    /**
     * Returns whether BM25 keyword matching is enabled for this field.
     *
     * @return {@code true} if BM25 matching is enabled
     */
    public Boolean getEnableMatch() {
        return enableMatch;
    }

    /**
     * Returns the type parameters of the field.
     *
     * @return the type parameters
     */
    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    /**
     * Returns the parameters of the multi-language analyzers.
     *
     * @return the multi-analyzer parameters
     */
    public Map<String, Object> getMultiAnalyzerParams() {
        return multiAnalyzerParams;
    }

    /**
     * Returns the external field name this field maps to.
     *
     * @return the external field name
     */
    public String getExternalField() {
        return externalField;
    }

    /**
     * Returns the sub-fields of a struct field.
     *
     * @return the struct sub-fields
     */
    public List<FieldSchema> getStructFields() {
        return structFields;
    }

    // Setters
    /**
     * Sets the field name.
     *
     * @param fieldName the field name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Sets the field description.
     *
     * @param description the field description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Sets the data type of the field.
     *
     * @param dataType the data type
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * Sets the maximum length allowed for the field.
     *
     * @param maxLength the max length
     */
    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * Sets whether the field is the primary key.
     *
     * @param isPrimaryKey {@code true} to make the field the primary key
     */
    public void setIsPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    /**
     * Sets whether the field is a partition key.
     *
     * @param isPartitionKey {@code true} to make the field a partition key
     */
    public void setIsPartitionKey(Boolean isPartitionKey) {
        this.isPartitionKey = isPartitionKey;
    }

    /**
     * Sets whether the field is a clustering key.
     *
     * @param isClusteringKey {@code true} to make the field a clustering key
     */
    public void setIsClusteringKey(Boolean isClusteringKey) {
        this.isClusteringKey = isClusteringKey;
    }

    /**
     * Sets whether the field has auto-generated IDs.
     *
     * @param autoID {@code true} to enable auto ID
     */
    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    /**
     * Sets the dimension of the vector field.
     *
     * @param dimension the vector dimension
     */
    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    /**
     * Sets the element type of an Array field.
     *
     * @param elementType the element type
     */
    public void setElementType(DataType elementType) {
        this.elementType = elementType;
    }

    /**
     * Sets the maximum number of elements an Array field can hold.
     *
     * @param maxCapacity the max capacity
     */
    public void setMaxCapacity(Integer maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    /**
     * Sets whether the scalar field is nullable.
     *
     * @param isNullable {@code true} to make the field nullable
     */
    public void setIsNullable(Boolean isNullable) {
        this.isNullable = isNullable;
    }

    /**
     * Sets the default value of the scalar field.
     *
     * @param defaultValue the default value
     */
    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Sets whether the default value should be passed to the server.
     *
     * @param enableDefaultValue {@code true} to enable the default value
     */
    public void setEnableDefaultValue(boolean enableDefaultValue) {
        this.enableDefaultValue = enableDefaultValue;
    }

    /**
     * Sets whether the BM25 analyzer is enabled for this field.
     *
     * @param enableAnalyzer {@code true} to enable the analyzer
     */
    public void setEnableAnalyzer(Boolean enableAnalyzer) {
        this.enableAnalyzer = enableAnalyzer;
    }

    /**
     * Sets the analyzer parameters for the BM25 tokenizer.
     *
     * @param analyzerParams the analyzer parameters
     */
    public void setAnalyzerParams(Map<String, Object> analyzerParams) {
        this.analyzerParams = analyzerParams;
    }

    /**
     * Sets whether BM25 keyword matching is enabled for this field.
     *
     * @param enableMatch {@code true} to enable BM25 matching
     */
    public void setEnableMatch(Boolean enableMatch) {
        this.enableMatch = enableMatch;
    }

    /**
     * Sets the type parameters of the field.
     *
     * @param typeParams the type parameters
     */
    public void setTypeParams(Map<String, String> typeParams) {
        this.typeParams = typeParams;
    }

    /**
     * Sets the parameters of the multi-language analyzers.
     *
     * @param multiAnalyzerParams the multi-analyzer parameters
     */
    public void setMultiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
        this.multiAnalyzerParams = multiAnalyzerParams;
    }

    /**
     * Sets the external field name this field maps to.
     *
     * @param externalField the external field name
     */
    public void setExternalField(String externalField) {
        this.externalField = externalField;
    }

    /**
     * Sets the sub-fields of a struct field.
     *
     * @param structFields the struct sub-fields
     */
    public void setStructFields(List<FieldSchema> structFields) {
        this.structFields = structFields;
    }

    @Override
    public String toString() {
        return "AddFieldReq{" +
                "fieldName='" + fieldName + '\'' +
                ", description='" + description + '\'' +
                ", dataType=" + dataType +
                ", maxLength=" + maxLength +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isPartitionKey=" + isPartitionKey +
                ", isClusteringKey=" + isClusteringKey +
                ", autoID=" + autoID +
                ", dimension=" + dimension +
                ", elementType=" + elementType +
                ", maxCapacity=" + maxCapacity +
                ", isNullable=" + isNullable +
                ", defaultValue=" + defaultValue +
                ", enableDefaultValue=" + enableDefaultValue +
                ", enableAnalyzer=" + enableAnalyzer +
                ", analyzerParams=" + analyzerParams +
                ", enableMatch=" + enableMatch +
                ", typeParams=" + typeParams +
                ", multiAnalyzerParams=" + multiAnalyzerParams +
                ", externalField='" + externalField + '\'' +
                ", structFields=" + structFields +
                '}';
    }

    public static class AddFieldReqBuilder<T extends AddFieldReqBuilder<T>> {
        private String fieldName;
        private String description;
        private DataType dataType;
        private Integer maxLength;
        private Boolean isPrimaryKey;
        private Boolean isPartitionKey;
        private Boolean isClusteringKey;
        private Boolean autoID;
        private Integer dimension;
        private DataType elementType;
        private Integer maxCapacity;
        private Boolean isNullable;
        private Object defaultValue;
        private boolean enableDefaultValue = false;
        private Boolean enableAnalyzer;
        private Map<String, Object> analyzerParams;
        private Boolean enableMatch;
        private Map<String, String> typeParams;
        private Map<String, Object> multiAnalyzerParams;
        private String externalField;
        private List<FieldSchema> structFields;

        /**
         * Sets the field name.
         *
         * @param fieldName the field name
         * @return this builder
         */
        public T fieldName(String fieldName) {
            this.fieldName = fieldName;
            return (T) this;
        }

        /**
         * Sets the field description.
         *
         * @param description the field description
         * @return this builder
         */
        public T description(String description) {
            this.description = description;
            return (T) this;
        }

        /**
         * Sets the data type of the field.
         *
         * @param dataType the data type
         * @return this builder
         */
        public T dataType(DataType dataType) {
            this.dataType = dataType;
            return (T) this;
        }

        /**
         * Sets the maximum length allowed for the field.
         *
         * @param maxLength the max length
         * @return this builder
         */
        public T maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return (T) this;
        }

        /**
         * Sets whether the field is the primary key.
         *
         * @param isPrimaryKey {@code true} to make the field the primary key
         * @return this builder
         */
        public T isPrimaryKey(Boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
            return (T) this;
        }

        /**
         * Sets whether the field is a partition key.
         *
         * @param isPartitionKey {@code true} to make the field a partition key
         * @return this builder
         */
        public T isPartitionKey(Boolean isPartitionKey) {
            this.isPartitionKey = isPartitionKey;
            return (T) this;
        }

        /**
         * Sets whether the field is a clustering key.
         *
         * @param isClusteringKey {@code true} to make the field a clustering key
         * @return this builder
         */
        public T isClusteringKey(Boolean isClusteringKey) {
            this.isClusteringKey = isClusteringKey;
            return (T) this;
        }

        /**
         * Sets whether the field has auto-generated IDs.
         *
         * @param autoID {@code true} to enable auto ID
         * @return this builder
         */
        public T autoID(Boolean autoID) {
            this.autoID = autoID;
            return (T) this;
        }

        /**
         * Sets the dimension of the vector field.
         *
         * @param dimension the vector dimension
         * @return this builder
         */
        public T dimension(Integer dimension) {
            this.dimension = dimension;
            return (T) this;
        }

        /**
         * Sets the element type of an Array field.
         *
         * @param elementType the element type
         * @return this builder
         */
        public T elementType(DataType elementType) {
            this.elementType = elementType;
            return (T) this;
        }

        /**
         * Sets the maximum number of elements an Array field can hold.
         *
         * @param maxCapacity the max capacity
         * @return this builder
         */
        public T maxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
            return (T) this;
        }

        /**
         * Sets whether the scalar field is nullable.
         *
         * @param isNullable {@code true} to make the field nullable
         * @return this builder
         */
        public T isNullable(Boolean isNullable) {
            this.isNullable = isNullable;
            return (T) this;
        }

        /**
         * Sets the default value of the scalar field and enables passing it to the server.
         *
         * @param defaultValue the default value
         * @return this builder
         */
        public T defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            this.enableDefaultValue = true;
            return (T) this;
        }

        /**
         * Sets whether the default value should be passed to the server.
         *
         * @param enableDefaultValue {@code true} to enable the default value
         * @return this builder
         */
        public T enableDefaultValue(boolean enableDefaultValue) {
            this.enableDefaultValue = enableDefaultValue;
            return (T) this;
        }

        /**
         * Sets whether the BM25 analyzer is enabled for this field.
         *
         * @param enableAnalyzer {@code true} to enable the analyzer
         * @return this builder
         */
        public T enableAnalyzer(Boolean enableAnalyzer) {
            this.enableAnalyzer = enableAnalyzer;
            return (T) this;
        }

        /**
         * Sets the analyzer parameters for the BM25 tokenizer.
         *
         * @param analyzerParams the analyzer parameters
         * @return this builder
         */
        public T analyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
            return (T) this;
        }

        /**
         * Sets whether BM25 keyword matching is enabled for this field.
         *
         * @param enableMatch {@code true} to enable BM25 matching
         * @return this builder
         */
        public T enableMatch(Boolean enableMatch) {
            this.enableMatch = enableMatch;
            return (T) this;
        }

        /**
         * Sets the type parameters of the field.
         *
         * @param typeParams the type parameters
         * @return this builder
         */
        public T typeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
            return (T) this;
        }

        /**
         * Sets the parameters of the multi-language analyzers.
         *
         * @param multiAnalyzerParams the multi-analyzer parameters
         * @return this builder
         */
        public T multiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
            this.multiAnalyzerParams = multiAnalyzerParams;
            return (T) this;
        }

        /**
         * Sets the external field name this field maps to.
         *
         * @param externalField the external field name
         * @return this builder
         */
        public T externalField(String externalField) {
            this.externalField = externalField;
            return (T) this;
        }

        /**
         * Sets the sub-fields of a struct field.
         *
         * @param structFields the struct sub-fields
         * @return this builder
         */
        public T structFields(List<CreateCollectionReq.FieldSchema> structFields) {
            this.structFields = structFields;
            return (T) this;
        }

        /**
         * Converts the given field request into a {@link FieldSchema} and appends it
         * to the struct sub-fields.
         *
         * @param addFieldReq the field request
         * @return this builder
         */
        public T addStructField(AddFieldReq addFieldReq) {
            if (this.structFields == null) {
                this.structFields = new ArrayList<>();
            }
            CreateCollectionReq.FieldSchema field = SchemaUtils.convertFieldReqToFieldSchema(addFieldReq);
            this.structFields.add(field);
            return (T) this;
        }

        /**
         * Builds an {@link AddFieldReq} with the configured parameters.
         *
         * @return the request
         */
        public AddFieldReq build() {
            return new AddFieldReq(this);
        }
    }
}

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
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.Map;
import java.util.Objects;

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

    AddFieldReq(Builder builder) {
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
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getFieldName() {
        return fieldName;
    }

    public String getDescription() {
        return description;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public Boolean getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public Boolean getIsPartitionKey() {
        return isPartitionKey;
    }

    public Boolean getIsClusteringKey() {
        return isClusteringKey;
    }

    public Boolean getAutoID() {
        return autoID;
    }

    public Integer getDimension() {
        return dimension;
    }

    public DataType getElementType() {
        return elementType;
    }

    public Integer getMaxCapacity() {
        return maxCapacity;
    }

    public Boolean getIsNullable() {
        return isNullable;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean isEnableDefaultValue() {
        return enableDefaultValue;
    }

    public Boolean getEnableAnalyzer() {
        return enableAnalyzer;
    }

    public Map<String, Object> getAnalyzerParams() {
        return analyzerParams;
    }

    public Boolean getEnableMatch() {
        return enableMatch;
    }

    public Map<String, String> getTypeParams() {
        return typeParams;
    }

    public Map<String, Object> getMultiAnalyzerParams() {
        return multiAnalyzerParams;
    }

    // Setters
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public void setIsPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public void setIsPartitionKey(Boolean isPartitionKey) {
        this.isPartitionKey = isPartitionKey;
    }

    public void setIsClusteringKey(Boolean isClusteringKey) {
        this.isClusteringKey = isClusteringKey;
    }

    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    public void setElementType(DataType elementType) {
        this.elementType = elementType;
    }

    public void setMaxCapacity(Integer maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public void setIsNullable(Boolean isNullable) {
        this.isNullable = isNullable;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void setEnableDefaultValue(boolean enableDefaultValue) {
        this.enableDefaultValue = enableDefaultValue;
    }

    public void setEnableAnalyzer(Boolean enableAnalyzer) {
        this.enableAnalyzer = enableAnalyzer;
    }

    public void setAnalyzerParams(Map<String, Object> analyzerParams) {
        this.analyzerParams = analyzerParams;
    }

    public void setEnableMatch(Boolean enableMatch) {
        this.enableMatch = enableMatch;
    }

    public void setTypeParams(Map<String, String> typeParams) {
        this.typeParams = typeParams;
    }

    public void setMultiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
        this.multiAnalyzerParams = multiAnalyzerParams;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        AddFieldReq that = (AddFieldReq) obj;
        
        return new EqualsBuilder()
                .append(enableDefaultValue, that.enableDefaultValue)
                .append(fieldName, that.fieldName)
                .append(description, that.description)
                .append(dataType, that.dataType)
                .append(maxLength, that.maxLength)
                .append(isPrimaryKey, that.isPrimaryKey)
                .append(isPartitionKey, that.isPartitionKey)
                .append(isClusteringKey, that.isClusteringKey)
                .append(autoID, that.autoID)
                .append(dimension, that.dimension)
                .append(elementType, that.elementType)
                .append(maxCapacity, that.maxCapacity)
                .append(isNullable, that.isNullable)
                .append(defaultValue, that.defaultValue)
                .append(enableAnalyzer, that.enableAnalyzer)
                .append(analyzerParams, that.analyzerParams)
                .append(enableMatch, that.enableMatch)
                .append(typeParams, that.typeParams)
                .append(multiAnalyzerParams, that.multiAnalyzerParams)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, description, dataType, maxLength, isPrimaryKey, 
                isPartitionKey, isClusteringKey, autoID, dimension, elementType, 
                maxCapacity, isNullable, defaultValue, enableDefaultValue, 
                enableAnalyzer, analyzerParams, enableMatch, typeParams, multiAnalyzerParams);
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
                '}';
    }

    public static class Builder {
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

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder dataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Builder isPrimaryKey(Boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
            return this;
        }

        public Builder isPartitionKey(Boolean isPartitionKey) {
            this.isPartitionKey = isPartitionKey;
            return this;
        }

        public Builder isClusteringKey(Boolean isClusteringKey) {
            this.isClusteringKey = isClusteringKey;
            return this;
        }

        public Builder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        public Builder dimension(Integer dimension) {
            this.dimension = dimension;
            return this;
        }

        public Builder elementType(DataType elementType) {
            this.elementType = elementType;
            return this;
        }

        public Builder maxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
            return this;
        }

        public Builder isNullable(Boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            this.enableDefaultValue = true;
            return this;
        }

        public Builder enableDefaultValue(boolean enableDefaultValue) {
            this.enableDefaultValue = enableDefaultValue;
            return this;
        }

        public Builder enableAnalyzer(Boolean enableAnalyzer) {
            this.enableAnalyzer = enableAnalyzer;
            return this;
        }

        public Builder analyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
            return this;
        }

        public Builder enableMatch(Boolean enableMatch) {
            this.enableMatch = enableMatch;
            return this;
        }

        public Builder typeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
            return this;
        }

        public Builder multiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
            this.multiAnalyzerParams = multiAnalyzerParams;
            return this;
        }

        public AddFieldReq build() {
            return new AddFieldReq(this);
        }
    }
}

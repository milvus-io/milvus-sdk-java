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

import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.SchemaUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateCollectionReq {
    private String databaseName;
    private String collectionName;
    private String description = "";
    private Integer dimension;

    private String primaryFieldName = "id";
    private DataType idType = DataType.Int64;
    private Integer maxLength = 65535;
    private String vectorFieldName = "vector";
    private String metricType = IndexParam.MetricType.COSINE.name();
    private Boolean autoID = Boolean.FALSE;

    // used by quickly create collections and create collections with schema
    // Note: This property is only for fast creating collection. If user use CollectionSchema to create a collection,
    //       the CollectionSchema.enableDynamicField must equal to CreateCollectionReq.enableDynamicField.
    private Boolean enableDynamicField = Boolean.TRUE;
    private Integer numShards = 1;

    // create collections with schema
    private CollectionSchema collectionSchema;

    private List<IndexParam> indexParams = new ArrayList<>();

    //private String partitionKeyField;
    private Integer numPartitions;

    private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;

    private final Map<String, String> properties = new HashMap<>();

    private CreateCollectionReq(Builder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }

        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.description = builder.description;
        this.dimension = builder.dimension;
        this.primaryFieldName = builder.primaryFieldName;
        this.idType = builder.idType;
        this.maxLength = builder.maxLength;
        this.vectorFieldName = builder.vectorFieldName;
        this.metricType = builder.metricType;
        this.autoID = builder.autoID;
        this.enableDynamicField = builder.enableDynamicField;
        this.numShards = builder.numShards;
        this.collectionSchema = builder.collectionSchema;
        this.indexParams = builder.indexParams;
        this.numPartitions = builder.numPartitions;
        this.consistencyLevel = builder.consistencyLevel;
        if (builder.properties != null) {
            this.properties.putAll(builder.properties);
        }
    }

    // Getters and Setters
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getDimension() {
        return dimension;
    }

    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    public String getPrimaryFieldName() {
        return primaryFieldName;
    }

    public void setPrimaryFieldName(String primaryFieldName) {
        this.primaryFieldName = primaryFieldName;
    }

    public DataType getIdType() {
        return idType;
    }

    public void setIdType(DataType idType) {
        this.idType = idType;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public void setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public Boolean getAutoID() {
        return autoID;
    }

    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    public Boolean getEnableDynamicField() {
        return enableDynamicField;
    }

    public void setEnableDynamicField(Boolean enableDynamicField) {
        this.enableDynamicField = enableDynamicField;
    }

    public Integer getNumShards() {
        return numShards;
    }

    public void setNumShards(Integer numShards) {
        this.numShards = numShards;
    }

    public CollectionSchema getCollectionSchema() {
        return collectionSchema;
    }

    public void setCollectionSchema(CollectionSchema collectionSchema) {
        this.collectionSchema = collectionSchema;
    }

    public List<IndexParam> getIndexParams() {
        return indexParams;
    }

    public void setIndexParams(List<IndexParam> indexParams) {
        this.indexParams = indexParams;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CreateCollectionReq that = (CreateCollectionReq) obj;
        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(collectionName, that.collectionName)
                .append(description, that.description)
                .append(dimension, that.dimension)
                .append(primaryFieldName, that.primaryFieldName)
                .append(idType, that.idType)
                .append(maxLength, that.maxLength)
                .append(vectorFieldName, that.vectorFieldName)
                .append(metricType, that.metricType)
                .append(autoID, that.autoID)
                .append(enableDynamicField, that.enableDynamicField)
                .append(numShards, that.numShards)
                .append(collectionSchema, that.collectionSchema)
                .append(indexParams, that.indexParams)
                .append(numPartitions, that.numPartitions)
                .append(consistencyLevel, that.consistencyLevel)
                .append(properties, that.properties)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionName)
                .append(description)
                .append(dimension)
                .append(primaryFieldName)
                .append(idType)
                .append(maxLength)
                .append(vectorFieldName)
                .append(metricType)
                .append(autoID)
                .append(enableDynamicField)
                .append(numShards)
                .append(collectionSchema)
                .append(indexParams)
                .append(numPartitions)
                .append(consistencyLevel)
                .append(properties)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CreateCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", description='" + description + '\'' +
                ", dimension=" + dimension +
                ", primaryFieldName='" + primaryFieldName + '\'' +
                ", idType=" + idType +
                ", maxLength=" + maxLength +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", metricType='" + metricType + '\'' +
                ", autoID=" + autoID +
                ", enableDynamicField=" + enableDynamicField +
                ", numShards=" + numShards +
                ", collectionSchema=" + collectionSchema +
                ", indexParams=" + indexParams +
                ", numPartitions=" + numPartitions +
                ", consistencyLevel=" + consistencyLevel +
                ", properties=" + properties +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseName;
        private String collectionName;
        private String description = "";
        private Integer dimension;
        private String primaryFieldName = "id";
        private DataType idType = DataType.Int64;
        private Integer maxLength = 65535;
        private String vectorFieldName = "vector";
        private String metricType = IndexParam.MetricType.COSINE.name();
        private Boolean autoID = Boolean.FALSE;
        private Boolean enableDynamicField = Boolean.TRUE;
        private Integer numShards = 1;
        private CollectionSchema collectionSchema;
        private List<IndexParam> indexParams = new ArrayList<>();
        private Integer numPartitions;
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
        private Map<String, String> properties = new HashMap<>();
        private boolean enableDynamicFieldSet = false;

        private Builder() {}

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder dimension(Integer dimension) {
            this.dimension = dimension;
            return this;
        }

        public Builder primaryFieldName(String primaryFieldName) {
            this.primaryFieldName = primaryFieldName;
            return this;
        }

        public Builder idType(DataType idType) {
            this.idType = idType;
            return this;
        }

        public Builder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Builder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        public Builder metricType(String metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        public Builder numShards(Integer numShards) {
            this.numShards = numShards;
            return this;
        }

        public Builder indexParams(List<IndexParam> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        public Builder numPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder indexParam(IndexParam indexParam) {
            if (this.indexParams == null) {
                this.indexParams = new ArrayList<>();
            }
            try {
                this.indexParams.add(indexParam);
            } catch (UnsupportedOperationException _e) {
                this.indexParams = new ArrayList<>(this.indexParams);
                this.indexParams.add(indexParam);
            }
            return this;
        }

        public Builder enableDynamicField(Boolean enableDynamicField) {
            if (this.collectionSchema != null && (this.collectionSchema.isEnableDynamicField() != enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by CollectionSchema, not allow to set different value by enableDynamicField().");
            }
            this.enableDynamicField = enableDynamicField;
            this.enableDynamicFieldSet = true;
            return this;
        }

        public Builder collectionSchema(CollectionSchema collectionSchema) {
            if (this.enableDynamicFieldSet && (collectionSchema.isEnableDynamicField() != this.enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by enableDynamicField(), not allow to set different value by collectionSchema.");
            }
            this.collectionSchema = collectionSchema;
            this.enableDynamicField = collectionSchema.isEnableDynamicField();
            this.enableDynamicFieldSet = true;
            return this;
        }

        public Builder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        public CreateCollectionReq build() {
            return new CreateCollectionReq(this);
        }
    }

    public static class CollectionSchema {
        private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        private boolean enableDynamicField = false;
        private List<CreateCollectionReq.Function> functionList = new ArrayList<>();

        private CollectionSchema(Builder builder) {
            this.fieldSchemaList = builder.fieldSchemaList;
            this.enableDynamicField = builder.enableDynamicField;
            this.functionList = builder.functionList;
        }

        public CollectionSchema addField(AddFieldReq addFieldReq) {
            fieldSchemaList.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        public CollectionSchema addFunction(Function function) {
            functionList.add(function);
            return this;
        }

        public CreateCollectionReq.FieldSchema getField(String fieldName) {
            for (CreateCollectionReq.FieldSchema field : fieldSchemaList) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
        }

        public List<CreateCollectionReq.FieldSchema> getFieldSchemaList() {
            return fieldSchemaList;
        }

        public void setFieldSchemaList(List<CreateCollectionReq.FieldSchema> fieldSchemaList) {
            this.fieldSchemaList = fieldSchemaList;
        }

        public boolean isEnableDynamicField() {
            return enableDynamicField;
        }

        public void setEnableDynamicField(boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
        }

        public List<CreateCollectionReq.Function> getFunctionList() {
            return functionList;
        }

        public void setFunctionList(List<CreateCollectionReq.Function> functionList) {
            this.functionList = functionList;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            CollectionSchema that = (CollectionSchema) obj;
            return new EqualsBuilder()
                    .append(enableDynamicField, that.enableDynamicField)
                    .append(fieldSchemaList, that.fieldSchemaList)
                    .append(functionList, that.functionList)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fieldSchemaList)
                    .append(enableDynamicField)
                    .append(functionList)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "CollectionSchema{" +
                    "fieldSchemaList=" + fieldSchemaList +
                    ", enableDynamicField=" + enableDynamicField +
                    ", functionList=" + functionList +
                    '}';
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
            private boolean enableDynamicField = false;
            private List<CreateCollectionReq.Function> functionList = new ArrayList<>();

            private Builder() {}

            public Builder fieldSchemaList(List<CreateCollectionReq.FieldSchema> fieldSchemaList) {
                this.fieldSchemaList = fieldSchemaList;
                return this;
            }

            public Builder enableDynamicField(boolean enableDynamicField) {
                this.enableDynamicField = enableDynamicField;
                return this;
            }

            public Builder functionList(List<CreateCollectionReq.Function> functionList) {
                this.functionList = functionList;
                return this;
            }

            public CollectionSchema build() {
                return new CollectionSchema(this);
            }
        }
    }

    public static class FieldSchema {
        private String name;
        private String description = "";
        private DataType dataType;
        private Integer maxLength = 65535;
        private Integer dimension;
        private Boolean isPrimaryKey = Boolean.FALSE;
        private Boolean isPartitionKey = Boolean.FALSE;
        private Boolean isClusteringKey = Boolean.FALSE;
        private Boolean autoID = Boolean.FALSE;
        private DataType elementType;
        private Integer maxCapacity;
        private Boolean isNullable = Boolean.FALSE; // only for scalar fields(not include Array fields)
        private Object defaultValue = null; // only for scalar fields
        private Boolean enableAnalyzer; // for BM25 tokenizer
        private Map<String, Object> analyzerParams; // for BM25 tokenizer
        private Boolean enableMatch; // for BM25 keyword search

        // If a specific field, such as maxLength, has been specified, it will override the corresponding key's value in typeParams.
        private Map<String, String> typeParams;
        private Map<String, Object> multiAnalyzerParams; // for multi‑language analyzers

        private FieldSchema(Builder builder) {
            this.name = builder.name;
            this.description = builder.description;
            this.dataType = builder.dataType;
            this.maxLength = builder.maxLength;
            this.dimension = builder.dimension;
            this.isPrimaryKey = builder.isPrimaryKey;
            this.isPartitionKey = builder.isPartitionKey;
            this.isClusteringKey = builder.isClusteringKey;
            this.autoID = builder.autoID;
            this.elementType = builder.elementType;
            this.maxCapacity = builder.maxCapacity;
            this.isNullable = builder.isNullable;
            this.defaultValue = builder.defaultValue;
            this.enableAnalyzer = builder.enableAnalyzer;
            this.analyzerParams = builder.analyzerParams;
            this.enableMatch = builder.enableMatch;
            this.typeParams = builder.typeParams;
            this.multiAnalyzerParams = builder.multiAnalyzerParams;
        }

        // Getters and Setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public DataType getDataType() {
            return dataType;
        }

        public void setDataType(DataType dataType) {
            this.dataType = dataType;
        }

        public Integer getMaxLength() {
            return maxLength;
        }

        public void setMaxLength(Integer maxLength) {
            this.maxLength = maxLength;
        }

        public Integer getDimension() {
            return dimension;
        }

        public void setDimension(Integer dimension) {
            this.dimension = dimension;
        }

        public Boolean getIsPrimaryKey() {
            return isPrimaryKey;
        }

        public void setIsPrimaryKey(Boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
        }

        public Boolean getIsPartitionKey() {
            return isPartitionKey;
        }

        public void setIsPartitionKey(Boolean isPartitionKey) {
            this.isPartitionKey = isPartitionKey;
        }

        public Boolean getIsClusteringKey() {
            return isClusteringKey;
        }

        public void setIsClusteringKey(Boolean isClusteringKey) {
            this.isClusteringKey = isClusteringKey;
        }

        public Boolean getAutoID() {
            return autoID;
        }

        public void setAutoID(Boolean autoID) {
            this.autoID = autoID;
        }

        public DataType getElementType() {
            return elementType;
        }

        public void setElementType(DataType elementType) {
            this.elementType = elementType;
        }

        public Integer getMaxCapacity() {
            return maxCapacity;
        }

        public void setMaxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
        }

        public Boolean getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(Boolean isNullable) {
            this.isNullable = isNullable;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        public Boolean getEnableAnalyzer() {
            return enableAnalyzer;
        }

        public void setEnableAnalyzer(Boolean enableAnalyzer) {
            this.enableAnalyzer = enableAnalyzer;
        }

        public Map<String, Object> getAnalyzerParams() {
            return analyzerParams;
        }

        public void setAnalyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
        }

        public Boolean getEnableMatch() {
            return enableMatch;
        }

        public void setEnableMatch(Boolean enableMatch) {
            this.enableMatch = enableMatch;
        }

        public Map<String, String> getTypeParams() {
            return typeParams;
        }

        public void setTypeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
        }

        public Map<String, Object> getMultiAnalyzerParams() {
            return multiAnalyzerParams;
        }

        public void setMultiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
            this.multiAnalyzerParams = multiAnalyzerParams;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            FieldSchema that = (FieldSchema) obj;
            return new EqualsBuilder()
                    .append(name, that.name)
                    .append(description, that.description)
                    .append(dataType, that.dataType)
                    .append(maxLength, that.maxLength)
                    .append(dimension, that.dimension)
                    .append(isPrimaryKey, that.isPrimaryKey)
                    .append(isPartitionKey, that.isPartitionKey)
                    .append(isClusteringKey, that.isClusteringKey)
                    .append(autoID, that.autoID)
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
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(description)
                    .append(dataType)
                    .append(maxLength)
                    .append(dimension)
                    .append(isPrimaryKey)
                    .append(isPartitionKey)
                    .append(isClusteringKey)
                    .append(autoID)
                    .append(elementType)
                    .append(maxCapacity)
                    .append(isNullable)
                    .append(defaultValue)
                    .append(enableAnalyzer)
                    .append(analyzerParams)
                    .append(enableMatch)
                    .append(typeParams)
                    .append(multiAnalyzerParams)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "FieldSchema{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", dataType=" + dataType +
                    ", maxLength=" + maxLength +
                    ", dimension=" + dimension +
                    ", isPrimaryKey=" + isPrimaryKey +
                    ", isPartitionKey=" + isPartitionKey +
                    ", isClusteringKey=" + isClusteringKey +
                    ", autoID=" + autoID +
                    ", elementType=" + elementType +
                    ", maxCapacity=" + maxCapacity +
                    ", isNullable=" + isNullable +
                    ", defaultValue=" + defaultValue +
                    ", enableAnalyzer=" + enableAnalyzer +
                    ", analyzerParams=" + analyzerParams +
                    ", enableMatch=" + enableMatch +
                    ", typeParams=" + typeParams +
                    ", multiAnalyzerParams=" + multiAnalyzerParams +
                    '}';
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String name;
            private String description = "";
            private DataType dataType;
            private Integer maxLength = 65535;
            private Integer dimension;
            private Boolean isPrimaryKey = Boolean.FALSE;
            private Boolean isPartitionKey = Boolean.FALSE;
            private Boolean isClusteringKey = Boolean.FALSE;
            private Boolean autoID = Boolean.FALSE;
            private DataType elementType;
            private Integer maxCapacity;
            private Boolean isNullable = Boolean.FALSE;
            private Object defaultValue = null;
            private Boolean enableAnalyzer;
            private Map<String, Object> analyzerParams;
            private Boolean enableMatch;
            private Map<String, String> typeParams;
            private Map<String, Object> multiAnalyzerParams;

            private Builder() {}

            public Builder name(String name) {
                this.name = name;
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

            public Builder dimension(Integer dimension) {
                this.dimension = dimension;
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

            public FieldSchema build() {
                return new FieldSchema(this);
            }
        }
    }

    public static class Function {
        private String name;
        private String description = "";
        private FunctionType functionType = FunctionType.UNKNOWN;
        private List<String> inputFieldNames = new ArrayList<>();
        private List<String> outputFieldNames = new ArrayList<>();
        private Map<String, String> params = new HashMap<>();

        protected Function(FunctionBuilder builder) {
            this.name = builder.name;
            this.description = builder.description;
            this.functionType = builder.functionType;
            this.inputFieldNames = builder.inputFieldNames;
            this.outputFieldNames = builder.outputFieldNames;
            this.params = builder.params;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public FunctionType getFunctionType() {
            return functionType;
        }

        public void setFunctionType(FunctionType functionType) {
            this.functionType = functionType;
        }

        public List<String> getInputFieldNames() {
            return inputFieldNames;
        }

        public void setInputFieldNames(List<String> inputFieldNames) {
            this.inputFieldNames = inputFieldNames;
        }

        public List<String> getOutputFieldNames() {
            return outputFieldNames;
        }

        public void setOutputFieldNames(List<String> outputFieldNames) {
            this.outputFieldNames = outputFieldNames;
        }

        public Map<String, String> getParams() {
            return params;
        }

        public void setParams(Map<String, String> params) {
            this.params = params;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Function function = (Function) obj;
            return new EqualsBuilder()
                    .append(name, function.name)
                    .append(description, function.description)
                    .append(functionType, function.functionType)
                    .append(inputFieldNames, function.inputFieldNames)
                    .append(outputFieldNames, function.outputFieldNames)
                    .append(params, function.params)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(description)
                    .append(functionType)
                    .append(inputFieldNames)
                    .append(outputFieldNames)
                    .append(params)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "Function{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", functionType=" + functionType +
                    ", inputFieldNames=" + inputFieldNames +
                    ", outputFieldNames=" + outputFieldNames +
                    ", params=" + params +
                    '}';
        }

        public static FunctionBuilder builder() {
            return new FunctionBuilder();
        }

        public static class FunctionBuilder {
            private String name;
            private String description = "";
            private FunctionType functionType = FunctionType.UNKNOWN;
            private List<String> inputFieldNames = new ArrayList<>();
            private List<String> outputFieldNames = new ArrayList<>();
            private Map<String, String> params = new HashMap<>();

            protected FunctionBuilder() {}

            public FunctionBuilder name(String name) {
                this.name = name;
                return this;
            }

            public FunctionBuilder description(String description) {
                this.description = description;
                return this;
            }

            public FunctionBuilder functionType(FunctionType functionType) {
                this.functionType = functionType;
                return this;
            }

            public FunctionBuilder inputFieldNames(List<String> inputFieldNames) {
                this.inputFieldNames = inputFieldNames;
                return this;
            }

            public FunctionBuilder outputFieldNames(List<String> outputFieldNames) {
                this.outputFieldNames = outputFieldNames;
                return this;
            }

            public FunctionBuilder params(Map<String, String> params) {
                this.params = params;
                return this;
            }

            public FunctionBuilder param(String key, String value) {
                if (this.params == null) {
                    this.params = new HashMap<>();
                }
                this.params.put(key, value);
                return this;
            }

            public Function build() {
                return new Function(this);
            }
        }
    }
}

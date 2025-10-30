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
import io.milvus.exception.ParamException;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.SchemaUtils;

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

    private CreateCollectionReq(CreateCollectionReqBuilder builder) {
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

    public static CreateCollectionReqBuilder builder() {
        return new CreateCollectionReqBuilder();
    }

    public static class CreateCollectionReqBuilder {
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

        private CreateCollectionReqBuilder() {
        }

        public CreateCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CreateCollectionReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        public CreateCollectionReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        public CreateCollectionReqBuilder dimension(Integer dimension) {
            this.dimension = dimension;
            return this;
        }

        public CreateCollectionReqBuilder primaryFieldName(String primaryFieldName) {
            this.primaryFieldName = primaryFieldName;
            return this;
        }

        public CreateCollectionReqBuilder idType(DataType idType) {
            this.idType = idType;
            return this;
        }

        public CreateCollectionReqBuilder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public CreateCollectionReqBuilder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        public CreateCollectionReqBuilder metricType(String metricType) {
            this.metricType = metricType;
            return this;
        }

        public CreateCollectionReqBuilder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        public CreateCollectionReqBuilder numShards(Integer numShards) {
            this.numShards = numShards;
            return this;
        }

        public CreateCollectionReqBuilder indexParams(List<IndexParam> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        public CreateCollectionReqBuilder numPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public CreateCollectionReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public CreateCollectionReqBuilder indexParam(IndexParam indexParam) {
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

        public CreateCollectionReqBuilder enableDynamicField(Boolean enableDynamicField) {
            if (this.collectionSchema != null && (this.collectionSchema.isEnableDynamicField() != enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by CollectionSchema, not allow to set different value by enableDynamicField().");
            }
            this.enableDynamicField = enableDynamicField;
            this.enableDynamicFieldSet = true;
            return this;
        }

        public CreateCollectionReqBuilder collectionSchema(CollectionSchema collectionSchema) {
            if (this.enableDynamicFieldSet && (collectionSchema.isEnableDynamicField() != this.enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by enableDynamicField(), not allow to set different value by collectionSchema.");
            }
            this.collectionSchema = collectionSchema;
            this.enableDynamicField = collectionSchema.isEnableDynamicField();
            this.enableDynamicFieldSet = true;
            return this;
        }

        public CreateCollectionReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public CreateCollectionReqBuilder property(String key, String value) {
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
        private List<CreateCollectionReq.StructFieldSchema> structFields = new ArrayList<>();

        private boolean enableDynamicField = false;
        private List<CreateCollectionReq.Function> functionList = new ArrayList<>();

        private CollectionSchema(CollectionSchemaBuilder builder) {
            this.fieldSchemaList = builder.fieldSchemaList;
            this.structFields = builder.structFields;
            this.enableDynamicField = builder.enableDynamicField;
            this.functionList = builder.functionList;
        }

        public CollectionSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array && addFieldReq.getElementType() == DataType.Struct) {
                structFields.add(SchemaUtils.convertFieldReqToStructFieldSchema(addFieldReq));
            } else {
                fieldSchemaList.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            }
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

        public List<CreateCollectionReq.StructFieldSchema> getStructFields() {
            return structFields;
        }

        public void setStructFields(List<CreateCollectionReq.StructFieldSchema> structFields) {
            this.structFields = structFields;
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
        public String toString() {
            return "CollectionSchema{" +
                    "fieldSchemaList=" + fieldSchemaList +
                    ", structFields=" + structFields +
                    ", enableDynamicField=" + enableDynamicField +
                    ", functionList=" + functionList +
                    '}';
        }

        public static CollectionSchemaBuilder builder() {
            return new CollectionSchemaBuilder();
        }

        public static class CollectionSchemaBuilder {
            private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
            private List<CreateCollectionReq.StructFieldSchema> structFields = new ArrayList<>();
            private boolean enableDynamicField = false;
            private List<CreateCollectionReq.Function> functionList = new ArrayList<>();

            private CollectionSchemaBuilder() {
            }

            public CollectionSchemaBuilder fieldSchemaList(List<CreateCollectionReq.FieldSchema> fieldSchemaList) {
                this.fieldSchemaList = fieldSchemaList;
                return this;
            }

            public CollectionSchemaBuilder structFields(List<CreateCollectionReq.StructFieldSchema> structFields) {
                this.structFields = structFields;
                return this;
            }

            public CollectionSchemaBuilder enableDynamicField(boolean enableDynamicField) {
                this.enableDynamicField = enableDynamicField;
                return this;
            }

            public CollectionSchemaBuilder functionList(List<CreateCollectionReq.Function> functionList) {
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

        private FieldSchema(FieldSchemaBuilder builder) {
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

        public static FieldSchemaBuilder builder() {
            return new FieldSchemaBuilder();
        }

        public static class FieldSchemaBuilder {
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

            private FieldSchemaBuilder() {
            }

            public FieldSchemaBuilder name(String name) {
                this.name = name;
                return this;
            }

            public FieldSchemaBuilder description(String description) {
                this.description = description;
                return this;
            }

            public FieldSchemaBuilder dataType(DataType dataType) {
                this.dataType = dataType;
                return this;
            }

            public FieldSchemaBuilder maxLength(Integer maxLength) {
                this.maxLength = maxLength;
                return this;
            }

            public FieldSchemaBuilder dimension(Integer dimension) {
                this.dimension = dimension;
                return this;
            }

            public FieldSchemaBuilder isPrimaryKey(Boolean isPrimaryKey) {
                this.isPrimaryKey = isPrimaryKey;
                return this;
            }

            public FieldSchemaBuilder isPartitionKey(Boolean isPartitionKey) {
                this.isPartitionKey = isPartitionKey;
                return this;
            }

            public FieldSchemaBuilder isClusteringKey(Boolean isClusteringKey) {
                this.isClusteringKey = isClusteringKey;
                return this;
            }

            public FieldSchemaBuilder autoID(Boolean autoID) {
                this.autoID = autoID;
                return this;
            }

            public FieldSchemaBuilder elementType(DataType elementType) {
                this.elementType = elementType;
                return this;
            }

            public FieldSchemaBuilder maxCapacity(Integer maxCapacity) {
                this.maxCapacity = maxCapacity;
                return this;
            }

            public FieldSchemaBuilder isNullable(Boolean isNullable) {
                this.isNullable = isNullable;
                return this;
            }

            public FieldSchemaBuilder defaultValue(Object defaultValue) {
                this.defaultValue = defaultValue;
                return this;
            }

            public FieldSchemaBuilder enableAnalyzer(Boolean enableAnalyzer) {
                this.enableAnalyzer = enableAnalyzer;
                return this;
            }

            public FieldSchemaBuilder analyzerParams(Map<String, Object> analyzerParams) {
                this.analyzerParams = analyzerParams;
                return this;
            }

            public FieldSchemaBuilder enableMatch(Boolean enableMatch) {
                this.enableMatch = enableMatch;
                return this;
            }

            public FieldSchemaBuilder typeParams(Map<String, String> typeParams) {
                this.typeParams = typeParams;
                return this;
            }

            public FieldSchemaBuilder multiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
                this.multiAnalyzerParams = multiAnalyzerParams;
                return this;
            }

            public FieldSchema build() {
                return new FieldSchema(this);
            }
        }
    }

    public static class Function {
        private String name = "";
        private String description = "";
        private FunctionType functionType = FunctionType.UNKNOWN;
        private List<String> inputFieldNames = new ArrayList<>();
        private List<String> outputFieldNames = new ArrayList<>();
        private Map<String, String> params = new HashMap<>();

        protected Function(FunctionBuilder<?> builder) {
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

        public static FunctionBuilder<?> builder() {
            return new FunctionBuilder<>();
        }

        public static class FunctionBuilder<T extends FunctionBuilder<T>> {
            private String name = "";
            private String description = "";
            private FunctionType functionType = FunctionType.UNKNOWN;
            private List<String> inputFieldNames = new ArrayList<>();
            private List<String> outputFieldNames = new ArrayList<>();
            private Map<String, String> params = new HashMap<>();

            protected FunctionBuilder() {
            }

            public T name(String name) {
                this.name = name;
                return (T) this;
            }

            public T description(String description) {
                this.description = description;
                return (T) this;
            }

            public T functionType(FunctionType functionType) {
                this.functionType = functionType;
                return (T) this;
            }

            public T inputFieldNames(List<String> inputFieldNames) {
                this.inputFieldNames = inputFieldNames;
                return (T) this;
            }

            public T outputFieldNames(List<String> outputFieldNames) {
                this.outputFieldNames = outputFieldNames;
                return (T) this;
            }

            public T params(Map<String, String> params) {
                this.params = params;
                return (T) this;
            }

            public T param(String key, String value) {
                if (this.params == null) {
                    this.params = new HashMap<>();
                }
                this.params.put(key, value);
                return (T) this;
            }

            public Function build() {
                return new Function(this);
            }
        }
    }

    public static class StructFieldSchema {
        private String name;
        private String description = "";
        private List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        private Integer maxCapacity;

        private StructFieldSchema(StructFieldSchemaBuilder builder) {
            this.name = builder.name;
            this.description = builder.description;
            this.fields = builder.fields;
            this.maxCapacity = builder.maxCapacity;
        }

        public StructFieldSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array || addFieldReq.getElementType() == DataType.Struct) {
                throw new ParamException("Struct field schema does not support Array, ArrayOfVector or Struct");
            }
            fields.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        public DataType getDataType() {
            return DataType.Array;
        }

        public DataType getElementType() {
            return DataType.Struct;
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

        public List<CreateCollectionReq.FieldSchema> getFields() {
            return fields;
        }

        public void setFields(List<CreateCollectionReq.FieldSchema> fields) {
            this.fields = fields;
        }

        public Integer getMaxCapacity() {
            return maxCapacity;
        }

        public void setMaxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
        }

        @Override
        public String toString() {
            return "StructFieldSchema{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", fields=" + fields +
                    ", maxCapacity=" + maxCapacity +
                    '}';
        }

        public static StructFieldSchemaBuilder builder() {
            return new StructFieldSchemaBuilder();
        }

        public static class StructFieldSchemaBuilder {
            private String name;
            private String description = "";
            private List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
            private Integer maxCapacity;

            private StructFieldSchemaBuilder() {
            }

            public StructFieldSchemaBuilder name(String name) {
                this.name = name;
                return this;
            }

            public StructFieldSchemaBuilder description(String description) {
                this.description = description;
                return this;
            }

            public StructFieldSchemaBuilder fields(List<CreateCollectionReq.FieldSchema> fields) {
                this.fields = fields;
                return this;
            }

            public StructFieldSchemaBuilder maxCapacity(Integer maxCapacity) {
                this.maxCapacity = maxCapacity;
                return this;
            }

            public StructFieldSchema build() {
                return new StructFieldSchema(this);
            }
        }
    }
}

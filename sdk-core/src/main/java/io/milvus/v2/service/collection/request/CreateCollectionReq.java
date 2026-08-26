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

import com.google.gson.JsonObject;
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

/**
 * Request parameters for the {@code createCollection} API.
 */
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
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        this.collectionName = collectionName;
    }

    /**
     * Returns the description of the collection.
     *
     * @return the collection description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the collection.
     *
     * @param description the collection description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the dimension of the vector field, used when quickly creating a collection.
     *
     * @return the vector dimension
     */
    public Integer getDimension() {
        return dimension;
    }

    /**
     * Sets the dimension of the vector field, used when quickly creating a collection.
     *
     * @param dimension the vector dimension
     */
    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    /**
     * Returns the name of the primary key field, used when quickly creating a collection.
     *
     * @return the primary field name
     */
    public String getPrimaryFieldName() {
        return primaryFieldName;
    }

    /**
     * Sets the name of the primary key field, used when quickly creating a collection.
     *
     * @param primaryFieldName the primary field name
     */
    public void setPrimaryFieldName(String primaryFieldName) {
        this.primaryFieldName = primaryFieldName;
    }

    /**
     * Returns the data type of the primary key field, used when quickly creating a collection.
     *
     * @return the primary field data type
     */
    public DataType getIdType() {
        return idType;
    }

    /**
     * Sets the data type of the primary key field, used when quickly creating a collection.
     *
     * @param idType the primary field data type
     */
    public void setIdType(DataType idType) {
        this.idType = idType;
    }

    /**
     * Returns the maximum length of the primary key field, used when quickly creating a collection.
     *
     * @return the primary field max length
     */
    public Integer getMaxLength() {
        return maxLength;
    }

    /**
     * Sets the maximum length of the primary key field, used when quickly creating a collection.
     *
     * @param maxLength the primary field max length
     */
    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * Returns the name of the vector field, used when quickly creating a collection.
     *
     * @return the vector field name
     */
    public String getVectorFieldName() {
        return vectorFieldName;
    }

    /**
     * Sets the name of the vector field, used when quickly creating a collection.
     *
     * @param vectorFieldName the vector field name
     */
    public void setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
    }

    /**
     * Returns the metric type of the vector field, used when quickly creating a collection.
     *
     * @return the metric type
     */
    public String getMetricType() {
        return metricType;
    }

    /**
     * Sets the metric type of the vector field, used when quickly creating a collection.
     *
     * @param metricType the metric type
     */
    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    /**
     * Returns whether auto-generated IDs are enabled for the primary key field.
     *
     * @return {@code true} if auto ID is enabled
     */
    public Boolean getAutoID() {
        return autoID;
    }

    /**
     * Sets whether auto-generated IDs are enabled for the primary key field.
     *
     * @param autoID {@code true} to enable auto ID
     */
    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    /**
     * Returns whether the dynamic field is enabled for the collection.
     *
     * @return {@code true} if the dynamic field is enabled
     */
    public Boolean getEnableDynamicField() {
        return enableDynamicField;
    }

    /**
     * Sets whether the dynamic field is enabled for the collection.
     *
     * @param enableDynamicField {@code true} to enable the dynamic field
     */
    public void setEnableDynamicField(Boolean enableDynamicField) {
        this.enableDynamicField = enableDynamicField;
    }

    /**
     * Returns the number of shards of the collection.
     *
     * @return the number of shards
     */
    public Integer getNumShards() {
        return numShards;
    }

    /**
     * Sets the number of shards of the collection.
     *
     * @param numShards the number of shards
     */
    public void setNumShards(Integer numShards) {
        this.numShards = numShards;
    }

    /**
     * Returns the collection schema used to create the collection.
     *
     * @return the collection schema
     */
    public CollectionSchema getCollectionSchema() {
        return collectionSchema;
    }

    /**
     * Sets the collection schema used to create the collection.
     *
     * @param collectionSchema the collection schema
     */
    public void setCollectionSchema(CollectionSchema collectionSchema) {
        this.collectionSchema = collectionSchema;
    }

    /**
     * Returns the index parameters to create together with the collection.
     *
     * @return the index parameters
     */
    public List<IndexParam> getIndexParams() {
        return indexParams;
    }

    /**
     * Sets the index parameters to create together with the collection.
     *
     * @param indexParams the index parameters
     */
    public void setIndexParams(List<IndexParam> indexParams) {
        this.indexParams = indexParams;
    }

    /**
     * Returns the number of partitions of the collection.
     *
     * @return the number of partitions
     */
    public Integer getNumPartitions() {
        return numPartitions;
    }

    /**
     * Sets the number of partitions of the collection.
     *
     * @param numPartitions the number of partitions
     */
    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Returns the consistency level of the collection.
     *
     * @return the consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Sets the consistency level of the collection.
     *
     * @param consistencyLevel the consistency level
     */
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    /**
     * Returns the properties of the collection.
     *
     * @return the collection properties
     */
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

    /**
     * Creates a new builder for {@link CreateCollectionReq}.
     *
     * @return the builder
     */
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

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public CreateCollectionReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public CreateCollectionReqBuilder collectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("Collection name cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the description of the collection.
         *
         * @param description the collection description
         * @return this builder
         */
        public CreateCollectionReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the dimension of the vector field, used when quickly creating a collection.
         *
         * @param dimension the vector dimension
         * @return this builder
         */
        public CreateCollectionReqBuilder dimension(Integer dimension) {
            this.dimension = dimension;
            return this;
        }

        /**
         * Sets the name of the primary key field, used when quickly creating a collection.
         *
         * @param primaryFieldName the primary field name
         * @return this builder
         */
        public CreateCollectionReqBuilder primaryFieldName(String primaryFieldName) {
            this.primaryFieldName = primaryFieldName;
            return this;
        }

        /**
         * Sets the data type of the primary key field, used when quickly creating a collection.
         *
         * @param idType the primary field data type
         * @return this builder
         */
        public CreateCollectionReqBuilder idType(DataType idType) {
            this.idType = idType;
            return this;
        }

        /**
         * Sets the maximum length of the primary key field, used when quickly creating a collection.
         *
         * @param maxLength the primary field max length
         * @return this builder
         */
        public CreateCollectionReqBuilder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /**
         * Sets the name of the vector field, used when quickly creating a collection.
         *
         * @param vectorFieldName the vector field name
         * @return this builder
         */
        public CreateCollectionReqBuilder vectorFieldName(String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets the metric type of the vector field, used when quickly creating a collection.
         *
         * @param metricType the metric type
         * @return this builder
         */
        public CreateCollectionReqBuilder metricType(String metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets whether auto-generated IDs are enabled for the primary key field.
         *
         * @param autoID {@code true} to enable auto ID
         * @return this builder
         */
        public CreateCollectionReqBuilder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        /**
         * Sets the number of shards of the collection.
         *
         * @param numShards the number of shards
         * @return this builder
         */
        public CreateCollectionReqBuilder numShards(Integer numShards) {
            this.numShards = numShards;
            return this;
        }

        /**
         * Sets the index parameters to create together with the collection.
         *
         * @param indexParams the index parameters
         * @return this builder
         */
        public CreateCollectionReqBuilder indexParams(List<IndexParam> indexParams) {
            this.indexParams = indexParams;
            return this;
        }

        /**
         * Sets the number of partitions of the collection.
         *
         * @param numPartitions the number of partitions
         * @return this builder
         */
        public CreateCollectionReqBuilder numPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Sets the consistency level of the collection.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public CreateCollectionReqBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Appends a single index parameter to create together with the collection.
         *
         * @param indexParam the index parameters
         * @return this builder
         */
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

        /**
         * Sets whether the dynamic field is enabled for the collection.
         *
         * @param enableDynamicField {@code true} to enable the dynamic field
         * @return this builder
         * @throws MilvusClientException if the flag conflicts with the one set by the collection schema
         */
        public CreateCollectionReqBuilder enableDynamicField(Boolean enableDynamicField) {
            if (this.collectionSchema != null && (this.collectionSchema.isEnableDynamicField() != enableDynamicField)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "The enableDynamicField flag has been set by CollectionSchema, not allow to set different value by enableDynamicField().");
            }
            this.enableDynamicField = enableDynamicField;
            this.enableDynamicFieldSet = true;
            return this;
        }

        /**
         * Sets the collection schema used to create the collection.
         *
         * @param collectionSchema the collection schema
         * @return this builder
         * @throws MilvusClientException if the schema's dynamic-field flag conflicts with the one set
         *                               by {@link #enableDynamicField(Boolean)}
         */
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

        /**
         * Sets the properties of the collection.
         *
         * @param properties the collection properties
         * @return this builder
         */
        public CreateCollectionReqBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Adds a single property to the collection.
         *
         * @param key the property key
         * @param value the property value
         * @return this builder
         */
        public CreateCollectionReqBuilder property(String key, String value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        /**
         * Builds a {@link CreateCollectionReq} with the configured parameters.
         *
         * @return the request
         */
        public CreateCollectionReq build() {
            return new CreateCollectionReq(this);
        }
    }

    /**
     * Schema definition of a collection, holding the field schema list, struct fields,
     * functions, dynamic-field flag and optional external source configuration.
     */
    public static class CollectionSchema {
        private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        private List<CreateCollectionReq.StructFieldSchema> structFields = new ArrayList<>();

        private boolean enableDynamicField = false;
        private List<CreateCollectionReq.Function> functionList = new ArrayList<>();
        private String externalSource = "";
        private JsonObject externalSpec;

        private CollectionSchema(CollectionSchemaBuilder builder) {
            this.fieldSchemaList = builder.fieldSchemaList;
            this.structFields = builder.structFields;
            this.enableDynamicField = builder.enableDynamicField;
            this.functionList = builder.functionList;
            this.externalSource = builder.externalSource;
            this.externalSpec = builder.externalSpec;
        }

        /**
         * Adds a field to the schema. Array-of-struct fields are stored as struct fields,
         * all other fields are stored as regular fields.
         *
         * @param addFieldReq the field to add
         * @return this schema
         */
        public CollectionSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array && addFieldReq.getElementType() == DataType.Struct) {
                structFields.add(SchemaUtils.convertFieldReqToStructFieldSchema(addFieldReq));
            } else {
                fieldSchemaList.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            }
            return this;
        }

        /**
         * Adds a function to the schema.
         *
         * @param function the function to add
         * @return this schema
         */
        public CollectionSchema addFunction(Function function) {
            functionList.add(function);
            return this;
        }

        /**
         * Returns the regular field with the given name, or {@code null} if not found.
         *
         * @param fieldName the field name
         * @return the field, or {@code null}
         */
        public CreateCollectionReq.FieldSchema getField(String fieldName) {
            for (CreateCollectionReq.FieldSchema field : fieldSchemaList) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
        }

        /**
         * Returns the list of regular fields in the schema.
         *
         * @return the field schema list
         */
        public List<CreateCollectionReq.FieldSchema> getFieldSchemaList() {
            return fieldSchemaList;
        }

        /**
         * Sets the list of regular fields in the schema.
         *
         * @param fieldSchemaList the field schema list
         */
        public void setFieldSchemaList(List<CreateCollectionReq.FieldSchema> fieldSchemaList) {
            this.fieldSchemaList = fieldSchemaList;
        }

        /**
         * Returns the struct field with the given name, or {@code null} if not found.
         *
         * @param fieldName the field name
         * @return the struct field, or {@code null}
         */
        public CreateCollectionReq.StructFieldSchema getStructField(String fieldName) {
            for (CreateCollectionReq.StructFieldSchema field : structFields) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return null;
        }

        /**
         * Returns the list of struct fields in the schema.
         *
         * @return the struct fields
         */
        public List<CreateCollectionReq.StructFieldSchema> getStructFields() {
            return structFields;
        }

        /**
         * Sets the list of struct fields in the schema.
         *
         * @param structFields the struct fields
         */
        public void setStructFields(List<CreateCollectionReq.StructFieldSchema> structFields) {
            this.structFields = structFields;
        }

        /**
         * Returns whether the dynamic field is enabled for the schema.
         *
         * @return {@code true} if the dynamic field is enabled
         */
        public boolean isEnableDynamicField() {
            return enableDynamicField;
        }

        /**
         * Sets whether the dynamic field is enabled for the schema.
         *
         * @param enableDynamicField {@code true} to enable the dynamic field
         */
        public void setEnableDynamicField(boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
        }

        /**
         * Returns the list of functions defined in the schema.
         *
         * @return the function list
         */
        public List<CreateCollectionReq.Function> getFunctionList() {
            return functionList;
        }

        /**
         * Sets the list of functions defined in the schema.
         *
         * @param functionList the function list
         */
        public void setFunctionList(List<CreateCollectionReq.Function> functionList) {
            this.functionList = functionList;
        }

        /**
         * Returns the external source name of the schema.
         *
         * @return the external source name
         */
        public String getExternalSource() {
            return externalSource;
        }

        /**
         * Sets the external source name of the schema.
         *
         * @param externalSource the external source name
         */
        public void setExternalSource(String externalSource) {
            this.externalSource = externalSource;
        }

        /**
         * Returns the external source specification of the schema.
         *
         * @return the external source specification
         */
        public JsonObject getExternalSpec() {
            return externalSpec;
        }

        /**
         * Sets the external source specification of the schema.
         *
         * @param externalSpec the external source specification
         */
        public void setExternalSpec(JsonObject externalSpec) {
            this.externalSpec = externalSpec;
        }

        @Override
        public String toString() {
            return "CollectionSchema{" +
                    "fieldSchemaList=" + fieldSchemaList +
                    ", structFields=" + structFields +
                    ", enableDynamicField=" + enableDynamicField +
                    ", functionList=" + functionList +
                    ", externalSource='" + externalSource + '\'' +
                    ", externalSpec=" + externalSpec +
                    '}';
        }

        /**
         * Creates a new builder for {@link CollectionSchema}.
         *
         * @return the builder
         */
        public static CollectionSchemaBuilder builder() {
            return new CollectionSchemaBuilder();
        }

        public static class CollectionSchemaBuilder {
            private List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
            private List<CreateCollectionReq.StructFieldSchema> structFields = new ArrayList<>();
            private boolean enableDynamicField = false;
            private List<CreateCollectionReq.Function> functionList = new ArrayList<>();
            private String externalSource = "";
            private JsonObject externalSpec;

            private CollectionSchemaBuilder() {
            }

            /**
             * Sets the list of regular fields in the schema.
             *
             * @param fieldSchemaList the field schema list
             * @return this builder
             */
            public CollectionSchemaBuilder fieldSchemaList(List<CreateCollectionReq.FieldSchema> fieldSchemaList) {
                this.fieldSchemaList = fieldSchemaList;
                return this;
            }

            /**
             * Sets the list of struct fields in the schema.
             *
             * @param structFields the struct fields
             * @return this builder
             */
            public CollectionSchemaBuilder structFields(List<CreateCollectionReq.StructFieldSchema> structFields) {
                this.structFields = structFields;
                return this;
            }

            /**
             * Sets whether the dynamic field is enabled for the schema.
             *
             * @param enableDynamicField {@code true} to enable the dynamic field
             * @return this builder
             */
            public CollectionSchemaBuilder enableDynamicField(boolean enableDynamicField) {
                this.enableDynamicField = enableDynamicField;
                return this;
            }

            /**
             * Sets the list of functions defined in the schema.
             *
             * @param functionList the function list
             * @return this builder
             */
            public CollectionSchemaBuilder functionList(List<CreateCollectionReq.Function> functionList) {
                this.functionList = functionList;
                return this;
            }

            /**
             * Sets the external source name of the schema.
             *
             * @param externalSource the external source name
             * @return this builder
             */
            public CollectionSchemaBuilder externalSource(String externalSource) {
                this.externalSource = externalSource;
                return this;
            }

            /**
             * Sets the external source specification of the schema.
             *
             * @param externalSpec the external source specification
             * @return this builder
             */
            public CollectionSchemaBuilder externalSpec(JsonObject externalSpec) {
                this.externalSpec = externalSpec;
                return this;
            }

            /**
             * Builds a {@link CollectionSchema} with the configured parameters.
             *
             * @return the schema
             */
            public CollectionSchema build() {
                return new CollectionSchema(this);
            }
        }
    }

    /**
     * Definition of a single field in a collection schema.
     */
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
        private String externalField = ""; // external field name mapping
        private Long fieldId; // field id, only populated by describe_collection
        private Boolean isDynamic; // whether this field is the dynamic field
        private Boolean isFunctionOutput; // whether this field is a function output field
        private List<Map<String, Object>> indexes = new ArrayList<>(); // only populated by describe_collection

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
            this.externalField = builder.externalField;
        }

        // Getters and Setters
        /**
         * Returns the field name.
         *
         * @return the field name
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the field name.
         *
         * @param name the field name
         */
        public void setName(String name) {
            this.name = name;
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
         * Sets the field description.
         *
         * @param description the field description
         */
        public void setDescription(String description) {
            this.description = description;
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
         * Sets the data type of the field.
         *
         * @param dataType the data type
         */
        public void setDataType(DataType dataType) {
            this.dataType = dataType;
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
         * Sets the maximum length allowed for the field.
         *
         * @param maxLength the max length
         */
        public void setMaxLength(Integer maxLength) {
            this.maxLength = maxLength;
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
         * Sets the dimension of the vector field.
         *
         * @param dimension the vector dimension
         */
        public void setDimension(Integer dimension) {
            this.dimension = dimension;
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
         * Sets whether the field is the primary key.
         *
         * @param isPrimaryKey {@code true} to make the field the primary key
         */
        public void setIsPrimaryKey(Boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
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
         * Sets whether the field is a partition key.
         *
         * @param isPartitionKey {@code true} to make the field a partition key
         */
        public void setIsPartitionKey(Boolean isPartitionKey) {
            this.isPartitionKey = isPartitionKey;
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
         * Sets whether the field is a clustering key.
         *
         * @param isClusteringKey {@code true} to make the field a clustering key
         */
        public void setIsClusteringKey(Boolean isClusteringKey) {
            this.isClusteringKey = isClusteringKey;
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
         * Sets whether the field has auto-generated IDs.
         *
         * @param autoID {@code true} to enable auto ID
         */
        public void setAutoID(Boolean autoID) {
            this.autoID = autoID;
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
         * Sets the element type of an Array field.
         *
         * @param elementType the element type
         */
        public void setElementType(DataType elementType) {
            this.elementType = elementType;
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
         * Sets the maximum number of elements an Array field can hold.
         *
         * @param maxCapacity the max capacity
         */
        public void setMaxCapacity(Integer maxCapacity) {
            this.maxCapacity = maxCapacity;
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
         * Sets whether the scalar field is nullable.
         *
         * @param isNullable {@code true} to make the field nullable
         */
        public void setIsNullable(Boolean isNullable) {
            this.isNullable = isNullable;
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
         * Sets the default value of the scalar field.
         *
         * @param defaultValue the default value
         */
        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
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
         * Sets whether the BM25 analyzer is enabled for this field.
         *
         * @param enableAnalyzer {@code true} to enable the analyzer
         */
        public void setEnableAnalyzer(Boolean enableAnalyzer) {
            this.enableAnalyzer = enableAnalyzer;
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
         * Sets the analyzer parameters for the BM25 tokenizer.
         *
         * @param analyzerParams the analyzer parameters
         */
        public void setAnalyzerParams(Map<String, Object> analyzerParams) {
            this.analyzerParams = analyzerParams;
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
         * Sets whether BM25 keyword matching is enabled for this field.
         *
         * @param enableMatch {@code true} to enable BM25 matching
         */
        public void setEnableMatch(Boolean enableMatch) {
            this.enableMatch = enableMatch;
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
         * Sets the type parameters of the field.
         *
         * @param typeParams the type parameters
         */
        public void setTypeParams(Map<String, String> typeParams) {
            this.typeParams = typeParams;
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
         * Sets the parameters of the multi-language analyzers.
         *
         * @param multiAnalyzerParams the multi-analyzer parameters
         */
        public void setMultiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
            this.multiAnalyzerParams = multiAnalyzerParams;
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
         * Sets the external field name this field maps to.
         *
         * @param externalField the external field name
         */
        public void setExternalField(String externalField) {
            this.externalField = externalField;
        }

        /**
         * Returns the field ID assigned by the server.
         *
         * @return the field ID
         */
        public Long getFieldId() {
            return fieldId;
        }

        /**
         * Returns whether this field is the dynamic field.
         *
         * @return {@code true} if the field is the dynamic field
         */
        public Boolean getIsDynamic() {
            return isDynamic;
        }

        /**
         * Returns whether this field is a function output field.
         *
         * @return {@code true} if the field is a function output field
         */
        public Boolean getIsFunctionOutput() {
            return isFunctionOutput;
        }

        /**
         * Returns the index definitions of this field.
         *
         * @return the field indexes
         */
        public List<Map<String, Object>> getIndexes() {
            return indexes;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setFieldId(Long fieldId) {
            this.fieldId = fieldId;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setIsDynamic(Boolean isDynamic) {
            this.isDynamic = isDynamic;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setIsFunctionOutput(Boolean isFunctionOutput) {
            this.isFunctionOutput = isFunctionOutput;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setIndexes(List<Map<String, Object>> indexes) {
            this.indexes = indexes;
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
                    ", externalField='" + externalField + '\'' +
                    ", indexes=" + indexes +
                    '}';
        }

        /**
         * Creates a new builder for {@link FieldSchema}.
         *
         * @return the builder
         */
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
            private String externalField = "";

            private FieldSchemaBuilder() {
            }

            /**
             * Sets the field name.
             *
             * @param name the field name
             * @return this builder
             */
            public FieldSchemaBuilder name(String name) {
                this.name = name;
                return this;
            }

            /**
             * Sets the field description.
             *
             * @param description the field description
             * @return this builder
             */
            public FieldSchemaBuilder description(String description) {
                this.description = description;
                return this;
            }

            /**
             * Sets the data type of the field.
             *
             * @param dataType the data type
             * @return this builder
             */
            public FieldSchemaBuilder dataType(DataType dataType) {
                this.dataType = dataType;
                return this;
            }

            /**
             * Sets the maximum length allowed for the field.
             *
             * @param maxLength the max length
             * @return this builder
             */
            public FieldSchemaBuilder maxLength(Integer maxLength) {
                this.maxLength = maxLength;
                return this;
            }

            /**
             * Sets the dimension of the vector field.
             *
             * @param dimension the vector dimension
             * @return this builder
             */
            public FieldSchemaBuilder dimension(Integer dimension) {
                this.dimension = dimension;
                return this;
            }

            /**
             * Sets whether the field is the primary key.
             *
             * @param isPrimaryKey {@code true} to make the field the primary key
             * @return this builder
             */
            public FieldSchemaBuilder isPrimaryKey(Boolean isPrimaryKey) {
                this.isPrimaryKey = isPrimaryKey;
                return this;
            }

            /**
             * Sets whether the field is a partition key.
             *
             * @param isPartitionKey {@code true} to make the field a partition key
             * @return this builder
             */
            public FieldSchemaBuilder isPartitionKey(Boolean isPartitionKey) {
                this.isPartitionKey = isPartitionKey;
                return this;
            }

            /**
             * Sets whether the field is a clustering key.
             *
             * @param isClusteringKey {@code true} to make the field a clustering key
             * @return this builder
             */
            public FieldSchemaBuilder isClusteringKey(Boolean isClusteringKey) {
                this.isClusteringKey = isClusteringKey;
                return this;
            }

            /**
             * Sets whether the field has auto-generated IDs.
             *
             * @param autoID {@code true} to enable auto ID
             * @return this builder
             */
            public FieldSchemaBuilder autoID(Boolean autoID) {
                this.autoID = autoID;
                return this;
            }

            /**
             * Sets the element type of an Array field.
             *
             * @param elementType the element type
             * @return this builder
             */
            public FieldSchemaBuilder elementType(DataType elementType) {
                this.elementType = elementType;
                return this;
            }

            /**
             * Sets the maximum number of elements an Array field can hold.
             *
             * @param maxCapacity the max capacity
             * @return this builder
             */
            public FieldSchemaBuilder maxCapacity(Integer maxCapacity) {
                this.maxCapacity = maxCapacity;
                return this;
            }

            /**
             * Sets whether the scalar field is nullable.
             *
             * @param isNullable {@code true} to make the field nullable
             * @return this builder
             */
            public FieldSchemaBuilder isNullable(Boolean isNullable) {
                this.isNullable = isNullable;
                return this;
            }

            /**
             * Sets the default value of the scalar field.
             *
             * @param defaultValue the default value
             * @return this builder
             */
            public FieldSchemaBuilder defaultValue(Object defaultValue) {
                this.defaultValue = defaultValue;
                return this;
            }

            /**
             * Sets whether the BM25 analyzer is enabled for this field.
             *
             * @param enableAnalyzer {@code true} to enable the analyzer
             * @return this builder
             */
            public FieldSchemaBuilder enableAnalyzer(Boolean enableAnalyzer) {
                this.enableAnalyzer = enableAnalyzer;
                return this;
            }

            /**
             * Sets the analyzer parameters for the BM25 tokenizer.
             *
             * @param analyzerParams the analyzer parameters
             * @return this builder
             */
            public FieldSchemaBuilder analyzerParams(Map<String, Object> analyzerParams) {
                this.analyzerParams = analyzerParams;
                return this;
            }

            /**
             * Sets whether BM25 keyword matching is enabled for this field.
             *
             * @param enableMatch {@code true} to enable BM25 matching
             * @return this builder
             */
            public FieldSchemaBuilder enableMatch(Boolean enableMatch) {
                this.enableMatch = enableMatch;
                return this;
            }

            /**
             * Sets the type parameters of the field.
             *
             * @param typeParams the type parameters
             * @return this builder
             */
            public FieldSchemaBuilder typeParams(Map<String, String> typeParams) {
                this.typeParams = typeParams;
                return this;
            }

            /**
             * Sets the parameters of the multi-language analyzers.
             *
             * @param multiAnalyzerParams the multi-analyzer parameters
             * @return this builder
             */
            public FieldSchemaBuilder multiAnalyzerParams(Map<String, Object> multiAnalyzerParams) {
                this.multiAnalyzerParams = multiAnalyzerParams;
                return this;
            }

            /**
             * Sets the external field name this field maps to.
             *
             * @param externalField the external field name
             * @return this builder
             */
            public FieldSchemaBuilder externalField(String externalField) {
                this.externalField = externalField;
                return this;
            }

            /**
             * Builds a {@link FieldSchema} with the configured parameters.
             *
             * @return the field schema
             */
            public FieldSchema build() {
                return new FieldSchema(this);
            }
        }
    }

    /**
     * Definition of a function in a collection schema, such as an embedding
     * or BM25 function, that maps input fields to output fields.
     */
    public static class Function {
        private String name = "";
        private String description = "";
        private FunctionType functionType = FunctionType.UNKNOWN;
        private List<String> inputFieldNames = new ArrayList<>();
        private List<String> outputFieldNames = new ArrayList<>();
        private Map<String, String> params = new HashMap<>();
        private Long id; // function id, only populated by describe_collection
        private List<Long> inputFieldIds = new ArrayList<>();
        private List<Long> outputFieldIds = new ArrayList<>();

        protected Function(FunctionBuilder<?> builder) {
            this.name = builder.name;
            this.description = builder.description;
            this.functionType = builder.functionType;
            this.inputFieldNames = builder.inputFieldNames;
            this.outputFieldNames = builder.outputFieldNames;
            this.params = builder.params;
        }

        /**
         * Returns the name of the function.
         *
         * @return the function name
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the name of the function.
         *
         * @param name the function name
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Returns the description of the function.
         *
         * @return the function description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Sets the description of the function.
         *
         * @param description the function description
         */
        public void setDescription(String description) {
            this.description = description;
        }

        /**
         * Returns the type of the function.
         *
         * @return the function type
         */
        public FunctionType getFunctionType() {
            return functionType;
        }

        /**
         * Sets the type of the function.
         *
         * @param functionType the function type
         */
        public void setFunctionType(FunctionType functionType) {
            this.functionType = functionType;
        }

        /**
         * Returns the names of the input fields of the function.
         *
         * @return the input field names
         */
        public List<String> getInputFieldNames() {
            return inputFieldNames;
        }

        /**
         * Sets the names of the input fields of the function.
         *
         * @param inputFieldNames the input field names
         */
        public void setInputFieldNames(List<String> inputFieldNames) {
            this.inputFieldNames = inputFieldNames;
        }

        /**
         * Returns the names of the output fields of the function.
         *
         * @return the output field names
         */
        public List<String> getOutputFieldNames() {
            return outputFieldNames;
        }

        /**
         * Sets the names of the output fields of the function.
         *
         * @param outputFieldNames the output field names
         */
        public void setOutputFieldNames(List<String> outputFieldNames) {
            this.outputFieldNames = outputFieldNames;
        }

        /**
         * Returns the parameters of the function.
         *
         * @return the function parameters
         */
        public Map<String, String> getParams() {
            return params;
        }

        /**
         * Sets the parameters of the function.
         *
         * @param params the function parameters
         */
        public void setParams(Map<String, String> params) {
            this.params = params;
        }

        /**
         * Returns the function ID assigned by the server.
         *
         * @return the function ID
         */
        public Long getId() {
            return id;
        }

        /**
         * Returns the IDs of the input fields of the function.
         *
         * @return the input field IDs
         */
        public List<Long> getInputFieldIds() {
            return inputFieldIds;
        }

        /**
         * Returns the IDs of the output fields of the function.
         *
         * @return the output field IDs
         */
        public List<Long> getOutputFieldIds() {
            return outputFieldIds;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setId(Long id) {
            this.id = id;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setInputFieldIds(List<Long> inputFieldIds) {
            this.inputFieldIds = inputFieldIds;
        }

        /**
         * Server-assigned attribute, populated only by describe_collection; not used during create_collection.
         */
        public void setOutputFieldIds(List<Long> outputFieldIds) {
            this.outputFieldIds = outputFieldIds;
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
                    ", id=" + id +
                    ", inputFieldIds=" + inputFieldIds +
                    ", outputFieldIds=" + outputFieldIds +
                    '}';
        }

        /**
         * Creates a new builder for {@link Function}.
         *
         * @return the builder
         */
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

            /**
             * Sets the name of the function.
             *
             * @param name the function name
             * @return this builder
             */
            public T name(String name) {
                this.name = name;
                return (T) this;
            }

            /**
             * Sets the description of the function.
             *
             * @param description the function description
             * @return this builder
             */
            public T description(String description) {
                this.description = description;
                return (T) this;
            }

            /**
             * Sets the type of the function.
             *
             * @param functionType the function type
             * @return this builder
             */
            public T functionType(FunctionType functionType) {
                this.functionType = functionType;
                return (T) this;
            }

            /**
             * Sets the names of the input fields of the function.
             *
             * @param inputFieldNames the input field names
             * @return this builder
             */
            public T inputFieldNames(List<String> inputFieldNames) {
                this.inputFieldNames = inputFieldNames;
                return (T) this;
            }

            /**
             * Sets the names of the output fields of the function.
             *
             * @param outputFieldNames the output field names
             * @return this builder
             */
            public T outputFieldNames(List<String> outputFieldNames) {
                this.outputFieldNames = outputFieldNames;
                return (T) this;
            }

            /**
             * Sets the parameters of the function.
             *
             * @param params the function parameters
             * @return this builder
             */
            public T params(Map<String, String> params) {
                this.params = params;
                return (T) this;
            }

            /**
             * Adds a single parameter to the function.
             *
             * @param key the parameter key
             * @param value the parameter value
             * @return this builder
             */
            public T param(String key, String value) {
                if (this.params == null) {
                    this.params = new HashMap<>();
                }
                this.params.put(key, value);
                return (T) this;
            }

            /**
             * Builds a {@link Function} with the configured parameters.
             *
             * @return the function
             */
            public Function build() {
                return new Function(this);
            }
        }
    }

    /**
     * Definition of a struct field in a collection schema. A struct field is stored as
     * an Array-of-Struct field whose element type is {@link DataType#Struct}.
     */
    public static class StructFieldSchema {
        private String name;
        private String description = "";
        private List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
        private Integer maxCapacity;
        private Boolean nullable = Boolean.FALSE;
        private Map<String, String> typeParams = new HashMap<>();

        private StructFieldSchema(StructFieldSchemaBuilder builder) {
            this.name = builder.name;
            this.description = builder.description;
            this.fields = builder.fields;
            this.maxCapacity = builder.maxCapacity;
            this.nullable = Boolean.TRUE.equals(builder.nullable);
            this.typeParams = builder.typeParams;
        }

        /**
         * Adds a sub-field to the struct field schema. Array, ArrayOfVector and Struct
         * element types are not supported.
         *
         * @param addFieldReq the sub-field to add
         * @return this schema
         * @throws ParamException if the sub-field data type is not supported
         */
        public StructFieldSchema addField(AddFieldReq addFieldReq) {
            if (addFieldReq.getDataType() == DataType.Array || addFieldReq.getElementType() == DataType.Struct) {
                throw new ParamException("Struct field schema does not support Array, ArrayOfVector or Struct");
            }
            fields.add(SchemaUtils.convertFieldReqToFieldSchema(addFieldReq));
            return this;
        }

        /**
         * Returns the data type of a struct field, which is always {@link DataType#Array}.
         *
         * @return {@link DataType#Array}
         */
        public DataType getDataType() {
            return DataType.Array;
        }

        /**
         * Returns the element type of a struct field, which is always {@link DataType#Struct}.
         *
         * @return {@link DataType#Struct}
         */
        public DataType getElementType() {
            return DataType.Struct;
        }

        // Getters and Setters
        /**
         * Returns the name of the struct field.
         *
         * @return the struct field name
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the name of the struct field.
         *
         * @param name the struct field name
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Returns the description of the struct field.
         *
         * @return the struct field description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Sets the description of the struct field.
         *
         * @param description the struct field description
         */
        public void setDescription(String description) {
            this.description = description;
        }

        /**
         * Returns the sub-fields of the struct field.
         *
         * @return the sub-fields
         */
        public List<CreateCollectionReq.FieldSchema> getFields() {
            return fields;
        }

        /**
         * Sets the sub-fields of the struct field.
         *
         * @param fields the sub-fields
         */
        public void setFields(List<CreateCollectionReq.FieldSchema> fields) {
            this.fields = fields;
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
            this.nullable = Boolean.TRUE.equals(nullable);
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

        @Override
        public String toString() {
            return "StructFieldSchema{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", fields=" + fields +
                    ", maxCapacity=" + maxCapacity +
                    ", nullable=" + nullable +
                    ", typeParams=" + typeParams +
                    '}';
        }

        /**
         * Creates a new builder for {@link StructFieldSchema}.
         *
         * @return the builder
         */
        public static StructFieldSchemaBuilder builder() {
            return new StructFieldSchemaBuilder();
        }

        public static class StructFieldSchemaBuilder {
            private String name;
            private String description = "";
            private List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>();
            private Integer maxCapacity;
            private Boolean nullable = Boolean.FALSE;
            private Map<String, String> typeParams = new HashMap<>();

            private StructFieldSchemaBuilder() {
            }

            /**
             * Sets the name of the struct field.
             *
             * @param name the struct field name
             * @return this builder
             */
            public StructFieldSchemaBuilder name(String name) {
                this.name = name;
                return this;
            }

            /**
             * Sets the description of the struct field.
             *
             * @param description the struct field description
             * @return this builder
             */
            public StructFieldSchemaBuilder description(String description) {
                this.description = description;
                return this;
            }

            /**
             * Sets the sub-fields of the struct field.
             *
             * @param fields the sub-fields
             * @return this builder
             */
            public StructFieldSchemaBuilder fields(List<CreateCollectionReq.FieldSchema> fields) {
                this.fields = fields;
                return this;
            }

            /**
             * Sets the maximum number of elements the struct field can hold.
             *
             * @param maxCapacity the max capacity
             * @return this builder
             */
            public StructFieldSchemaBuilder maxCapacity(Integer maxCapacity) {
                this.maxCapacity = maxCapacity;
                return this;
            }

            /**
             * Sets whether the struct field is nullable.
             *
             * @param nullable {@code true} to make the field nullable
             * @return this builder
             */
            public StructFieldSchemaBuilder nullable(Boolean nullable) {
                this.nullable = Boolean.TRUE.equals(nullable);
                return this;
            }

            /**
             * Sets the type parameters of the struct field.
             *
             * @param typeParams the type parameters
             * @return this builder
             */
            public StructFieldSchemaBuilder typeParams(Map<String, String> typeParams) {
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
            public StructFieldSchemaBuilder typeParam(String key, String value) {
                if (this.typeParams == null) {
                    this.typeParams = new HashMap<>();
                }
                this.typeParams.put(key, value);
                return this;
            }

            /**
             * Builds a {@link StructFieldSchema} with the configured parameters.
             *
             * @return the struct field schema
             */
            public StructFieldSchema build() {
                return new StructFieldSchema(this);
            }
        }
    }
}

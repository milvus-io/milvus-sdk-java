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

package io.milvus.v2.service.collection.response;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response of the {@code describeCollection} API, holding the details of a collection.
 */
public class DescribeCollectionResp {
    private String collectionName;
    private Long collectionID;
    private String databaseName;
    private String description;
    private Long numOfPartitions;
    private List<String> fieldNames;
    private List<String> vectorFieldNames;
    private String primaryFieldName;
    private Boolean enableDynamicField;
    private Boolean autoID;
    private CreateCollectionReq.CollectionSchema collectionSchema;
    private Long createTime;
    private Long createUtcTime;
    private ConsistencyLevel consistencyLevel;
    private Integer shardsNum;
    private final Map<String, String> properties;
    private List<String> aliases = new ArrayList<>();
    private Long updateTimestamp;
    private Boolean enableNamespace;
    private Integer schemaVersion;

    private DescribeCollectionResp(DescribeCollectionRespBuilder builder) {
        this.collectionName = builder.collectionName;
        this.collectionID = builder.collectionID;
        this.databaseName = builder.databaseName;
        this.description = builder.description;
        this.numOfPartitions = builder.numOfPartitions;
        this.fieldNames = builder.fieldNames != null ? builder.fieldNames : new ArrayList<>();
        this.vectorFieldNames = builder.vectorFieldNames != null ? builder.vectorFieldNames : new ArrayList<>();
        this.primaryFieldName = builder.primaryFieldName;
        this.enableDynamicField = builder.enableDynamicField;
        this.autoID = builder.autoID;
        this.collectionSchema = builder.collectionSchema;
        this.createTime = builder.createTime;
        this.createUtcTime = builder.createUtcTime;
        this.consistencyLevel = builder.consistencyLevel;
        this.shardsNum = builder.shardsNum;
        this.properties = builder.properties != null ? builder.properties : new HashMap<>();
        this.aliases = builder.aliases != null ? builder.aliases : new ArrayList<>();
        this.updateTimestamp = builder.updateTimestamp;
        this.enableNamespace = builder.enableNamespace;
        this.schemaVersion = builder.schemaVersion;
    }

    /**
     * Creates a new builder for {@link DescribeCollectionResp}.
     *
     * @return the builder
     */
    public static DescribeCollectionRespBuilder builder() {
        return new DescribeCollectionRespBuilder();
    }

    // Getters
    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns the collection ID.
     *
     * @return the collection ID
     */
    public Long getCollectionID() {
        return collectionID;
    }

    /**
     * Returns the database name of the collection.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
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
     * Returns the number of partitions of the collection.
     *
     * @return the number of partitions
     */
    public Long getNumOfPartitions() {
        return numOfPartitions;
    }

    /**
     * Returns the names of all fields of the collection.
     *
     * @return the field names
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * Returns the names of the vector fields of the collection.
     *
     * @return the vector field names
     */
    public List<String> getVectorFieldNames() {
        return vectorFieldNames;
    }

    /**
     * Returns the name of the primary key field of the collection.
     *
     * @return the primary field name
     */
    public String getPrimaryFieldName() {
        return primaryFieldName;
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
     * Returns whether auto-generated IDs are enabled for the collection.
     *
     * @return {@code true} if auto ID is enabled
     */
    public Boolean getAutoID() {
        return autoID;
    }

    /**
     * Returns the schema of the collection.
     *
     * @return the collection schema
     */
    public CreateCollectionReq.CollectionSchema getCollectionSchema() {
        return collectionSchema;
    }

    /**
     * Returns the creation time of the collection.
     *
     * @return the creation time
     */
    public Long getCreateTime() {
        return createTime;
    }

    /**
     * Returns the UTC creation time of the collection.
     *
     * @return the UTC creation time
     */
    public Long getCreateUtcTime() {
        return createUtcTime;
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
     * Returns the number of shards of the collection.
     *
     * @return the number of shards
     */
    public Integer getShardsNum() {
        return shardsNum;
    }

    /**
     * Returns the properties of the collection.
     *
     * @return the collection properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns the aliases of the collection.
     *
     * @return the collection aliases
     */
    public List<String> getAliases() {
        return aliases;
    }

    /**
     * Returns the timestamp of the last update of the collection.
     *
     * @return the update timestamp
     */
    public Long getUpdateTimestamp() {
        return updateTimestamp;
    }

    /**
     * Returns whether the namespace feature is enabled for the collection.
     *
     * @return {@code true} if the namespace is enabled
     */
    public Boolean getEnableNamespace() {
        return enableNamespace;
    }

    /**
     * Returns the schema version of the collection.
     *
     * @return the schema version
     */
    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    // Setters
    /**
     * Sets the aliases of the collection.
     *
     * @param aliases the collection aliases
     */
    public void setAliases(List<String> aliases) {
        this.aliases = aliases;
    }

    /**
     * Sets the timestamp of the last update of the collection.
     *
     * @param updateTimestamp the update timestamp
     */
    public void setUpdateTimestamp(Long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    /**
     * Sets whether the namespace feature is enabled for the collection.
     *
     * @param enableNamespace {@code true} if the namespace is enabled
     */
    public void setEnableNamespace(Boolean enableNamespace) {
        this.enableNamespace = enableNamespace;
    }

    /**
     * Sets the schema version of the collection.
     *
     * @param schemaVersion the schema version
     */
    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
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
     * Sets the collection ID.
     *
     * @param collectionID the collection ID
     */
    public void setCollectionID(Long collectionID) {
        this.collectionID = collectionID;
    }

    /**
     * Sets the database name of the collection.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
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
     * Sets the number of partitions of the collection.
     *
     * @param numOfPartitions the number of partitions
     */
    public void setNumOfPartitions(Long numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    /**
     * Sets the names of all fields of the collection.
     *
     * @param fieldNames the field names
     */
    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Sets the names of the vector fields of the collection.
     *
     * @param vectorFieldNames the vector field names
     */
    public void setVectorFieldNames(List<String> vectorFieldNames) {
        this.vectorFieldNames = vectorFieldNames;
    }

    /**
     * Sets the name of the primary key field of the collection.
     *
     * @param primaryFieldName the primary field name
     */
    public void setPrimaryFieldName(String primaryFieldName) {
        this.primaryFieldName = primaryFieldName;
    }

    /**
     * Sets whether the dynamic field is enabled for the collection.
     *
     * @param enableDynamicField {@code true} if the dynamic field is enabled
     */
    public void setEnableDynamicField(Boolean enableDynamicField) {
        this.enableDynamicField = enableDynamicField;
    }

    /**
     * Sets whether auto-generated IDs are enabled for the collection.
     *
     * @param autoID {@code true} if auto ID is enabled
     */
    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    /**
     * Sets the schema of the collection.
     *
     * @param collectionSchema the collection schema
     */
    public void setCollectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
        this.collectionSchema = collectionSchema;
    }

    /**
     * Sets the creation time of the collection.
     *
     * @param createTime the creation time
     */
    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Sets the UTC creation time of the collection.
     *
     * @param createUtcTime the UTC creation time
     */
    public void setCreateUtcTime(Long createUtcTime) {
        this.createUtcTime = createUtcTime;
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
     * Sets the number of shards of the collection.
     *
     * @param shardsNum the number of shards
     */
    public void setShardsNum(Integer shardsNum) {
        this.shardsNum = shardsNum;
    }

    @Override
    public String toString() {
        return "DescribeCollectionResp{" +
                "collectionName='" + collectionName + '\'' +
                ", collectionID=" + collectionID +
                ", databaseName='" + databaseName + '\'' +
                ", description='" + description + '\'' +
                ", numOfPartitions=" + numOfPartitions +
                ", fieldNames=" + fieldNames +
                ", vectorFieldNames=" + vectorFieldNames +
                ", primaryFieldName='" + primaryFieldName + '\'' +
                ", enableDynamicField=" + enableDynamicField +
                ", autoID=" + autoID +
                ", collectionSchema=" + collectionSchema +
                ", createTime=" + createTime +
                ", createUtcTime=" + createUtcTime +
                ", consistencyLevel=" + consistencyLevel +
                ", shardsNum=" + shardsNum +
                ", properties=" + properties +
                ", aliases=" + aliases +
                ", updateTimestamp=" + updateTimestamp +
                ", enableNamespace=" + enableNamespace +
                ", schemaVersion=" + schemaVersion +
                '}';
    }

    public static class DescribeCollectionRespBuilder {
        private String collectionName;
        private Long collectionID;
        private String databaseName;
        private String description;
        private Long numOfPartitions;
        private List<String> fieldNames;
        private List<String> vectorFieldNames;
        private String primaryFieldName;
        private Boolean enableDynamicField;
        private Boolean autoID;
        private CreateCollectionReq.CollectionSchema collectionSchema;
        private Long createTime;
        private Long createUtcTime;
        private ConsistencyLevel consistencyLevel;
        private Integer shardsNum;
        private Map<String, String> properties;
        private List<String> aliases;
        private Long updateTimestamp;
        private Boolean enableNamespace;
        private Integer schemaVersion;

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DescribeCollectionRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the collection ID.
         *
         * @param collectionID the collection ID
         * @return this builder
         */
        public DescribeCollectionRespBuilder collectionID(Long collectionID) {
            this.collectionID = collectionID;
            return this;
        }

        /**
         * Sets the database name of the collection.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public DescribeCollectionRespBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the description of the collection.
         *
         * @param description the collection description
         * @return this builder
         */
        public DescribeCollectionRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the number of partitions of the collection.
         *
         * @param numOfPartitions the number of partitions
         * @return this builder
         */
        public DescribeCollectionRespBuilder numOfPartitions(Long numOfPartitions) {
            this.numOfPartitions = numOfPartitions;
            return this;
        }

        /**
         * Sets the names of all fields of the collection.
         *
         * @param fieldNames the field names
         * @return this builder
         */
        public DescribeCollectionRespBuilder fieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /**
         * Sets the names of the vector fields of the collection.
         *
         * @param vectorFieldNames the vector field names
         * @return this builder
         */
        public DescribeCollectionRespBuilder vectorFieldNames(List<String> vectorFieldNames) {
            this.vectorFieldNames = vectorFieldNames;
            return this;
        }

        /**
         * Sets the name of the primary key field of the collection.
         *
         * @param primaryFieldName the primary field name
         * @return this builder
         */
        public DescribeCollectionRespBuilder primaryFieldName(String primaryFieldName) {
            this.primaryFieldName = primaryFieldName;
            return this;
        }

        /**
         * Sets whether the dynamic field is enabled for the collection.
         *
         * @param enableDynamicField {@code true} if the dynamic field is enabled
         * @return this builder
         */
        public DescribeCollectionRespBuilder enableDynamicField(Boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
            return this;
        }

        /**
         * Sets whether auto-generated IDs are enabled for the collection.
         *
         * @param autoID {@code true} if auto ID is enabled
         * @return this builder
         */
        public DescribeCollectionRespBuilder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        /**
         * Sets the schema of the collection.
         *
         * @param collectionSchema the collection schema
         * @return this builder
         */
        public DescribeCollectionRespBuilder collectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
            this.collectionSchema = collectionSchema;
            return this;
        }

        /**
         * Sets the creation time of the collection.
         *
         * @param createTime the creation time
         * @return this builder
         */
        public DescribeCollectionRespBuilder createTime(Long createTime) {
            this.createTime = createTime;
            return this;
        }

        /**
         * Sets the UTC creation time of the collection.
         *
         * @param createUtcTime the UTC creation time
         * @return this builder
         */
        public DescribeCollectionRespBuilder createUtcTime(Long createUtcTime) {
            this.createUtcTime = createUtcTime;
            return this;
        }

        /**
         * Sets the consistency level of the collection.
         *
         * @param consistencyLevel the consistency level
         * @return this builder
         */
        public DescribeCollectionRespBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Sets the number of shards of the collection.
         *
         * @param shardsNum the number of shards
         * @return this builder
         */
        public DescribeCollectionRespBuilder shardsNum(Integer shardsNum) {
            this.shardsNum = shardsNum;
            return this;
        }

        /**
         * Sets the properties of the collection.
         *
         * @param properties the collection properties
         * @return this builder
         */
        public DescribeCollectionRespBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Sets the aliases of the collection.
         *
         * @param aliases the collection aliases
         * @return this builder
         */
        public DescribeCollectionRespBuilder aliases(List<String> aliases) {
            this.aliases = aliases;
            return this;
        }

        /**
         * Sets the timestamp of the last update of the collection.
         *
         * @param updateTimestamp the update timestamp
         * @return this builder
         */
        public DescribeCollectionRespBuilder updateTimestamp(Long updateTimestamp) {
            this.updateTimestamp = updateTimestamp;
            return this;
        }

        /**
         * Sets whether the namespace feature is enabled for the collection.
         *
         * @param enableNamespace {@code true} if the namespace is enabled
         * @return this builder
         */
        public DescribeCollectionRespBuilder enableNamespace(Boolean enableNamespace) {
            this.enableNamespace = enableNamespace;
            return this;
        }

        /**
         * Sets the schema version of the collection.
         *
         * @param schemaVersion the schema version
         * @return this builder
         */
        public DescribeCollectionRespBuilder schemaVersion(Integer schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        /**
         * Builds a {@link DescribeCollectionResp} with the configured parameters.
         *
         * @return the response
         */
        public DescribeCollectionResp build() {
            return new DescribeCollectionResp(this);
        }
    }
}

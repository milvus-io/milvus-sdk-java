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
    }

    public static DescribeCollectionRespBuilder builder() {
        return new DescribeCollectionRespBuilder();
    }

    // Getters
    public String getCollectionName() {
        return collectionName;
    }

    public Long getCollectionID() {
        return collectionID;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDescription() {
        return description;
    }

    public Long getNumOfPartitions() {
        return numOfPartitions;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<String> getVectorFieldNames() {
        return vectorFieldNames;
    }

    public String getPrimaryFieldName() {
        return primaryFieldName;
    }

    public Boolean getEnableDynamicField() {
        return enableDynamicField;
    }

    public Boolean getAutoID() {
        return autoID;
    }

    public CreateCollectionReq.CollectionSchema getCollectionSchema() {
        return collectionSchema;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public Long getCreateUtcTime() {
        return createUtcTime;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public Integer getShardsNum() {
        return shardsNum;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // Setters
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setCollectionID(Long collectionID) {
        this.collectionID = collectionID;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setNumOfPartitions(Long numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public void setVectorFieldNames(List<String> vectorFieldNames) {
        this.vectorFieldNames = vectorFieldNames;
    }

    public void setPrimaryFieldName(String primaryFieldName) {
        this.primaryFieldName = primaryFieldName;
    }

    public void setEnableDynamicField(Boolean enableDynamicField) {
        this.enableDynamicField = enableDynamicField;
    }

    public void setAutoID(Boolean autoID) {
        this.autoID = autoID;
    }

    public void setCollectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
        this.collectionSchema = collectionSchema;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public void setCreateUtcTime(Long createUtcTime) {
        this.createUtcTime = createUtcTime;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

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

        public DescribeCollectionRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DescribeCollectionRespBuilder collectionID(Long collectionID) {
            this.collectionID = collectionID;
            return this;
        }

        public DescribeCollectionRespBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public DescribeCollectionRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        public DescribeCollectionRespBuilder numOfPartitions(Long numOfPartitions) {
            this.numOfPartitions = numOfPartitions;
            return this;
        }

        public DescribeCollectionRespBuilder fieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public DescribeCollectionRespBuilder vectorFieldNames(List<String> vectorFieldNames) {
            this.vectorFieldNames = vectorFieldNames;
            return this;
        }

        public DescribeCollectionRespBuilder primaryFieldName(String primaryFieldName) {
            this.primaryFieldName = primaryFieldName;
            return this;
        }

        public DescribeCollectionRespBuilder enableDynamicField(Boolean enableDynamicField) {
            this.enableDynamicField = enableDynamicField;
            return this;
        }

        public DescribeCollectionRespBuilder autoID(Boolean autoID) {
            this.autoID = autoID;
            return this;
        }

        public DescribeCollectionRespBuilder collectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
            this.collectionSchema = collectionSchema;
            return this;
        }

        public DescribeCollectionRespBuilder createTime(Long createTime) {
            this.createTime = createTime;
            return this;
        }

        public DescribeCollectionRespBuilder createUtcTime(Long createUtcTime) {
            this.createUtcTime = createUtcTime;
            return this;
        }

        public DescribeCollectionRespBuilder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public DescribeCollectionRespBuilder shardsNum(Integer shardsNum) {
            this.shardsNum = shardsNum;
            return this;
        }

        public DescribeCollectionRespBuilder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public DescribeCollectionResp build() {
            return new DescribeCollectionResp(this);
        }
    }
}

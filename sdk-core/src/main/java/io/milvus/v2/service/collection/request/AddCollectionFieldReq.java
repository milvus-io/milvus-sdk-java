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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AddCollectionFieldReq extends AddFieldReq {
    private String collectionName;
    private String databaseName;

    private AddCollectionFieldReq(Builder builder) {
        super(builder);
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        AddCollectionFieldReq that = (AddCollectionFieldReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(databaseName, that.databaseName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(collectionName)
                .append(databaseName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "AddCollectionFieldReq{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", " + super.toString() +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AddFieldReq.Builder {
        private String collectionName;
        private String databaseName;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        // Override all parent builder methods to return the correct type
        @Override
        public Builder fieldName(String fieldName) {
            super.fieldName(fieldName);
            return this;
        }

        @Override
        public Builder description(String description) {
            super.description(description);
            return this;
        }

        @Override
        public Builder dataType(io.milvus.v2.common.DataType dataType) {
            super.dataType(dataType);
            return this;
        }

        @Override
        public Builder maxLength(Integer maxLength) {
            super.maxLength(maxLength);
            return this;
        }

        @Override
        public Builder isPrimaryKey(Boolean isPrimaryKey) {
            super.isPrimaryKey(isPrimaryKey);
            return this;
        }

        @Override
        public Builder isPartitionKey(Boolean isPartitionKey) {
            super.isPartitionKey(isPartitionKey);
            return this;
        }

        @Override
        public Builder isClusteringKey(Boolean isClusteringKey) {
            super.isClusteringKey(isClusteringKey);
            return this;
        }

        @Override
        public Builder autoID(Boolean autoID) {
            super.autoID(autoID);
            return this;
        }

        @Override
        public Builder dimension(Integer dimension) {
            super.dimension(dimension);
            return this;
        }

        @Override
        public Builder elementType(io.milvus.v2.common.DataType elementType) {
            super.elementType(elementType);
            return this;
        }

        @Override
        public Builder maxCapacity(Integer maxCapacity) {
            super.maxCapacity(maxCapacity);
            return this;
        }

        @Override
        public Builder isNullable(Boolean isNullable) {
            super.isNullable(isNullable);
            return this;
        }

        @Override
        public Builder defaultValue(Object defaultValue) {
            super.defaultValue(defaultValue);
            return this;
        }

        @Override
        public Builder enableDefaultValue(boolean enableDefaultValue) {
            super.enableDefaultValue(enableDefaultValue);
            return this;
        }

        @Override
        public Builder enableAnalyzer(Boolean enableAnalyzer) {
            super.enableAnalyzer(enableAnalyzer);
            return this;
        }

        @Override
        public Builder analyzerParams(java.util.Map<String, Object> analyzerParams) {
            super.analyzerParams(analyzerParams);
            return this;
        }

        @Override
        public Builder enableMatch(Boolean enableMatch) {
            super.enableMatch(enableMatch);
            return this;
        }

        @Override
        public Builder typeParams(java.util.Map<String, String> typeParams) {
            super.typeParams(typeParams);
            return this;
        }

        @Override
        public Builder multiAnalyzerParams(java.util.Map<String, Object> multiAnalyzerParams) {
            super.multiAnalyzerParams(multiAnalyzerParams);
            return this;
        }

        @Override
        public AddCollectionFieldReq build() {
            return new AddCollectionFieldReq(this);
        }
    }
}

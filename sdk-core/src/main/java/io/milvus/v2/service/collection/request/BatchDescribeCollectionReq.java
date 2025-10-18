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

import java.util.List;

public class BatchDescribeCollectionReq {
    private String databaseName;
    private List<String> collectionNames;

    // Private constructor for builder
    private BatchDescribeCollectionReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionNames = builder.collectionNames;
    }

    // Static method to create builder
    public static Builder builder() {
        return new Builder();
    }

    // Getter methods
    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getCollectionNames() {
        return collectionNames;
    }

    // Setter methods
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchDescribeCollectionReq that = (BatchDescribeCollectionReq) o;

        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(collectionNames, that.collectionNames)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionNames)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "BatchDescribeCollectionReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionNames=" + collectionNames +
                '}';
    }

    // Builder class
    public static class Builder {
        private String databaseName;
        private List<String> collectionNames;

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public BatchDescribeCollectionReq build() {
            return new BatchDescribeCollectionReq(this);
        }
    }
}

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

public class GetCollectionStatsReq {
    private String databaseName;
    private String collectionName;

    private GetCollectionStatsReq(Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GetCollectionStatsReq that = (GetCollectionStatsReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "GetCollectionStatsReq{" +
                "collectionName='" + collectionName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetCollectionStatsReq build() {
            return new GetCollectionStatsReq(this);
        }
    }
}

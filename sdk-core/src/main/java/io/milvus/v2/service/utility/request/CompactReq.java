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

package io.milvus.v2.service.utility.request;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class CompactReq {
    private String databaseName;
    private String collectionName;
    private Boolean isClustering = Boolean.FALSE;

    private CompactReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.isClustering = builder.isClustering;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Boolean getIsClustering() {
        return isClustering;
    }

    public void setIsClustering(Boolean isClustering) {
        this.isClustering = isClustering;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CompactReq that = (CompactReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(isClustering, that.isClustering)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(isClustering)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CompactReq{" +
                "collectionName='" + collectionName + '\'' +
                ", isClustering=" + isClustering +
                '}';
    }

    public static class Builder {
        private String collectionName;
        private Boolean isClustering = Boolean.FALSE;

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder isClustering(Boolean isClustering) {
            this.isClustering = isClustering;
            return this;
        }

        public CompactReq build() {
            return new CompactReq(this);
        }
    }
}

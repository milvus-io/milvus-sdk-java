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

public class ReleaseCollectionReq {
    private String collectionName;
    @Deprecated
    private Boolean async = Boolean.TRUE;
    private Long timeout = 60000L;

    private ReleaseCollectionReq(Builder builder) {
        this.collectionName = builder.collectionName;
        this.async = builder.async;
        this.timeout = builder.timeout;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Deprecated
    public Boolean getAsync() {
        return async;
    }

    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReleaseCollectionReq that = (ReleaseCollectionReq) obj;
        return new EqualsBuilder()
                .append(collectionName, that.collectionName)
                .append(async, that.async)
                .append(timeout, that.timeout)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(collectionName)
                .append(async)
                .append(timeout)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "ReleaseCollectionReq{" +
                "collectionName='" + collectionName + '\'' +
                ", async=" + async +
                ", timeout=" + timeout +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String collectionName;
        private Boolean async = Boolean.TRUE;
        private Long timeout = 60000L;

        private Builder() {}

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        @Deprecated
        public Builder async(Boolean async) {
            this.async = async;
            return this;
        }

        public Builder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public ReleaseCollectionReq build() {
            return new ReleaseCollectionReq(this);
        }
    }
}

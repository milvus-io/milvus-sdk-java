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

package io.milvus.v2.service.vector.response;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

public class UpsertResp {
    private long upsertCnt;

    // From v2.4.10, milvus allows upsert for auto-id=true, the server will return a new pk.
    // the new pk is not equal to the original pk, the original entity is deleted, and a new entity
    // is created with this new pk. Here we return this new pk to user.
    private List<Object> primaryKeys;

    private UpsertResp(Builder builder) {
        this.upsertCnt = builder.upsertCnt;
        this.primaryKeys = builder.primaryKeys;
    }

    // Getters and Setters
    public long getUpsertCnt() {
        return upsertCnt;
    }

    public void setUpsertCnt(long upsertCnt) {
        this.upsertCnt = upsertCnt;
    }

    public List<Object> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<Object> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UpsertResp that = (UpsertResp) obj;
        return new EqualsBuilder()
                .append(upsertCnt, that.upsertCnt)
                .append(primaryKeys, that.primaryKeys)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = (int) (upsertCnt ^ (upsertCnt >>> 32));
        result = 31 * result + (primaryKeys != null ? primaryKeys.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UpsertResp{" +
                "upsertCnt=" + upsertCnt +
                ", primaryKeys=" + primaryKeys +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long upsertCnt;
        private List<Object> primaryKeys = new ArrayList<>(); // default value

        private Builder() {}

        public Builder upsertCnt(long upsertCnt) {
            this.upsertCnt = upsertCnt;
            return this;
        }

        public Builder primaryKeys(List<Object> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public UpsertResp build() {
            return new UpsertResp(this);
        }
    }
}

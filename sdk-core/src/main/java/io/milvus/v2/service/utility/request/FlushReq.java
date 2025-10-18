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

import java.util.ArrayList;
import java.util.List;

public class FlushReq {
    private String databaseName;
    private List<String> collectionNames;
    private Long waitFlushedTimeoutMs; // 0 - waiting util flush task is done

    private FlushReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionNames = builder.collectionNames;
        this.waitFlushedTimeoutMs = builder.waitFlushedTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<String> getCollectionNames() {
        return collectionNames;
    }

    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    public Long getWaitFlushedTimeoutMs() {
        return waitFlushedTimeoutMs;
    }

    public void setWaitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
        this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FlushReq that = (FlushReq) obj;
        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(collectionNames, that.collectionNames)
                .append(waitFlushedTimeoutMs, that.waitFlushedTimeoutMs)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(collectionNames)
                .append(waitFlushedTimeoutMs)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "FlushReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionNames=" + collectionNames +
                ", waitFlushedTimeoutMs=" + waitFlushedTimeoutMs +
                '}';
    }

    public static class Builder {
        private String databaseName;
        private List<String> collectionNames = new ArrayList<>();
        private Long waitFlushedTimeoutMs = 0L; // 0 - waiting util flush task is done

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        public Builder waitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
            this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
            return this;
        }

        public FlushReq build() {
            return new FlushReq(this);
        }
    }
}

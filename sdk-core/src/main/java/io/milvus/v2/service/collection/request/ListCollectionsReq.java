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

import java.util.Objects;

public class ListCollectionsReq {
    private String databaseName;

    private ListCollectionsReq(Builder builder) {
        this.databaseName = builder.databaseName;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getDatabaseName() {
        return databaseName;
    }

    // Setters
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ListCollectionsReq that = (ListCollectionsReq) obj;

        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName);
    }

    @Override
    public String toString() {
        return "ListCollectionsReq{" +
                "databaseName='" + databaseName +
                '}';
    }

    public static class Builder {
        private String databaseName;

        private Builder() {}

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public ListCollectionsReq build() {
            return new ListCollectionsReq(this);
        }
    }
}

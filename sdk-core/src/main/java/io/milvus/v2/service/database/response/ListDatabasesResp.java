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

package io.milvus.v2.service.database.response;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

public class ListDatabasesResp {
    private List<String> databaseNames;

    private ListDatabasesResp(Builder builder) {
        this.databaseNames = builder.databaseNames;
    }

    public List<String> getDatabaseNames() {
        return databaseNames;
    }

    public void setDatabaseNames(List<String> databaseNames) {
        this.databaseNames = databaseNames;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ListDatabasesResp that = (ListDatabasesResp) obj;
        return new EqualsBuilder()
                .append(databaseNames, that.databaseNames)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return databaseNames != null ? databaseNames.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ListDatabasesResp{" +
                "databaseNames=" + databaseNames +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> databaseNames = new ArrayList<>();

        private Builder() {}

        public Builder databaseNames(List<String> databaseNames) {
            this.databaseNames = databaseNames;
            return this;
        }

        public ListDatabasesResp build() {
            return new ListDatabasesResp(this);
        }
    }
}

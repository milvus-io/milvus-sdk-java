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

package io.milvus.v2.service.database.request;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class AlterDatabaseReq {
    private String databaseName;
    private Map<String, String> properties = new HashMap<>();

    private AlterDatabaseReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.properties = builder.properties;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AlterDatabaseReq that = (AlterDatabaseReq) obj;
        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(properties, that.properties)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = databaseName != null ? databaseName.hashCode() : 0;
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AlterDatabaseReq{" +
                "databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseName;
        private Map<String, String> properties = new HashMap<>();

        private Builder() {}

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public AlterDatabaseReq build() {
            return new AlterDatabaseReq(this);
        }
    }
}

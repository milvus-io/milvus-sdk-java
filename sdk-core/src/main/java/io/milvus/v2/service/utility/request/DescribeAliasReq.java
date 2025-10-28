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

public class DescribeAliasReq {
    private String databaseName;
    private String alias;

    private DescribeAliasReq(Builder builder) {
        this.databaseName = builder.databaseName;
        this.alias = builder.alias;
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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeAliasReq that = (DescribeAliasReq) obj;
        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(alias, that.alias)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(databaseName)
                .append(alias)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "DescribeAliasReq{" +
                "databaseName='" + databaseName + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }

    public static class Builder {
        private String databaseName;
        private String alias;

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder alias(String alias) {
            this.alias = alias;
            return this;
        }

        public DescribeAliasReq build() {
            return new DescribeAliasReq(this);
        }
    }
}

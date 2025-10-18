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

package io.milvus.v2.service.rbac.request;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class DescribeRoleReq {
    private String roleName;
    private String dbName;

    private DescribeRoleReq(Builder builder) {
        this.roleName = builder.roleName;
        this.dbName = builder.dbName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeRoleReq that = (DescribeRoleReq) obj;
        return new EqualsBuilder()
                .append(roleName, that.roleName)
                .append(dbName, that.dbName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = roleName != null ? roleName.hashCode() : 0;
        result = 31 * result + (dbName != null ? dbName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DescribeRoleReq{" +
                "roleName='" + roleName + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String roleName;
        private String dbName;

        private Builder() {}

        public Builder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public DescribeRoleReq build() {
            return new DescribeRoleReq(this);
        }
    }
}

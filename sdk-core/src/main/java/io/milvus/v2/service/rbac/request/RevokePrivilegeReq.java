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

public class RevokePrivilegeReq {
    private String roleName;
    private String dbName;
    private String objectType;
    private String privilege;
    private String objectName;

    private RevokePrivilegeReq(Builder builder) {
        this.roleName = builder.roleName;
        this.dbName = builder.dbName;
        this.objectType = builder.objectType;
        this.privilege = builder.privilege;
        this.objectName = builder.objectName;
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

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getPrivilege() {
        return privilege;
    }

    public void setPrivilege(String privilege) {
        this.privilege = privilege;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RevokePrivilegeReq that = (RevokePrivilegeReq) obj;
        return new EqualsBuilder()
                .append(roleName, that.roleName)
                .append(dbName, that.dbName)
                .append(objectType, that.objectType)
                .append(privilege, that.privilege)
                .append(objectName, that.objectName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = roleName != null ? roleName.hashCode() : 0;
        result = 31 * result + (dbName != null ? dbName.hashCode() : 0);
        result = 31 * result + (objectType != null ? objectType.hashCode() : 0);
        result = 31 * result + (privilege != null ? privilege.hashCode() : 0);
        result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RevokePrivilegeReq{" +
                "roleName='" + roleName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", objectType='" + objectType + '\'' +
                ", privilege='" + privilege + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String roleName;
        private String dbName;
        private String objectType;
        private String privilege;
        private String objectName;

        private Builder() {}

        public Builder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder objectType(String objectType) {
            this.objectType = objectType;
            return this;
        }

        public Builder privilege(String privilege) {
            this.privilege = privilege;
            return this;
        }

        public Builder objectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public RevokePrivilegeReq build() {
            return new RevokePrivilegeReq(this);
        }
    }
}

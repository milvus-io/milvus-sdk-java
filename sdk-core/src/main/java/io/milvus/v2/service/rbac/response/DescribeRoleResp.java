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

package io.milvus.v2.service.rbac.response;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

public class DescribeRoleResp {
    private List<GrantInfo> grantInfos;

    private DescribeRoleResp(Builder builder) {
        this.grantInfos = builder.grantInfos;
    }

    public List<GrantInfo> getGrantInfos() {
        return grantInfos;
    }

    public void setGrantInfos(List<GrantInfo> grantInfos) {
        this.grantInfos = grantInfos;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeRoleResp that = (DescribeRoleResp) obj;
        return new EqualsBuilder()
                .append(grantInfos, that.grantInfos)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return grantInfos != null ? grantInfos.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DescribeRoleResp{" +
                "grantInfos=" + grantInfos +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<GrantInfo> grantInfos = new ArrayList<>();

        private Builder() {}

        public Builder grantInfos(List<GrantInfo> grantInfos) {
            this.grantInfos = grantInfos;
            return this;
        }

        public DescribeRoleResp build() {
            return new DescribeRoleResp(this);
        }
    }

    public static class GrantInfo {
        private String objectType;
        private String objectName;
        private String roleName;
        private String grantor;
        private String privilege;
        private String dbName;

        private GrantInfo(Builder builder) {
            this.objectType = builder.objectType;
            this.objectName = builder.objectName;
            this.roleName = builder.roleName;
            this.grantor = builder.grantor;
            this.privilege = builder.privilege;
            this.dbName = builder.dbName;
        }

        public String getObjectType() {
            return objectType;
        }

        public void setObjectType(String objectType) {
            this.objectType = objectType;
        }

        public String getObjectName() {
            return objectName;
        }

        public void setObjectName(String objectName) {
            this.objectName = objectName;
        }

        public String getRoleName() {
            return roleName;
        }

        public void setRoleName(String roleName) {
            this.roleName = roleName;
        }

        public String getGrantor() {
            return grantor;
        }

        public void setGrantor(String grantor) {
            this.grantor = grantor;
        }

        public String getPrivilege() {
            return privilege;
        }

        public void setPrivilege(String privilege) {
            this.privilege = privilege;
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
            GrantInfo grantInfo = (GrantInfo) obj;
            return new EqualsBuilder()
                    .append(objectType, grantInfo.objectType)
                    .append(objectName, grantInfo.objectName)
                    .append(roleName, grantInfo.roleName)
                    .append(grantor, grantInfo.grantor)
                    .append(privilege, grantInfo.privilege)
                    .append(dbName, grantInfo.dbName)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            int result = objectType != null ? objectType.hashCode() : 0;
            result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
            result = 31 * result + (roleName != null ? roleName.hashCode() : 0);
            result = 31 * result + (grantor != null ? grantor.hashCode() : 0);
            result = 31 * result + (privilege != null ? privilege.hashCode() : 0);
            result = 31 * result + (dbName != null ? dbName.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "GrantInfo{" +
                    "objectType='" + objectType + '\'' +
                    ", objectName='" + objectName + '\'' +
                    ", roleName='" + roleName + '\'' +
                    ", grantor='" + grantor + '\'' +
                    ", privilege='" + privilege + '\'' +
                    ", dbName='" + dbName + '\'' +
                    '}';
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String objectType;
            private String objectName;
            private String roleName;
            private String grantor;
            private String privilege;
            private String dbName;

            private Builder() {}

            public Builder objectType(String objectType) {
                this.objectType = objectType;
                return this;
            }

            public Builder objectName(String objectName) {
                this.objectName = objectName;
                return this;
            }

            public Builder roleName(String roleName) {
                this.roleName = roleName;
                return this;
            }

            public Builder grantor(String grantor) {
                this.grantor = grantor;
                return this;
            }

            public Builder privilege(String privilege) {
                this.privilege = privilege;
                return this;
            }

            public Builder dbName(String dbName) {
                this.dbName = dbName;
                return this;
            }

            public GrantInfo build() {
                return new GrantInfo(this);
            }
        }
    }
}

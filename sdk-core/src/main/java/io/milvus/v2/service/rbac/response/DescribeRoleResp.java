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

import java.util.ArrayList;
import java.util.List;

public class DescribeRoleResp {
    private List<GrantInfo> grantInfos;

    private DescribeRoleResp(DescribeRoleRespBuilder builder) {
        this.grantInfos = builder.grantInfos;
    }

    public List<GrantInfo> getGrantInfos() {
        return grantInfos;
    }

    public void setGrantInfos(List<GrantInfo> grantInfos) {
        this.grantInfos = grantInfos;
    }

    @Override
    public String toString() {
        return "DescribeRoleResp{" +
                "grantInfos=" + grantInfos +
                '}';
    }

    public static DescribeRoleRespBuilder builder() {
        return new DescribeRoleRespBuilder();
    }

    public static class DescribeRoleRespBuilder {
        private List<GrantInfo> grantInfos = new ArrayList<>();

        private DescribeRoleRespBuilder() {
        }

        public DescribeRoleRespBuilder grantInfos(List<GrantInfo> grantInfos) {
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

        private GrantInfo(GrantInfoBuilder builder) {
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

        public static GrantInfoBuilder builder() {
            return new GrantInfoBuilder();
        }

        public static class GrantInfoBuilder {
            private String objectType;
            private String objectName;
            private String roleName;
            private String grantor;
            private String privilege;
            private String dbName;

            private GrantInfoBuilder() {
            }

            public GrantInfoBuilder objectType(String objectType) {
                this.objectType = objectType;
                return this;
            }

            public GrantInfoBuilder objectName(String objectName) {
                this.objectName = objectName;
                return this;
            }

            public GrantInfoBuilder roleName(String roleName) {
                this.roleName = roleName;
                return this;
            }

            public GrantInfoBuilder grantor(String grantor) {
                this.grantor = grantor;
                return this;
            }

            public GrantInfoBuilder privilege(String privilege) {
                this.privilege = privilege;
                return this;
            }

            public GrantInfoBuilder dbName(String dbName) {
                this.dbName = dbName;
                return this;
            }

            public GrantInfo build() {
                return new GrantInfo(this);
            }
        }
    }
}

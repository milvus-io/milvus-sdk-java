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

public class GrantPrivilegeReq {
    private String roleName;
    private String objectType;
    private String privilege;
    private String objectName;

    private GrantPrivilegeReq(GrantPrivilegeReqBuilder builder) {
        this.roleName = builder.roleName;
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
    public String toString() {
        return "GrantPrivilegeReq{" +
                "roleName='" + roleName + '\'' +
                ", objectType='" + objectType + '\'' +
                ", privilege='" + privilege + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }

    public static GrantPrivilegeReqBuilder builder() {
        return new GrantPrivilegeReqBuilder();
    }

    public static class GrantPrivilegeReqBuilder {
        private String roleName;
        private String objectType;
        private String privilege;
        private String objectName;

        private GrantPrivilegeReqBuilder() {
        }

        public GrantPrivilegeReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public GrantPrivilegeReqBuilder objectType(String objectType) {
            this.objectType = objectType;
            return this;
        }

        public GrantPrivilegeReqBuilder privilege(String privilege) {
            this.privilege = privilege;
            return this;
        }

        public GrantPrivilegeReqBuilder objectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public GrantPrivilegeReq build() {
            return new GrantPrivilegeReq(this);
        }
    }
}

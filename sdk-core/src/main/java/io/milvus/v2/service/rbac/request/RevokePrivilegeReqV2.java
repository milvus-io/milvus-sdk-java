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

public class RevokePrivilegeReqV2 {
    private String roleName;
    private String privilege;
    private String dbName;
    private String collectionName;

    private RevokePrivilegeReqV2(RevokePrivilegeReqV2Builder builder) {
        this.roleName = builder.roleName;
        this.privilege = builder.privilege;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
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

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        return "RevokePrivilegeReqV2{" +
                "roleName='" + roleName + '\'' +
                ", privilege='" + privilege + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    public static RevokePrivilegeReqV2Builder builder() {
        return new RevokePrivilegeReqV2Builder();
    }

    public static class RevokePrivilegeReqV2Builder {
        private String roleName;
        private String privilege;
        private String dbName;
        private String collectionName;

        private RevokePrivilegeReqV2Builder() {
        }

        public RevokePrivilegeReqV2Builder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public RevokePrivilegeReqV2Builder privilege(String privilege) {
            this.privilege = privilege;
            return this;
        }

        public RevokePrivilegeReqV2Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public RevokePrivilegeReqV2Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public RevokePrivilegeReqV2 build() {
            return new RevokePrivilegeReqV2(this);
        }
    }
}

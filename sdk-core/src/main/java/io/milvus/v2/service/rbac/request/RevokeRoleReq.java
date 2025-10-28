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

public class RevokeRoleReq {
    private String userName;
    private String roleName;

    private RevokeRoleReq(RevokeRoleReqBuilder builder) {
        this.userName = builder.userName;
        this.roleName = builder.roleName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    @Override
    public String toString() {
        return "RevokeRoleReq{" +
                "userName='" + userName + '\'' +
                ", roleName='" + roleName + '\'' +
                '}';
    }

    public static RevokeRoleReqBuilder builder() {
        return new RevokeRoleReqBuilder();
    }

    public static class RevokeRoleReqBuilder {
        private String userName;
        private String roleName;

        private RevokeRoleReqBuilder() {
        }

        public RevokeRoleReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public RevokeRoleReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public RevokeRoleReq build() {
            return new RevokeRoleReq(this);
        }
    }
}

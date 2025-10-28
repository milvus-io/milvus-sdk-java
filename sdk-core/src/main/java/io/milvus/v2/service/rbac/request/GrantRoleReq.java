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

public class GrantRoleReq {
    private String userName;
    private String roleName;

    private GrantRoleReq(GrantRoleReqBuilder builder) {
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
        return "GrantRoleReq{" +
                "userName='" + userName + '\'' +
                ", roleName='" + roleName + '\'' +
                '}';
    }

    public static GrantRoleReqBuilder builder() {
        return new GrantRoleReqBuilder();
    }

    public static class GrantRoleReqBuilder {
        private String userName;
        private String roleName;

        private GrantRoleReqBuilder() {
        }

        public GrantRoleReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public GrantRoleReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public GrantRoleReq build() {
            return new GrantRoleReq(this);
        }
    }
}

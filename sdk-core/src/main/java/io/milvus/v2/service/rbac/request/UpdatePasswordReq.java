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

public class UpdatePasswordReq {
    private String userName;
    private String password;
    private String newPassword;

    private UpdatePasswordReq(UpdatePasswordReqBuilder builder) {
        this.userName = builder.userName;
        this.password = builder.password;
        this.newPassword = builder.newPassword;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    @Override
    public String toString() {
        return "UpdatePasswordReq{" +
                "userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", newPassword='" + newPassword + '\'' +
                '}';
    }

    public static UpdatePasswordReqBuilder builder() {
        return new UpdatePasswordReqBuilder();
    }

    public static class UpdatePasswordReqBuilder {
        private String userName;
        private String password;
        private String newPassword;

        private UpdatePasswordReqBuilder() {
        }

        public UpdatePasswordReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public UpdatePasswordReqBuilder password(String password) {
            this.password = password;
            return this;
        }

        public UpdatePasswordReqBuilder newPassword(String newPassword) {
            this.newPassword = newPassword;
            return this;
        }

        public UpdatePasswordReq build() {
            return new UpdatePasswordReq(this);
        }
    }
}

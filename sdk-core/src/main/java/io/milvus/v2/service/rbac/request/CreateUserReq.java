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

public class CreateUserReq {
    private String userName;
    private String password;

    private CreateUserReq(CreateUserReqBuilder builder) {
        this.userName = builder.userName;
        this.password = builder.password;
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

    @Override
    public String toString() {
        return "CreateUserReq{" +
                "userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public static CreateUserReqBuilder builder() {
        return new CreateUserReqBuilder();
    }

    public static class CreateUserReqBuilder {
        private String userName;
        private String password;

        private CreateUserReqBuilder() {
        }

        public CreateUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public CreateUserReqBuilder password(String password) {
            this.password = password;
            return this;
        }

        public CreateUserReq build() {
            return new CreateUserReq(this);
        }
    }
}

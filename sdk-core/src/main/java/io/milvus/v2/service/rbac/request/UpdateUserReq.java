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

public class UpdateUserReq {
    private String userName;
    private String description;

    private UpdateUserReq(UpdateUserReqBuilder builder) {
        this.userName = builder.userName;
        this.description = builder.description;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "UpdateUserReq{" +
                "userName='" + userName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    public static UpdateUserReqBuilder builder() {
        return new UpdateUserReqBuilder();
    }

    public static class UpdateUserReqBuilder {
        private String userName;
        private String description = "";

        private UpdateUserReqBuilder() {
        }

        public UpdateUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public UpdateUserReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        public UpdateUserReq build() {
            return new UpdateUserReq(this);
        }
    }
}

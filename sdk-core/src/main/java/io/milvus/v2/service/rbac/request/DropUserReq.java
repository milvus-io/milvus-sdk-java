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

public class DropUserReq {
    private String userName;

    private DropUserReq(DropUserReqBuilder builder) {
        this.userName = builder.userName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "DropUserReq{" +
                "userName='" + userName + '\'' +
                '}';
    }

    public static DropUserReqBuilder builder() {
        return new DropUserReqBuilder();
    }

    public static class DropUserReqBuilder {
        private String userName;

        private DropUserReqBuilder() {
        }

        public DropUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public DropUserReq build() {
            return new DropUserReq(this);
        }
    }
}

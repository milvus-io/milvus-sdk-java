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

public class DescribeUserReq {
    private String userName;

    private DescribeUserReq(DescribeUserReqBuilder builder) {
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
        return "DescribeUserReq{" +
                "userName='" + userName + '\'' +
                '}';
    }

    public static DescribeUserReqBuilder builder() {
        return new DescribeUserReqBuilder();
    }

    public static class DescribeUserReqBuilder {
        private String userName;

        private DescribeUserReqBuilder() {
        }

        public DescribeUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public DescribeUserReq build() {
            return new DescribeUserReq(this);
        }
    }
}

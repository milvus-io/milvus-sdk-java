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

public class DescribeUserResp {
    private List<String> roles;

    private DescribeUserResp(DescribeUserRespBuilder builder) {
        this.roles = builder.roles;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    @Override
    public String toString() {
        return "DescribeUserResp{" +
                "roles=" + roles +
                '}';
    }

    public static DescribeUserRespBuilder builder() {
        return new DescribeUserRespBuilder();
    }

    public static class DescribeUserRespBuilder {
        private List<String> roles = new ArrayList<>();

        private DescribeUserRespBuilder() {
        }

        public DescribeUserRespBuilder roles(List<String> roles) {
            this.roles = roles;
            return this;
        }

        public DescribeUserResp build() {
            return new DescribeUserResp(this);
        }
    }
}

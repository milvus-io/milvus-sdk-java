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

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

public class DescribeUserResp {
    private List<String> roles;

    private DescribeUserResp(Builder builder) {
        this.roles = builder.roles;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DescribeUserResp that = (DescribeUserResp) obj;
        return new EqualsBuilder()
                .append(roles, that.roles)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return roles != null ? roles.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DescribeUserResp{" +
                "roles=" + roles +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> roles = new ArrayList<>();

        private Builder() {}

        public Builder roles(List<String> roles) {
            this.roles = roles;
            return this;
        }

        public DescribeUserResp build() {
            return new DescribeUserResp(this);
        }
    }
}

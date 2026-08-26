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

/**
 * Response returned by the {@code describeUser} API.
 */
public class DescribeUserResp {
    private String userName;
    private List<String> roles;
    private String description;

    private DescribeUserResp(DescribeUserRespBuilder builder) {
        this.userName = builder.userName;
        this.roles = builder.roles;
        this.description = builder.description;
    }

    /**
     * Returns the user name.
     *
     * @return the user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the user name.
     *
     * @param userName the user name
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the roles granted to the user.
     *
     * @return the roles of the user
     */
    public List<String> getRoles() {
        return roles;
    }

    /**
     * Sets the roles granted to the user.
     *
     * @param roles the roles of the user
     */
    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    /**
     * Returns the description of the user.
     *
     * @return the user description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the user.
     *
     * @param description the user description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "DescribeUserResp{" +
                "userName='" + userName + '\'' +
                ", roles=" + roles +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeUserResp}.
     *
     * @return a new {@link DescribeUserRespBuilder}
     */
    public static DescribeUserRespBuilder builder() {
        return new DescribeUserRespBuilder();
    }

    public static class DescribeUserRespBuilder {
        private String userName = "";
        private List<String> roles = new ArrayList<>();
        private String description = "";

        private DescribeUserRespBuilder() {
        }

        /**
         * Sets the user name.
         *
         * @param userName the user name
         * @return this builder
         */
        public DescribeUserRespBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the roles granted to the user.
         *
         * @param roles the roles of the user
         * @return this builder
         */
        public DescribeUserRespBuilder roles(List<String> roles) {
            this.roles = roles;
            return this;
        }

        /**
         * Sets the description of the user.
         *
         * @param description the user description
         * @return this builder
         */
        public DescribeUserRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link DescribeUserResp}.
         *
         * @return the built response
         */
        public DescribeUserResp build() {
            return new DescribeUserResp(this);
        }
    }
}

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

/**
 * Request parameters for the {@code createRole} API.
 */
public class CreateRoleReq {
    private String roleName;
    private String description;

    private CreateRoleReq(CreateRoleReqBuilder builder) {
        this.roleName = builder.roleName;
        this.description = builder.description;
    }

    /**
     * Returns the role name.
     *
     * @return the role name
     */
    public String getRoleName() {
        return roleName;
    }

    /**
     * Sets the role name.
     *
     * @param roleName the role name
     */
    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    /**
     * Returns the description of the role.
     *
     * @return the role description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the role.
     *
     * @param description the role description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "CreateRoleReq{" +
                "roleName='" + roleName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link CreateRoleReq}.
     *
     * @return a new {@link CreateRoleReqBuilder}
     */
    public static CreateRoleReqBuilder builder() {
        return new CreateRoleReqBuilder();
    }

    public static class CreateRoleReqBuilder {
        private String roleName;
        private String description = "";

        private CreateRoleReqBuilder() {
        }

        /**
         * Sets the role name.
         *
         * @param roleName the role name
         * @return this builder
         */
        public CreateRoleReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the description of the role.
         *
         * @param description the role description
         * @return this builder
         */
        public CreateRoleReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link CreateRoleReq}.
         *
         * @return the built request
         */
        public CreateRoleReq build() {
            return new CreateRoleReq(this);
        }
    }
}

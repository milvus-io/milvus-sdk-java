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
 * Request parameters for the {@code dropRole} API.
 */
public class DropRoleReq {
    private String roleName;
    private boolean forceDrop;

    private DropRoleReq(DropRoleReqBuilder builder) {
        this.roleName = builder.roleName;
        this.forceDrop = builder.forceDrop;
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
     * Returns whether the role should be dropped forcibly even if it is still in use.
     *
     * @return {@code true} if the role is force dropped, {@code false} otherwise
     */
    public boolean isForceDrop() {
        return forceDrop;
    }

    /**
     * Sets whether the role should be dropped forcibly even if it is still in use.
     *
     * @param forceDrop {@code true} to force drop the role, {@code false} otherwise
     */
    public void setForceDrop(boolean forceDrop) {
        this.forceDrop = forceDrop;
    }

    @Override
    public String toString() {
        return "DropRoleReq{" +
                "roleName='" + roleName + '\'' +
                ", forceDrop=" + forceDrop +
                '}';
    }

    /**
     * Creates a new builder for {@link DropRoleReq}.
     *
     * @return a new {@link DropRoleReqBuilder}
     */
    public static DropRoleReqBuilder builder() {
        return new DropRoleReqBuilder();
    }

    public static class DropRoleReqBuilder {
        private String roleName;
        private boolean forceDrop;

        private DropRoleReqBuilder() {
        }

        /**
         * Sets the role name.
         *
         * @param roleName the role name
         * @return this builder
         */
        public DropRoleReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets whether the role should be dropped forcibly even if it is still in use.
         *
         * @param forceDrop {@code true} to force drop the role, {@code false} otherwise
         * @return this builder
         */
        public DropRoleReqBuilder forceDrop(boolean forceDrop) {
            this.forceDrop = forceDrop;
            return this;
        }

        /**
         * Builds the {@link DropRoleReq}.
         *
         * @return the built request
         */
        public DropRoleReq build() {
            return new DropRoleReq(this);
        }
    }
}

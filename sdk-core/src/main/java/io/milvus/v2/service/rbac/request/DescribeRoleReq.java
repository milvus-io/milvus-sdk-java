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
 * Request parameters for the {@code describeRole} API.
 */
public class DescribeRoleReq {
    private String roleName;
    private String dbName;

    private DescribeRoleReq(DescribeRoleReqBuilder builder) {
        this.roleName = builder.roleName;
        this.dbName = builder.dbName;
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
     * Returns the name of the database that the role applies to.
     *
     * @return the database name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Sets the name of the database that the role applies to.
     *
     * @param dbName the database name
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        return "DescribeRoleReq{" +
                "roleName='" + roleName + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeRoleReq}.
     *
     * @return a new {@link DescribeRoleReqBuilder}
     */
    public static DescribeRoleReqBuilder builder() {
        return new DescribeRoleReqBuilder();
    }

    public static class DescribeRoleReqBuilder {
        private String roleName;
        private String dbName;

        private DescribeRoleReqBuilder() {
        }

        /**
         * Sets the role name.
         *
         * @param roleName the role name
         * @return this builder
         */
        public DescribeRoleReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the name of the database that the role applies to.
         *
         * @param dbName the database name
         * @return this builder
         */
        public DescribeRoleReqBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Builds the {@link DescribeRoleReq}.
         *
         * @return the built request
         */
        public DescribeRoleReq build() {
            return new DescribeRoleReq(this);
        }
    }
}

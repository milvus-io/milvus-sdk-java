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
 * Request parameters for the {@code grantPrivilegeV2} API.
 */
public class GrantPrivilegeReqV2 {
    private String roleName;
    private String privilege;
    private String dbName;
    private String collectionName;

    private GrantPrivilegeReqV2(GrantPrivilegeReqV2Builder builder) {
        this.roleName = builder.roleName;
        this.privilege = builder.privilege;
        this.dbName = builder.dbName;
        this.collectionName = builder.collectionName;
    }

    /**
     * Returns the name of the role to which the privilege is granted.
     *
     * @return the role name
     */
    public String getRoleName() {
        return roleName;
    }

    /**
     * Sets the name of the role to which the privilege is granted.
     *
     * @param roleName the role name
     */
    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    /**
     * Returns the privilege to grant.
     *
     * @return the privilege to grant
     */
    public String getPrivilege() {
        return privilege;
    }

    /**
     * Sets the privilege to grant.
     *
     * @param privilege the privilege to grant
     */
    public void setPrivilege(String privilege) {
        this.privilege = privilege;
    }

    /**
     * Returns the name of the database to which the privilege applies.
     *
     * @return the database name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Sets the name of the database to which the privilege applies.
     *
     * @param dbName the database name
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Returns the name of the collection to which the privilege applies.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the name of the collection to which the privilege applies.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        return "GrantPrivilegeReqV2{" +
                "roleName='" + roleName + '\'' +
                ", privilege='" + privilege + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                '}';
    }

    /**
     * Creates a new {@code GrantPrivilegeReqV2} builder.
     *
     * @return the builder
     */
    public static GrantPrivilegeReqV2Builder builder() {
        return new GrantPrivilegeReqV2Builder();
    }

    /**
     * Builder for {@link GrantPrivilegeReqV2}.
     */
    public static class GrantPrivilegeReqV2Builder {
        private String roleName;
        private String privilege;
        private String dbName;
        private String collectionName;

        private GrantPrivilegeReqV2Builder() {
        }

        /**
         * Sets the name of the role to which the privilege is granted.
         *
         * @param roleName the role name
         * @return this builder
         */
        public GrantPrivilegeReqV2Builder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the privilege to grant.
         *
         * @param privilege the privilege to grant
         * @return this builder
         */
        public GrantPrivilegeReqV2Builder privilege(String privilege) {
            this.privilege = privilege;
            return this;
        }

        /**
         * Sets the name of the database to which the privilege applies.
         *
         * @param dbName the database name
         * @return this builder
         */
        public GrantPrivilegeReqV2Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Sets the name of the collection to which the privilege applies.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public GrantPrivilegeReqV2Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Builds the {@link GrantPrivilegeReqV2}.
         *
         * @return the request
         */
        public GrantPrivilegeReqV2 build() {
            return new GrantPrivilegeReqV2(this);
        }
    }
}

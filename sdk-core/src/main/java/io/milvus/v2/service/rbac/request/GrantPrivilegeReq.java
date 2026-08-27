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
 * Request parameters for the {@code grantPrivilege} API.
 */
public class GrantPrivilegeReq {
    private String roleName;
    private String dbName;
    private String objectType;
    private String privilege;
    private String objectName;

    private GrantPrivilegeReq(GrantPrivilegeReqBuilder builder) {
        this.roleName = builder.roleName;
        this.dbName = builder.dbName;
        this.objectType = builder.objectType;
        this.privilege = builder.privilege;
        this.objectName = builder.objectName;
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
     * Returns the type of the object to which the privilege applies.
     *
     * @return the object type
     */
    public String getObjectType() {
        return objectType;
    }

    /**
     * Sets the type of the object to which the privilege applies.
     *
     * @param objectType the object type
     */
    public void setObjectType(String objectType) {
        this.objectType = objectType;
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
     * Returns the name of the object to which the privilege applies.
     *
     * @return the object name
     */
    public String getObjectName() {
        return objectName;
    }

    /**
     * Sets the name of the object to which the privilege applies.
     *
     * @param objectName the object name
     */
    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    @Override
    public String toString() {
        return "GrantPrivilegeReq{" +
                "roleName='" + roleName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", objectType='" + objectType + '\'' +
                ", privilege='" + privilege + '\'' +
                ", objectName='" + objectName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link GrantPrivilegeReq}.
     *
     * @return a new {@link GrantPrivilegeReqBuilder}
     */
    public static GrantPrivilegeReqBuilder builder() {
        return new GrantPrivilegeReqBuilder();
    }

    public static class GrantPrivilegeReqBuilder {
        private String roleName;
        private String dbName;
        private String objectType;
        private String privilege;
        private String objectName;

        private GrantPrivilegeReqBuilder() {
        }

        /**
         * Sets the role name.
         *
         * @param roleName the role name
         * @return this builder
         */
        public GrantPrivilegeReqBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the name of the database to which the privilege applies.
         *
         * @param dbName the database name
         * @return this builder
         */
        public GrantPrivilegeReqBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * Sets the type of the object to which the privilege applies.
         *
         * @param objectType the object type
         * @return this builder
         */
        public GrantPrivilegeReqBuilder objectType(String objectType) {
            this.objectType = objectType;
            return this;
        }

        /**
         * Sets the privilege to grant.
         *
         * @param privilege the privilege to grant
         * @return this builder
         */
        public GrantPrivilegeReqBuilder privilege(String privilege) {
            this.privilege = privilege;
            return this;
        }

        /**
         * Sets the name of the object to which the privilege applies.
         *
         * @param objectName the object name
         * @return this builder
         */
        public GrantPrivilegeReqBuilder objectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        /**
         * Builds the {@link GrantPrivilegeReq}.
         *
         * @return the built request
         */
        public GrantPrivilegeReq build() {
            return new GrantPrivilegeReq(this);
        }
    }
}

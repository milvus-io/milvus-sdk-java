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
 * Response returned by the {@code describeRole} API.
 */
public class DescribeRoleResp {
    private String roleName;
    private List<GrantInfo> grantInfos;
    private String description;

    private DescribeRoleResp(DescribeRoleRespBuilder builder) {
        this.roleName = builder.roleName;
        this.grantInfos = builder.grantInfos;
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
     * Returns the privileges granted to the role.
     *
     * @return the grant information of the role
     */
    public List<GrantInfo> getGrantInfos() {
        return grantInfos;
    }

    /**
     * Sets the privileges granted to the role.
     *
     * @param grantInfos the grant information of the role
     */
    public void setGrantInfos(List<GrantInfo> grantInfos) {
        this.grantInfos = grantInfos;
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
        return "DescribeRoleResp{" +
                "roleName='" + roleName + '\'' +
                ", grantInfos=" + grantInfos +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DescribeRoleResp}.
     *
     * @return a new {@link DescribeRoleRespBuilder}
     */
    public static DescribeRoleRespBuilder builder() {
        return new DescribeRoleRespBuilder();
    }

    public static class DescribeRoleRespBuilder {
        private String roleName = "";
        private List<GrantInfo> grantInfos = new ArrayList<>();
        private String description = "";

        private DescribeRoleRespBuilder() {
        }

        /**
         * Sets the role name.
         *
         * @param roleName the role name
         * @return this builder
         */
        public DescribeRoleRespBuilder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the privileges granted to the role.
         *
         * @param grantInfos the grant information of the role
         * @return this builder
         */
        public DescribeRoleRespBuilder grantInfos(List<GrantInfo> grantInfos) {
            this.grantInfos = grantInfos;
            return this;
        }

        /**
         * Sets the description of the role.
         *
         * @param description the role description
         * @return this builder
         */
        public DescribeRoleRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link DescribeRoleResp}.
         *
         * @return the built response
         */
        public DescribeRoleResp build() {
            return new DescribeRoleResp(this);
        }
    }

    /**
     * Represents a single privilege grant to a role.
     */
    public static class GrantInfo {
        private String objectType;
        private String objectName;
        private String roleName;
        private String grantor;
        private String privilege;
        private String dbName;

        private GrantInfo(GrantInfoBuilder builder) {
            this.objectType = builder.objectType;
            this.objectName = builder.objectName;
            this.roleName = builder.roleName;
            this.grantor = builder.grantor;
            this.privilege = builder.privilege;
            this.dbName = builder.dbName;
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
         * Returns the name of the user who granted the privilege.
         *
         * @return the grantor name
         */
        public String getGrantor() {
            return grantor;
        }

        /**
         * Sets the name of the user who granted the privilege.
         *
         * @param grantor the grantor name
         */
        public void setGrantor(String grantor) {
            this.grantor = grantor;
        }

        /**
         * Returns the granted privilege.
         *
         * @return the granted privilege
         */
        public String getPrivilege() {
            return privilege;
        }

        /**
         * Sets the granted privilege.
         *
         * @param privilege the granted privilege
         */
        public void setPrivilege(String privilege) {
            this.privilege = privilege;
        }

        /**
         * Returns the name of the database in which the privilege is granted.
         *
         * @return the database name
         */
        public String getDbName() {
            return dbName;
        }

        /**
         * Sets the name of the database in which the privilege is granted.
         *
         * @param dbName the database name
         */
        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        @Override
        public String toString() {
            return "GrantInfo{" +
                    "objectType='" + objectType + '\'' +
                    ", objectName='" + objectName + '\'' +
                    ", roleName='" + roleName + '\'' +
                    ", grantor='" + grantor + '\'' +
                    ", privilege='" + privilege + '\'' +
                    ", dbName='" + dbName + '\'' +
                    '}';
        }

        /**
         * Creates a new builder for {@link GrantInfo}.
         *
         * @return a new {@link GrantInfoBuilder}
         */
        public static GrantInfoBuilder builder() {
            return new GrantInfoBuilder();
        }

        public static class GrantInfoBuilder {
            private String objectType;
            private String objectName;
            private String roleName;
            private String grantor;
            private String privilege;
            private String dbName;

            private GrantInfoBuilder() {
            }

            /**
             * Sets the type of the object to which the privilege applies.
             *
             * @param objectType the object type
             * @return this builder
             */
            public GrantInfoBuilder objectType(String objectType) {
                this.objectType = objectType;
                return this;
            }

            /**
             * Sets the name of the object to which the privilege applies.
             *
             * @param objectName the object name
             * @return this builder
             */
            public GrantInfoBuilder objectName(String objectName) {
                this.objectName = objectName;
                return this;
            }

            /**
             * Sets the name of the role to which the privilege is granted.
             *
             * @param roleName the role name
             * @return this builder
             */
            public GrantInfoBuilder roleName(String roleName) {
                this.roleName = roleName;
                return this;
            }

            /**
             * Sets the name of the user who granted the privilege.
             *
             * @param grantor the grantor name
             * @return this builder
             */
            public GrantInfoBuilder grantor(String grantor) {
                this.grantor = grantor;
                return this;
            }

            /**
             * Sets the granted privilege.
             *
             * @param privilege the granted privilege
             * @return this builder
             */
            public GrantInfoBuilder privilege(String privilege) {
                this.privilege = privilege;
                return this;
            }

            /**
             * Sets the name of the database in which the privilege is granted.
             *
             * @param dbName the database name
             * @return this builder
             */
            public GrantInfoBuilder dbName(String dbName) {
                this.dbName = dbName;
                return this;
            }

            /**
             * Builds the {@link GrantInfo}.
             *
             * @return the built grant info
             */
            public GrantInfo build() {
                return new GrantInfo(this);
            }
        }
    }
}

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

package io.milvus.v2.service.rbac;

import java.util.ArrayList;
import java.util.List;

/**
 * A privilege group that bundles multiple privileges for RBAC management.
 */
public class PrivilegeGroup {
    private String groupName;
    private List<String> privileges;

    private PrivilegeGroup(PrivilegeGroupBuilder builder) {
        this.groupName = builder.groupName;
        this.privileges = builder.privileges;
    }

    /**
     * Returns the name of the privilege group.
     *
     * @return the privilege group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the name of the privilege group.
     *
     * @param groupName the privilege group name
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Returns the privileges contained in the group.
     *
     * @return the privileges
     */
    public List<String> getPrivileges() {
        return privileges;
    }

    /**
     * Sets the privileges contained in the group.
     *
     * @param privileges the privileges
     */
    public void setPrivileges(List<String> privileges) {
        this.privileges = privileges;
    }

    @Override
    public String toString() {
        return "PrivilegeGroup{" +
                "groupName='" + groupName + '\'' +
                ", privileges=" + privileges +
                '}';
    }

    /**
     * Creates a new {@code PrivilegeGroup} builder.
     *
     * @return the builder
     */
    public static PrivilegeGroupBuilder builder() {
        return new PrivilegeGroupBuilder();
    }

    /**
     * Builder for {@link PrivilegeGroup}.
     */
    public static class PrivilegeGroupBuilder {
        private String groupName;
        private List<String> privileges = new ArrayList<>();

        private PrivilegeGroupBuilder() {
        }

        /**
         * Sets the name of the privilege group.
         *
         * @param groupName the privilege group name
         * @return this builder
         */
        public PrivilegeGroupBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        /**
         * Sets the privileges contained in the group.
         *
         * @param privileges the privileges
         * @return this builder
         */
        public PrivilegeGroupBuilder privileges(List<String> privileges) {
            this.privileges = privileges;
            return this;
        }

        /**
         * Builds the {@link PrivilegeGroup}.
         *
         * @return the privilege group
         */
        public PrivilegeGroup build() {
            return new PrivilegeGroup(this);
        }
    }
}

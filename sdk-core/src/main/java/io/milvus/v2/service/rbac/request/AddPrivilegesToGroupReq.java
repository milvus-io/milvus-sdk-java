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

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.ArrayList;
import java.util.List;

public class AddPrivilegesToGroupReq {
    private String groupName;
    private List<String> privileges = new ArrayList<>();

    private AddPrivilegesToGroupReq(Builder builder) {
        this.groupName = builder.groupName;
        this.privileges = builder.privileges;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<String> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(List<String> privileges) {
        this.privileges = privileges;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AddPrivilegesToGroupReq that = (AddPrivilegesToGroupReq) obj;
        return new EqualsBuilder()
                .append(groupName, that.groupName)
                .append(privileges, that.privileges)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = groupName != null ? groupName.hashCode() : 0;
        result = 31 * result + (privileges != null ? privileges.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AddPrivilegesToGroupReq{" +
                "groupName='" + groupName + '\'' +
                ", privileges=" + privileges +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String groupName;
        private List<String> privileges = new ArrayList<>();

        private Builder() {}

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder privileges(List<String> privileges) {
            this.privileges = privileges;
            return this;
        }

        public AddPrivilegesToGroupReq build() {
            return new AddPrivilegesToGroupReq(this);
        }
    }
}

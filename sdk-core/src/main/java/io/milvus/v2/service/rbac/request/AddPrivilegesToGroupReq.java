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

import java.util.ArrayList;
import java.util.List;

public class AddPrivilegesToGroupReq {
    private String groupName;
    private List<String> privileges = new ArrayList<>();

    private AddPrivilegesToGroupReq(AddPrivilegesToGroupReqBuilder builder) {
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
    public String toString() {
        return "AddPrivilegesToGroupReq{" +
                "groupName='" + groupName + '\'' +
                ", privileges=" + privileges +
                '}';
    }

    public static AddPrivilegesToGroupReqBuilder builder() {
        return new AddPrivilegesToGroupReqBuilder();
    }

    public static class AddPrivilegesToGroupReqBuilder {
        private String groupName;
        private List<String> privileges = new ArrayList<>();

        private AddPrivilegesToGroupReqBuilder() {
        }

        public AddPrivilegesToGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public AddPrivilegesToGroupReqBuilder privileges(List<String> privileges) {
            this.privileges = privileges;
            return this;
        }

        public AddPrivilegesToGroupReq build() {
            return new AddPrivilegesToGroupReq(this);
        }
    }
}

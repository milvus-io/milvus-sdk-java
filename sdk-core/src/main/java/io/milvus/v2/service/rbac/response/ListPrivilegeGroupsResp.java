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

import io.milvus.v2.service.rbac.PrivilegeGroup;

import java.util.ArrayList;
import java.util.List;

public class ListPrivilegeGroupsResp {
    private List<PrivilegeGroup> privilegeGroups;

    private ListPrivilegeGroupsResp(ListPrivilegeGroupsRespBuilder builder) {
        this.privilegeGroups = builder.privilegeGroups;
    }

    public List<PrivilegeGroup> getPrivilegeGroups() {
        return privilegeGroups;
    }

    public void setPrivilegeGroups(List<PrivilegeGroup> privilegeGroups) {
        this.privilegeGroups = privilegeGroups;
    }

    @Override
    public String toString() {
        return "ListPrivilegeGroupsResp{" +
                "privilegeGroups=" + privilegeGroups +
                '}';
    }

    public static ListPrivilegeGroupsRespBuilder builder() {
        return new ListPrivilegeGroupsRespBuilder();
    }

    public static class ListPrivilegeGroupsRespBuilder {
        private List<PrivilegeGroup> privilegeGroups = new ArrayList<>();

        private ListPrivilegeGroupsRespBuilder() {
        }

        public ListPrivilegeGroupsRespBuilder privilegeGroups(List<PrivilegeGroup> privilegeGroups) {
            this.privilegeGroups = privilegeGroups;
            return this;
        }

        public ListPrivilegeGroupsResp build() {
            return new ListPrivilegeGroupsResp(this);
        }
    }
}

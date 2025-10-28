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

public class DropPrivilegeGroupReq {
    private String groupName;

    private DropPrivilegeGroupReq(DropPrivilegeGroupReqBuilder builder) {
        this.groupName = builder.groupName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "DropPrivilegeGroupReq{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    public static DropPrivilegeGroupReqBuilder builder() {
        return new DropPrivilegeGroupReqBuilder();
    }

    public static class DropPrivilegeGroupReqBuilder {
        private String groupName;

        private DropPrivilegeGroupReqBuilder() {
        }

        public DropPrivilegeGroupReqBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public DropPrivilegeGroupReq build() {
            return new DropPrivilegeGroupReq(this);
        }
    }
}

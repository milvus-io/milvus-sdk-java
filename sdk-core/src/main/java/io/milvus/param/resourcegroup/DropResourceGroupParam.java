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


package io.milvus.param.resourcegroup;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

public class DropResourceGroupParam {
    private final String groupName;

    private DropResourceGroupParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.groupName = builder.groupName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getGroupName() {
        return groupName;
    }

    @Override
    public String toString() {
        return "DropResourceGroupParam{" +
                "groupName='" + groupName + '\'' +
                '}';
    }

    /**
     * Builder for {@link DropResourceGroupParam} class.
     */
    public static final class Builder {
        private String groupName;

        private Builder() {
        }

        /**
         * Sets the group name. group name cannot be empty or null.
         *
         * @param groupName group name
         * @return <code>Builder</code>
         */
        public Builder withGroupName(String groupName) {
            if (groupName == null) {
                throw new IllegalArgumentException("Group name cannot be null");
            }
            this.groupName = groupName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DropResourceGroupParam} instance.
         *
         * @return {@link DropResourceGroupParam}
         */
        public DropResourceGroupParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(groupName, "Group name");

            return new DropResourceGroupParam(this);
        }
    }

}

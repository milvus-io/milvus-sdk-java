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

package io.milvus.param.role;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

public class DropRoleParam {

    private final String roleName;
    private final boolean forceDrop;

    private DropRoleParam(DropRoleParam.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.roleName = builder.roleName;
        this.forceDrop = builder.forceDrop;
    }

    public static DropRoleParam.Builder newBuilder() {
        return new DropRoleParam.Builder();
    }

    public String getRoleName() {
        return roleName;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public String toString() {
        return "DropRoleParam{" +
                "roleName='" + roleName + '\'' +
                ", forceDrop=" + forceDrop +
                '}';
    }

    /**
     * Builder for {@link DropRoleParam} class.
     */
    public static final class Builder {
        private String roleName;
        private boolean forceDrop;

        private Builder() {
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public DropRoleParam.Builder withRoleName(String roleName) {
            if (roleName == null) {
                throw new IllegalArgumentException("Role name cannot be null");
            }
            this.roleName = roleName;
            return this;
        }

        /**
         * Sets the forceDrop flag. If true, the role will be force dropped.
         *
         * @param forceDrop forceDrop
         * @return <code>Builder</code>
         */
        public DropRoleParam.Builder withForceDrop(boolean forceDrop) {
            this.forceDrop = forceDrop;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DropRoleParam} instance.
         *
         * @return {@link DropRoleParam}
         */
        public DropRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new DropRoleParam(this);
        }
    }

}

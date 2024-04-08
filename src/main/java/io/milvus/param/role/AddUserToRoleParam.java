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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class AddUserToRoleParam {
    private final String userName;

    private final String roleName;

    private AddUserToRoleParam(@NonNull AddUserToRoleParam.Builder builder) {
        this.userName = builder.userName;
        this.roleName = builder.roleName;
    }

    public static AddUserToRoleParam.Builder newBuilder() {
        return new AddUserToRoleParam.Builder();
    }

    /**
     * Builder for {@link AddUserToRoleParam} class.
     */
    public static final class Builder {
        private String userName;
        private String roleName;

        private Builder() {
        }

        /**
         * Sets the username. UserName cannot be empty or null.
         *
         * @param userName userName
         * @return <code>Builder</code>
         */
        public AddUserToRoleParam.Builder withUserName(@NonNull String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public AddUserToRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link AddUserToRoleParam} instance.
         *
         * @return {@link AddUserToRoleParam}
         */
        public AddUserToRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(userName, "UserName");
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new AddUserToRoleParam(this);
        }
    }

}
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
import io.milvus.param.partition.ShowPartitionsParam;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class SelectGrantForRoleParam {
    private final String databaseName;

    private final String roleName;

    private SelectGrantForRoleParam(@NonNull SelectGrantForRoleParam.Builder builder) {
        this.databaseName = builder.databaseName;
        this.roleName = builder.roleName;
    }

    public static SelectGrantForRoleParam.Builder newBuilder() {
        return new SelectGrantForRoleParam.Builder();
    }

    /**
     * Builder for {@link SelectGrantForRoleParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String roleName;

        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the roleName. RoleName cannot be empty or null.
         *
         * @param roleName roleName
         * @return <code>Builder</code>
         */
        public SelectGrantForRoleParam.Builder withRoleName(@NonNull String roleName) {
            this.roleName = roleName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectGrantForRoleParam} instance.
         *
         * @return {@link SelectGrantForRoleParam}
         */
        public SelectGrantForRoleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(roleName, "RoleName");

            return new SelectGrantForRoleParam(this);
        }
    }

}

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

public class SelectUserParam {

    private final String userName;

    private final boolean includeRoleInfo;

    private SelectUserParam(SelectUserParam.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        this.userName = builder.userName;
        this.includeRoleInfo = builder.includeRoleInfo;
    }

    public static SelectUserParam.Builder newBuilder() {
        return new SelectUserParam.Builder();
    }

    public String getUserName() {
        return userName;
    }

    public boolean isIncludeRoleInfo() {
        return includeRoleInfo;
    }

    @Override
    public String toString() {
        return "SelectUserParam{" +
                "userName='" + userName + '\'' +
                ", includeRoleInfo=" + includeRoleInfo +
                '}';
    }

    /**
     * Builder for {@link SelectUserParam} class.
     */
    public static final class Builder {
        private String userName;
        private boolean includeRoleInfo;

        private Builder() {
        }

        /**
         * Sets the userName. userName cannot be empty or null.
         *
         * @param userName userName
         * @return <code>Builder</code>
         */
        public SelectUserParam.Builder withUserName(String userName) {
            if (userName == null) {
                throw new IllegalArgumentException("User name cannot be null");
            }
            this.userName = userName;
            return this;
        }

        /**
         * Sets the includeRoleInfo. includeRoleInfo default false.
         *
         * @param includeRoleInfo includeRoleInfo
         * @return <code>Builder</code>
         */
        public SelectUserParam.Builder withIncludeRoleInfo(boolean includeRoleInfo) {
            this.includeRoleInfo = includeRoleInfo;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SelectUserParam} instance.
         *
         * @return {@link SelectUserParam}
         */
        public SelectUserParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(userName, "UserName");

            return new SelectUserParam(this);
        }
    }

}

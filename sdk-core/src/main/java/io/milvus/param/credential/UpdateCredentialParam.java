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

package io.milvus.param.credential;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

public class UpdateCredentialParam {
    private final String username;
    private final String oldPassword;
    private final String newPassword;

    private UpdateCredentialParam(UpdateCredentialParam.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.username = builder.username;
        this.oldPassword = builder.oldPassword;
        this.newPassword = builder.newPassword;
    }

    public static UpdateCredentialParam.Builder newBuilder() {
        return new UpdateCredentialParam.Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getUsername() {
        return username;
    }

    public String getOldPassword() {
        return oldPassword;
    }

    public String getNewPassword() {
        return newPassword;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "UpdateCredentialParam{" +
                "username='" + username + '\'' +
                ", oldPassword='" + oldPassword + '\'' +
                ", newPassword='" + newPassword + '\'' +
                '}';
    }

    /**
     * Builder for {@link UpdateCredentialParam} class.
     */
    public static final class Builder {
        private String username;
        private String oldPassword;
        private String newPassword;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withUsername(String username) {
            // Replace @NonNull logic with explicit null check
            if (username == null) {
                throw new IllegalArgumentException("username cannot be null");
            }
            this.username = username;
            return this;
        }

        /**
         * Sets the old password. Old password cannot be empty or null.
         *
         * @param password old password
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withOldPassword(String password) {
            // Replace @NonNull logic with explicit null check
            if (password == null) {
                throw new IllegalArgumentException("password cannot be null");
            }
            this.oldPassword = password;
            return this;
        }

        /**
         * Sets the new password. New password cannot be empty or null.
         *
         * @param password password
         * @return <code>Builder</code>
         */
        public UpdateCredentialParam.Builder withNewPassword(String password) {
            // Replace @NonNull logic with explicit null check
            if (password == null) {
                throw new IllegalArgumentException("password cannot be null");
            }
            this.newPassword = password;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link UpdateCredentialParam} instance.
         *
         * @return {@link UpdateCredentialParam}
         */
        public UpdateCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");
            ParamUtils.CheckNullString(oldPassword, "OldPassword");
            ParamUtils.CheckNullEmptyString(newPassword, "NewPassword");

            return new UpdateCredentialParam(this);
        }
    }
}

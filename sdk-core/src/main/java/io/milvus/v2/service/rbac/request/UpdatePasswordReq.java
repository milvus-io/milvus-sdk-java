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

import static io.milvus.common.utils.RedactCredential.redactCredential;

/**
 * Request parameters for the {@code updatePassword} API.
 */
public class UpdatePasswordReq {
    private String userName;
    private String password;
    private String newPassword;
    private Boolean resetConnection;
    private String description;

    private UpdatePasswordReq(UpdatePasswordReqBuilder builder) {
        this.userName = builder.userName;
        this.password = builder.password;
        this.newPassword = builder.newPassword;
        this.resetConnection = builder.resetConnection;
        this.description = builder.description;
    }

    /**
     * Returns the user name.
     *
     * @return the user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the user name.
     *
     * @param userName the user name
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the current password of the user.
     *
     * @return the current user password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the current password of the user.
     *
     * @param password the current user password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Returns the new password of the user.
     *
     * @return the new user password
     */
    public String getNewPassword() {
        return newPassword;
    }

    /**
     * Sets the new password of the user.
     *
     * @param newPassword the new user password
     */
    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    /**
     * Returns whether existing connections of the user are reset after the password update.
     *
     * @return {@code true} if existing connections are reset, {@code false} otherwise
     */
    public Boolean getResetConnection() {
        return resetConnection;
    }

    /**
     * Sets whether existing connections of the user are reset after the password update.
     *
     * @param resetConnection {@code true} to reset existing connections, {@code false} otherwise
     */
    public void setResetConnection(Boolean resetConnection) {
        this.resetConnection = resetConnection;
    }

    /**
     * Returns the description of the user.
     *
     * @return the user description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the user.
     *
     * @param description the user description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "UpdatePasswordReq{" +
                "userName='" + userName + '\'' +
                ", password='" + redactCredential(password) + '\'' +
                ", newPassword='" + redactCredential(newPassword) + '\'' +
                ", resetConnection=" + resetConnection +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link UpdatePasswordReq}.
     *
     * @return a new {@link UpdatePasswordReqBuilder}
     */
    public static UpdatePasswordReqBuilder builder() {
        return new UpdatePasswordReqBuilder();
    }

    public static class UpdatePasswordReqBuilder {
        private String userName;
        private String password;
        private String newPassword;
        private Boolean resetConnection = Boolean.FALSE;
        private String description = "";

        private UpdatePasswordReqBuilder() {
        }

        /**
         * Sets the user name.
         *
         * @param userName the user name
         * @return this builder
         */
        public UpdatePasswordReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the current password of the user.
         *
         * @param password the current user password
         * @return this builder
         */
        public UpdatePasswordReqBuilder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the new password of the user.
         *
         * @param newPassword the new user password
         * @return this builder
         */
        public UpdatePasswordReqBuilder newPassword(String newPassword) {
            this.newPassword = newPassword;
            return this;
        }

        /**
         * Sets whether existing connections of the user are reset after the password update.
         *
         * @param resetConnection {@code true} to reset existing connections, {@code false} otherwise
         * @return this builder
         */
        public UpdatePasswordReqBuilder resetConnection(Boolean resetConnection) {
            this.resetConnection = resetConnection;
            return this;
        }

        /**
         * Sets the description of the user.
         *
         * @param description the user description
         * @return this builder
         */
        public UpdatePasswordReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link UpdatePasswordReq}.
         *
         * @return the built request
         */
        public UpdatePasswordReq build() {
            return new UpdatePasswordReq(this);
        }
    }
}

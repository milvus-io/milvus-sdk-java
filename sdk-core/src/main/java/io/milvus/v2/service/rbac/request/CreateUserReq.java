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
 * Request parameters for the {@code createUser} API.
 */
public class CreateUserReq {
    private String userName;
    private String password;
    private String description;

    private CreateUserReq(CreateUserReqBuilder builder) {
        this.userName = builder.userName;
        this.password = builder.password;
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
     * Returns the password of the user.
     *
     * @return the user password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password of the user.
     *
     * @param password the user password
     */
    public void setPassword(String password) {
        this.password = password;
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
        return "CreateUserReq{" +
                "userName='" + userName + '\'' +
                ", password='" + redactCredential(password) + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link CreateUserReq}.
     *
     * @return a new {@link CreateUserReqBuilder}
     */
    public static CreateUserReqBuilder builder() {
        return new CreateUserReqBuilder();
    }

    public static class CreateUserReqBuilder {
        private String userName;
        private String password;
        private String description = "";

        private CreateUserReqBuilder() {
        }

        /**
         * Sets the user name.
         *
         * @param userName the user name
         * @return this builder
         */
        public CreateUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the password of the user.
         *
         * @param password the user password
         * @return this builder
         */
        public CreateUserReqBuilder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the description of the user.
         *
         * @param description the user description
         * @return this builder
         */
        public CreateUserReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link CreateUserReq}.
         *
         * @return the built request
         */
        public CreateUserReq build() {
            return new CreateUserReq(this);
        }
    }
}

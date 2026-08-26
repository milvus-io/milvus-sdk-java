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

/**
 * Request parameters for the {@code updateUser} API.
 */
public class UpdateUserReq {
    private String userName;
    private String description;

    private UpdateUserReq(UpdateUserReqBuilder builder) {
        this.userName = builder.userName;
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
        return "UpdateUserReq{" +
                "userName='" + userName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link UpdateUserReq}.
     *
     * @return a new {@link UpdateUserReqBuilder}
     */
    public static UpdateUserReqBuilder builder() {
        return new UpdateUserReqBuilder();
    }

    public static class UpdateUserReqBuilder {
        private String userName;
        private String description = "";

        private UpdateUserReqBuilder() {
        }

        /**
         * Sets the user name.
         *
         * @param userName the user name
         * @return this builder
         */
        public UpdateUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Sets the description of the user.
         *
         * @param description the user description
         * @return this builder
         */
        public UpdateUserReqBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builds the {@link UpdateUserReq}.
         *
         * @return the built request
         */
        public UpdateUserReq build() {
            return new UpdateUserReq(this);
        }
    }
}

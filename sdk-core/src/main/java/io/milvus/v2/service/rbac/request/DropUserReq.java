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
 * Request parameters for the {@code dropUser} API.
 */
public class DropUserReq {
    private String userName;

    private DropUserReq(DropUserReqBuilder builder) {
        this.userName = builder.userName;
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

    @Override
    public String toString() {
        return "DropUserReq{" +
                "userName='" + userName + '\'' +
                '}';
    }

    /**
     * Creates a new builder for {@link DropUserReq}.
     *
     * @return a new {@link DropUserReqBuilder}
     */
    public static DropUserReqBuilder builder() {
        return new DropUserReqBuilder();
    }

    public static class DropUserReqBuilder {
        private String userName;

        private DropUserReqBuilder() {
        }

        /**
         * Sets the user name.
         *
         * @param userName the user name
         * @return this builder
         */
        public DropUserReqBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        /**
         * Builds the {@link DropUserReq}.
         *
         * @return the built request
         */
        public DropUserReq build() {
            return new DropUserReq(this);
        }
    }
}

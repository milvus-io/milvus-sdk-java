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

public class DeleteCredentialParam {
    private final String username;

    private DeleteCredentialParam(DeleteCredentialParam.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.username = builder.username;
    }

    public static DeleteCredentialParam.Builder newBuilder() {
        return new DeleteCredentialParam.Builder();
    }

    // Getter method to replace @Getter annotation
    public String getUsername() {
        return username;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "DeleteCredentialParam{" +
                "username='" + username + '\'' +
                '}';
    }

    /**
     * Builder for {@link DeleteCredentialParam} class.
     */
    public static final class Builder {
        private String username;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public DeleteCredentialParam.Builder withUsername(String username) {
            // Replace @NonNull logic with explicit null check
            if (username == null) {
                throw new IllegalArgumentException("username cannot be null");
            }
            this.username = username;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DeleteCredentialParam} instance.
         *
         * @return {@link DeleteCredentialParam}
         */
        public DeleteCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");

            return new DeleteCredentialParam(this);
        }
    }
}

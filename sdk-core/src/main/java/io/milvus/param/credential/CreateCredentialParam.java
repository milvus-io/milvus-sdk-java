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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class CreateCredentialParam {
    private final String username;

    private final String password;

    private CreateCredentialParam(@NonNull CreateCredentialParam.Builder builder) {
        this.username = builder.username;
        this.password = builder.password;
    }

    public static CreateCredentialParam.Builder newBuilder() {
        return new CreateCredentialParam.Builder();
    }

    /**
     * Builder for {@link CreateCredentialParam} class.
     */
    public static final class Builder {
        private String username;
        private String password;

        private Builder() {
        }

        /**
         * Sets the username. Username cannot be empty or null.
         *
         * @param username username
         * @return <code>Builder</code>
         */
        public CreateCredentialParam.Builder withUsername(@NonNull String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password. Password cannot be empty or null.
         *
         * @param password password
         * @return <code>Builder</code>
         */
        public CreateCredentialParam.Builder withPassword(@NonNull String password) {
            this.password = password;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateCredentialParam} instance.
         *
         * @return {@link CreateCredentialParam}
         */
        public CreateCredentialParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(username, "Username");
            ParamUtils.CheckNullEmptyString(password, "Password");

            return new CreateCredentialParam(this);
        }
    }

}

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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
public class ListCredUsersParam {

    private ListCredUsersParam(@NonNull ListCredUsersParam.Builder builder) {
    }

    public static ListCredUsersParam.Builder newBuilder() {
        return new ListCredUsersParam.Builder();
    }

    /**
     * Builder for {@link ListCredUsersParam} class.
     */
    public static final class Builder {

        private Builder() {
        }

        /**
         * Verifies parameters and creates a new {@link ListCredUsersParam} instance.
         *
         * @return {@link ListCredUsersParam}
         */
        public ListCredUsersParam build() throws ParamException {
            return new ListCredUsersParam(this);
        }
    }

}

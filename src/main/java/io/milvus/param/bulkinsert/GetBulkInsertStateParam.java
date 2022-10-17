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

package io.milvus.param.bulkinsert;

import io.milvus.exception.ParamException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Parameters for <code>getBulkInsertState</code> interface.
 */
@Getter
public class GetBulkInsertStateParam {
    private final Long task;

    private GetBulkInsertStateParam(@NonNull Builder builder) {
        this.task = builder.task;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetBulkInsertStateParam} class.
     */
    public static final class Builder {
        private Long task;

        private Builder() {
        }

        /**
         * Sets the task id. task cannot be empty or null.
         *
         * @param task task id
         * @return <code>Builder</code>
         */
        public Builder withTask(@NonNull Long task) {
            this.task = task;
            return this;
        }


        /**
         * Verifies parameters and creates a new {@link GetBulkInsertStateParam} instance.
         *
         * @return {@link GetBulkInsertStateParam}
         */
        public GetBulkInsertStateParam build() throws ParamException {
            return new GetBulkInsertStateParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link GetBulkInsertStateParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetBulkInsertStateParam{" +
                "task='" + task + '\'' +
                '}';
    }
}
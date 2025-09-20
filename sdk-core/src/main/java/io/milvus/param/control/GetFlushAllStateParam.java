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

package io.milvus.param.control;

import io.milvus.exception.ParamException;

/**
 * Parameters for <code>getFlushAllState</code> interface.
 */
public class GetFlushAllStateParam {
    private final String databaseName;
    private final long flushAllTs;

    private GetFlushAllStateParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.flushAllTs = builder.flushAllTs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getDatabaseName() {
        return databaseName;
    }

    public long getFlushAllTs() {
        return flushAllTs;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "GetFlushAllStateParam{" +
                "databaseName='" + databaseName + '\'' +
                ", flushAllTs=" + flushAllTs +
                '}';
    }

    /**
     * Builder for {@link GetFlushAllStateParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private Long flushAllTs;

        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withFlushAllTs(Long flushAllTs) {
            // Replace @NonNull logic with explicit null check
            if (flushAllTs == null) {
                throw new IllegalArgumentException("flushAllTs cannot be null");
            }
            this.flushAllTs = flushAllTs;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetFlushAllStateParam} instance.
         *
         * @return {@link GetFlushAllStateParam}
         */
        public GetFlushAllStateParam build() throws ParamException {
            return new GetFlushAllStateParam(this);
        }
    }
}

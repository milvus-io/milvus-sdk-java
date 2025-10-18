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

package io.milvus.param.collection;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

/**
 * Parameters for <code>describeDatabase</code> interface.
 */
public class DescribeDatabaseParam {
    private final String databaseName;

    private DescribeDatabaseParam(Builder builder) {
        if (builder.databaseName == null) {
            throw new IllegalArgumentException("databaseName cannot be null");
        }
        this.databaseName = builder.databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return "DescribeDatabaseParam{" +
                "databaseName='" + databaseName + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link DescribeDatabaseParam} class.
     */
    public static final class Builder {
        private String databaseName;

        private Builder() {
        }

        /**
         * Set the database name. Database name cannot be empty or null.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            if (databaseName == null) {
                throw new IllegalArgumentException("databaseName cannot be null");
            }
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DescribeDatabaseParam} instance.
         *
         * @return {@link DescribeDatabaseParam}
         */
        public DescribeDatabaseParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(databaseName, "Database name");

            return new DescribeDatabaseParam(this);
        }
    }

}

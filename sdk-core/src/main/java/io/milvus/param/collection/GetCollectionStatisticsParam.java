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

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>getCollectionStatistics</code> interface.
 */
@Getter
@ToString
public class GetCollectionStatisticsParam {
    private final String databaseName;
    private final String collectionName;
    private final boolean flushCollection;

    private GetCollectionStatisticsParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.flushCollection = builder.flushCollection;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetCollectionStatisticsParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;

        // if flushCollection is true, getCollectionStatistics() firstly call flush() and wait flush() finish
        // Note: use default interval and timeout to wait flush()
        private Boolean flushCollection = Boolean.FALSE;

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

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Requires a flush action before retrieving collection statistics.
         *
         * @param flush <code>Boolean.TRUE</code> require a flush action
         * @return <code>Builder</code>
         */
        public Builder withFlush(@NonNull Boolean flush) {
            this.flushCollection = flush;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetCollectionStatisticsParam} instance.
         *
         * @return {@link GetCollectionStatisticsParam}
         */
        public GetCollectionStatisticsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetCollectionStatisticsParam(this);
        }
    }

}

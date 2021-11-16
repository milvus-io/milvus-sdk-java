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

/**
 * Parameters for <code>getCollectionStatistics</code> interface.
 */
@Getter
public class GetCollectionStatisticsParam {
    private final String collectionName;
    private final boolean flushCollection;

    private GetCollectionStatisticsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.flushCollection = builder.flushCollection;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>GetCollectionStatisticsParam</code> class.
     */
    public static final class Builder {
        private String collectionName;

        // if flushCollection is true, getCollectionStatistics() firstly call flush() and wait flush() finish
        // Note: use default interval and timeout to wait flush()
        private Boolean flushCollection = Boolean.TRUE;

        private Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Require a flush action before retrieving collection statistics.
         *
         * @param flush <code>Boolean.TRUE</code> require a flush action
         * @return <code>Builder</code>
         */
        public Builder withFlush(@NonNull Boolean flush) {
            this.flushCollection = flush;
            return this;
        }

        /**
         * Verify parameters and create a new <code>GetCollectionStatisticsParam</code> instance.
         *
         * @return <code>GetCollectionStatisticsParam</code>
         */
        public GetCollectionStatisticsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetCollectionStatisticsParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>GetCollectionStatisticsParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "GetCollectionStatisticsParam{" +
                "collectionName='" + collectionName + '\'' +
                " flush=" + flushCollection +
                '}';
    }
}

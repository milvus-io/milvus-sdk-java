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
 * Parameters for <code>releaseCollection</code> interface.
 */
@Getter
public class ReleaseCollectionParam {
    private final String collectionName;

    private ReleaseCollectionParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>ReleaseCollectionParam</code> class.
     */
    public static final class Builder {
        private String collectionName;

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
         * Verify parameters and create a new <code>ReleaseCollectionParam</code> instance.
         *
         * @return <code>ReleaseCollectionParam</code>
         */
        public ReleaseCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new ReleaseCollectionParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>ReleaseCollectionParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ReleaseCollectionParam{" +
                "collectionName='" + collectionName + '\'' + '}';
    }
}

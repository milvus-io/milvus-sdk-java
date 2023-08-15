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
import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Parameters for <code>alterCollection</code> interface.
 */
@Getter
public class AlterCollectionParam {
    private final String collectionName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        if (builder.ttlSeconds >= 0) {
            this.properties.put(Constant.TTL_SECONDS, Integer.toString(builder.ttlSeconds));
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link AlterCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;

        // The ttlSeconds value should be 0 or greater.
        // In server side, the default value is 0, which means TTL is disabled.
        // Here we set ttlSeconds = -1 to avoid being overwritten with unconscious
        private Integer ttlSeconds = -1;


        private Builder() {
        }

        /**
         * Set the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Collection time to live (TTL) is the expiration time of data in a collection.
         * Expired data in the collection will be cleaned up and will not be involved in searches or queries.
         * Specify TTL in the unit of seconds.
         *
         * @param ttlSeconds TTL seconds, value should be 0 or greater
         * @return <code>Builder</code>
         */
        public Builder withTTL(@NonNull Integer ttlSeconds) {
            if (ttlSeconds < 0) {
                throw new ParamException("The ttlSeconds value should be 0 or greater");
            }
            this.ttlSeconds = ttlSeconds;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link AlterCollectionParam} instance.
         *
         * @return {@link AlterCollectionParam}
         */
        public AlterCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new AlterCollectionParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link AlterCollectionParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "AlterCollectionParam{" +
                "collectionName='" + collectionName + '\'' +
                ", properties='" + properties.toString() + '\'' +
                '}';
    }
}
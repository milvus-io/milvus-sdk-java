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
import io.milvus.param.ParamUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for <code>alterCollection</code> interface.
 */
public class AlterCollectionParam {
    private final String collectionName;
    private final String databaseName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterCollectionParam(Builder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("collectionName cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.properties.putAll(builder.properties);
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "AlterCollectionParam{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link AlterCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String databaseName;

        private final Map<String, String> properties = new HashMap<>();

        private Builder() {
        }

        /**
         * Set the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionName = collectionName;
            return this;
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
         * Collection time to live (TTL) is the expiration time of data in a collection.
         * Expired data in the collection will be cleaned up and will not be involved in searches or queries.
         * In server side, the default value is 0, which means TTL is disabled.
         * Specify TTL in the unit of seconds.
         *
         * @param ttlSeconds TTL seconds, value should be 0 or greater
         * @return <code>Builder</code>
         */
        public Builder withTTL(Integer ttlSeconds) {
            if (ttlSeconds == null) {
                throw new IllegalArgumentException("ttlSeconds cannot be null");
            }
            if (ttlSeconds < 0) {
                throw new ParamException("The ttlSeconds value should be 0 or greater");
            }

            return this.withProperty(Constant.TTL_SECONDS, Integer.toString(ttlSeconds));
        }

        /**
         * Enable MMap or not for index data files.
         *
         * @param enabledMMap enabled or not
         * @return <code>Builder</code>
         */
        public Builder withMMapEnabled(boolean enabledMMap) {
            return this.withProperty(Constant.MMAP_ENABLED, Boolean.toString(enabledMMap));
        }

        /**
         * Basic method to set a key-value property.
         *
         * @param key the key
         * @param value the value
         * @return <code>Builder</code>
         */
        public Builder withProperty(String key, String value) {
            if (key == null) {
                throw new IllegalArgumentException("key cannot be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null");
            }
            this.properties.put(key, value);
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

}

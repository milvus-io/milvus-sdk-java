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

package io.milvus.param.index;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for <code>alterIndex</code> interface.
 */
public class AlterIndexParam {
    private final String collectionName;
    private final String databaseName;
    private final String indexName;
    private final Map<String, String> properties = new HashMap<>();

    private AlterIndexParam(Builder builder) {
        // Replace @NonNull logic with explicit null check
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.indexName = builder.indexName;
        this.properties.putAll(builder.properties);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getCollectionName() {
        return collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "AlterIndexParam{" +
                "collectionName='" + collectionName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", properties=" + properties +
                '}';
    }

    /**
     * Builder for {@link AlterIndexParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String databaseName;
        private String indexName;

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
            // Replace @NonNull logic with explicit null check
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
         * Set the index name. Index name cannot be empty or null.
         *
         * @param indexName index name
         * @return <code>Builder</code>
         */
        public Builder withIndexName(String indexName) {
            // Replace @NonNull logic with explicit null check
            if (indexName == null) {
                throw new IllegalArgumentException("indexName cannot be null");
            }
            this.indexName = indexName;
            return this;
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
         * @param key   the key
         * @param value the value
         * @return <code>Builder</code>
         */
        public Builder withProperty(String key, String value) {
            // Replace @NonNull logic with explicit null check
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
         * Verifies parameters and creates a new {@link AlterIndexParam} instance.
         *
         * @return {@link AlterIndexParam}
         */
        public AlterIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(indexName, "Index name");

            return new AlterIndexParam(this);
        }
    }
}

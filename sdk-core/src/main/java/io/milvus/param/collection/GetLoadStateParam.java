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

import com.google.common.collect.Lists;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import java.util.List;

/**
 * Parameters for <code>getLoadState</code> interface.
 */
public class GetLoadStateParam {
    private final String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;

    public GetLoadStateParam(Builder builder) {
        if (builder.collectionName == null) {
            throw new IllegalArgumentException("collectionName cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return "GetLoadStateParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetLoadStateParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;

        private final List<String> partitionNames = Lists.newArrayList();

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
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets partition names list(Optional).
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(List<String> partitionNames) {
            if (partitionNames == null) {
                throw new IllegalArgumentException("partitionNames cannot be null");
            }
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * Adds a partition by name. Partition name cannot be empty or null.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(String partitionName) {
            if (partitionName == null) {
                throw new IllegalArgumentException("partitionName cannot be null");
            }
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetLoadStateParam} instance.
         *
         * @return {@link GetLoadStateParam}
         */
        public GetLoadStateParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetLoadStateParam(this);
        }
    }

}

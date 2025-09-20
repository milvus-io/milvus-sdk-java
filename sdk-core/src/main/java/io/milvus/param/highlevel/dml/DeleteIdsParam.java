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

package io.milvus.param.highlevel.dml;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>delete</code> interface.
 */
public class DeleteIdsParam {
    private final String collectionName;
    private final String partitionName;
    private final List<?> primaryIds;

    private DeleteIdsParam(Builder builder) {
        // Replace @NonNull logic with explicit null check
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.primaryIds = builder.primaryIds;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<?> getPrimaryIds() {
        return primaryIds;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "DeleteIdsParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", primaryIds=" + primaryIds +
                '}';
    }

    /**
     * Builder for {@link DeleteIdsParam} class.
     */
    public static class Builder<T> {
        private String collectionName;
        private String partitionName = "";
        private List<T> primaryIds = new ArrayList<>();

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
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
         * Sets the partition name (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(String partitionName) {
            // Replace @NonNull logic with explicit null check
            if (partitionName == null) {
                throw new IllegalArgumentException("partitionName cannot be null");
            }
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Specifies primaryId fields. PrimaryIds cannot be empty or null.
         *
         * @param primaryIds input primary key list
         * @return <code>Builder</code>
         */
        public Builder withPrimaryIds(List<T> primaryIds) {
            // Replace @NonNull logic with explicit null check
            if (primaryIds == null) {
                throw new IllegalArgumentException("primaryIds cannot be null");
            }
            this.primaryIds.addAll(primaryIds);
            return this;
        }

        /**
         * Specifies primaryId field. PrimaryId cannot be empty or null.
         *
         * @param primaryId input primary key id
         * @return <code>Builder</code>
         */
        public Builder addPrimaryId(T primaryId) {
            // Replace @NonNull logic with explicit null check
            if (primaryId == null) {
                throw new IllegalArgumentException("primaryId cannot be null");
            }
            this.primaryIds.add(primaryId);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DeleteIdsParam} instance.
         *
         * @return {@link DeleteIdsParam}
         */
        public DeleteIdsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            if (CollectionUtils.isEmpty(primaryIds)) {
                throw new ParamException("PrimaryIds cannot be empty");
            }
            return new DeleteIdsParam(this);
        }
    }
}

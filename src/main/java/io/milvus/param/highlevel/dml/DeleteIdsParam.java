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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>delete</code> interface.
 */
@Getter
@ToString
public class DeleteIdsParam {
    private final String collectionName;
    private final String partitionName;
    private final List<?> primaryIds;

    private DeleteIdsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.primaryIds = builder.primaryIds;
    }

    public static Builder newBuilder() {
        return new Builder();
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
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition name (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Specifies primaryId fields. PrimaryIds cannot be empty or null.
         *
         * @param primaryIds input primary key list
         * @return <code>Builder</code>
         */
        public Builder withPrimaryIds(@NonNull List<T> primaryIds) {
            this.primaryIds.addAll(primaryIds);
            return this;
        }

        /**
         * Specifies primaryId field. PrimaryId cannot be empty or null.
         *
         * @param primaryId input primary key id
         * @return <code>Builder</code>
         */
        public Builder addPrimaryId(@NonNull T primaryId) {
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

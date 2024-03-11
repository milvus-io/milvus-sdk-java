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
import io.milvus.grpc.ShowType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>showCollections</code> interface.
 */
@Getter
@ToString
public class ShowCollectionsParam {
    private final List<String> collectionNames;
    private final ShowType showType;
    private final String databaseName;

    private ShowCollectionsParam(@NonNull Builder builder) {
        this.collectionNames = builder.collectionNames;
        this.showType = builder.showType;
        this.databaseName = builder.databaseName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ShowCollectionsParam} class.
     */
    public static final class Builder {
        private final List<String> collectionNames = new ArrayList<>();
        // showType:
        //   default showType = ShowType.All
        //   if collectionNames is not empty, set showType = ShowType.InMemory
        private ShowType showType = ShowType.All;
        private String databaseName;

        private Builder() {
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets a list of collection names. Collection name cannot be empty or null.
         *
         * @param collectionNames list of collection names
         * @return <code>Builder</code>
         */
        public Builder withCollectionNames(@NonNull List<String> collectionNames) {
            collectionNames.forEach(this::addCollectionName);
            return this;
        }

        /**
         * Sets a show type. Show Type can be empty or null, default value is ShowType.All.
         *
         * @param showType ShowType
         * @return <code>Builder</code>
         */
        public Builder withShowType(ShowType showType) {
            this.showType = showType;
            return this;
        }

        /**
         * Adds a collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder addCollectionName(@NonNull String collectionName) {
            if (!this.collectionNames.contains(collectionName)) {
                this.collectionNames.add(collectionName);
            }
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ShowCollectionsParam} instance.
         *
         * @return {@link ShowCollectionsParam}
         */
        public ShowCollectionsParam build() throws ParamException {
            if (!collectionNames.isEmpty()) {
                for (String collectionName : collectionNames) {
                    ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
                }
                this.showType = ShowType.InMemory;
            }

            return new ShowCollectionsParam(this);
        }
    }

}

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

package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.CollectionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Response of the {@code listCollections} API, holding the collections in a database.
 */
public class ListCollectionsResp {
    private List<String> collectionNames;
    private List<CollectionInfo> collectionInfos;

    private ListCollectionsResp(ListCollectionsRespBuilder builder) {
        this.collectionNames = builder.collectionNames != null ? builder.collectionNames : new ArrayList<>();
        this.collectionInfos = builder.collectionInfos != null ? builder.collectionInfos : new ArrayList<>();
    }

    /**
     * Creates a new builder for {@link ListCollectionsResp}.
     *
     * @return the builder
     */
    public static ListCollectionsRespBuilder builder() {
        return new ListCollectionsRespBuilder();
    }

    // Getters
    /**
     * Returns the names of the collections in the database.
     *
     * @return the collection names
     */
    public List<String> getCollectionNames() {
        return collectionNames;
    }

    /**
     * Returns the information of the collections in the database.
     *
     * @return the collection information
     */
    public List<CollectionInfo> getCollectionInfos() {
        return collectionInfos;
    }

    // Setters
    /**
     * Sets the names of the collections in the database.
     *
     * @param collectionNames the collection names
     */
    public void setCollectionNames(List<String> collectionNames) {
        this.collectionNames = collectionNames;
    }

    /**
     * Sets the information of the collections in the database.
     *
     * @param collectionInfos the collection information
     */
    public void setCollectionInfos(List<CollectionInfo> collectionInfos) {
        this.collectionInfos = collectionInfos;
    }

    @Override
    public String toString() {
        return "ListCollectionsResp{" +
                "collectionNames=" + collectionNames +
                ", collectionInfos=" + collectionInfos +
                '}';
    }

    public static class ListCollectionsRespBuilder {
        private List<String> collectionNames;
        private List<CollectionInfo> collectionInfos;

        /**
         * Sets the names of the collections in the database.
         *
         * @param collectionNames the collection names
         * @return this builder
         */
        public ListCollectionsRespBuilder collectionNames(List<String> collectionNames) {
            this.collectionNames = collectionNames;
            return this;
        }

        /**
         * Sets the information of the collections in the database.
         *
         * @param collectionInfos the collection information
         * @return this builder
         */
        public ListCollectionsRespBuilder collectionInfos(List<CollectionInfo> collectionInfos) {
            this.collectionInfos = collectionInfos;
            return this;
        }

        /**
         * Builds a {@link ListCollectionsResp} with the configured parameters.
         *
         * @return the response
         */
        public ListCollectionsResp build() {
            return new ListCollectionsResp(this);
        }
    }
}

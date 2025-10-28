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

package io.milvus.response;

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.ShowCollectionsResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>showCollections</code> interface.
 */
public class ShowCollResponseWrapper {
    private final ShowCollectionsResponse response;

    public ShowCollResponseWrapper(ShowCollectionsResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("ShowCollectionsResponse cannot be null");
        }
        this.response = response;
    }

    public List<String> getCollectionNames() {
        return response.getCollectionNamesList();
    }

    /**
     * Get information of the collections.
     *
     * @return List of CollectionInfo, information array of the collections
     */
    public List<CollectionInfo> getCollectionsInfo() throws IllegalResponseException {
        if (response.getCollectionNamesCount() != response.getCollectionIdsCount()
            || response.getCollectionNamesCount() != response.getCreatedUtcTimestampsCount()) {
            throw new IllegalResponseException("Collection information count doesn't match");
        }

        List<CollectionInfo> results = new ArrayList<>();
        for (int i = 0; i < response.getCollectionNamesCount(); ++i) {
            CollectionInfo info = new CollectionInfo(response.getCollectionNames(i), response.getCollectionIds(i),
                    response.getCreatedUtcTimestamps(i));
            if (response.getInMemoryPercentagesCount() > i) {
                info.setInMemoryPercentage(response.getInMemoryPercentages(i));
            }
            results.add(info);
        }

        return results;
    }

    /**
     * Get information of one collection by name.
     *
     * @param collectionName collection name to get information
     * @return {@link CollectionInfo} information of the collection
     */
    public CollectionInfo getCollectionInfoByName(String collectionName) {
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }
        for (int i = 0; i < response.getCollectionNamesCount(); ++i) {
            if (collectionName.compareTo(response.getCollectionNames(i)) == 0) {
                CollectionInfo info = new CollectionInfo(response.getCollectionNames(i), response.getCollectionIds(i),
                        response.getCreatedUtcTimestamps(i));
                if (response.getInMemoryPercentagesCount() > i) {
                    info.setInMemoryPercentage(response.getInMemoryPercentages(i));
                }
                return info;
            }
        }

        return null;
    }

    /**
     * Internal-use class to wrap response of <code>showCollections</code> interface.
     */
    public static final class CollectionInfo {
        private final String name;
        private final long id;
        private final long utcTimestamp;
        private long inMemoryPercentage = 0;

        public CollectionInfo(String name, long id, long utcTimestamp) {
            this.name = name;
            this.id = id;
            this.utcTimestamp = utcTimestamp;
        }

        public void setInMemoryPercentage(long inMemoryPercentage) {
            this.inMemoryPercentage = inMemoryPercentage;
        }

        // Getter methods
        public String getName() {
            return name;
        }

        public long getId() {
            return id;
        }

        public long getUtcTimestamp() {
            return utcTimestamp;
        }

        public long getInMemoryPercentage() {
            return inMemoryPercentage;
        }

        @Override
        public String toString() {
            return "(name: " + getName() + " id: " + getId() + " utcTimestamp: " + getUtcTimestamp() +
                    " inMemoryPercentage: " + getInMemoryPercentage() + ")";
        }
    }

    /**
     * Construct a <code>String</code> by {@link ShowCollResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Collections{" +
                getCollectionsInfo().toString() +
                '}';
    }
}

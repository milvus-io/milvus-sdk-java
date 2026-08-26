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

package io.milvus.common.utils.cache;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Immutable cache key that uniquely identifies a collection across the client pool and iterator
 * caches. The key is composed of a normalized endpoint, database name and collection name.
 *
 * <p>An empty database name is normalized to {@code "default"}, and the endpoint is normalized to a
 * lower-case {@code host:port} form so that the same server address expressed in different ways maps
 * to the same key.
 */
public final class CollectionCacheKey {
    private final String endpoint;
    private final String databaseName;
    private final String collectionName;

    private CollectionCacheKey(String endpoint, String databaseName, String collectionName) {
        this.endpoint = normalizeEndpoint(endpoint);
        this.databaseName = databaseName == null || databaseName.isEmpty() ? "default" : databaseName;
        this.collectionName = collectionName == null ? "" : collectionName;
    }

    /**
     * Creates a cache key for the given endpoint, database name and collection name.
     *
     * @param endpoint       the server endpoint, such as {@code "localhost:19530"}
     * @param databaseName   the database name; empty is normalized to {@code "default"}
     * @param collectionName the collection name
     * @return the cache key
     */
    public static CollectionCacheKey create(String endpoint, String databaseName, String collectionName) {
        return new CollectionCacheKey(endpoint, databaseName, collectionName);
    }

    /**
     * Returns the normalized endpoint of the key.
     *
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Returns the database name of the key.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the collection name of the key.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            return "";
        }

        String value = endpoint.trim();
        String uriValue = value.contains("://") ? value : "http://" + value;
        try {
            URI uri = new URI(uriValue);
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return value;
            }
            int defaultPort = "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 19530;
            int port = uri.getPort() > 0 ? uri.getPort() : defaultPort;
            if (host.contains(":") && !host.startsWith("[")) {
                host = "[" + host + "]";
            }
            return host.toLowerCase(java.util.Locale.ROOT) + ":" + port;
        } catch (URISyntaxException ignored) {
            return value;
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CollectionCacheKey)) {
            return false;
        }
        CollectionCacheKey that = (CollectionCacheKey) object;
        return endpoint.equals(that.endpoint)
                && databaseName.equals(that.databaseName)
                && collectionName.equals(that.collectionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, databaseName, collectionName);
    }
}

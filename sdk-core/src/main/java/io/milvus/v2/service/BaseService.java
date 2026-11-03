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

package io.milvus.v2.service;

import io.milvus.grpc.BoolResponse;
import io.milvus.grpc.HasCollectionRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.ConvertUtils;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.RpcUtils;
import io.milvus.v2.utils.VectorUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base service for the Milvus v2 clients. Provides shared utilities and cache management
 * for the domain-specific service classes.
 */
public class BaseService {
    protected static final Logger logger = LoggerFactory.getLogger(BaseService.class);
    public RpcUtils rpcUtils = new RpcUtils();
    public DataUtils dataUtils = new DataUtils();
    public VectorUtils vectorUtils = new VectorUtils();
    public ConvertUtils convertUtils = new ConvertUtils();
    private String currentDbName;
    private String endpoint = "";

    /**
     * Sets the current database name. The name is propagated to the vector utilities so
     * that schema and timestamp caches are keyed correctly.
     *
     * @param dbName the database name
     */
    public void setCurrentDbName(String dbName) {
        currentDbName = dbName;
        this.vectorUtils.setCurrentDbName(dbName);
    }

    /**
     * Sets the server endpoint used to key schema and timestamp caches.
     *
     * @param endpoint the server endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint == null ? "" : endpoint;
        this.vectorUtils.setEndpoint(this.endpoint);
    }

    protected String getEndpoint() {
        return endpoint;
    }

    protected void invalidateSchemaCache(String databaseName, String collectionName) {
        SchemaCache.getInstance().invalidate(endpoint, actualDbName(databaseName), collectionName);
    }

    protected void invalidateTsCache(String databaseName, String collectionName) {
        CollectionTsCache.getInstance().invalidate(endpoint, actualDbName(databaseName), collectionName);
    }

    protected void updateTsCache(String databaseName, String collectionName, long timestamp) {
        CollectionTsCache.getInstance().set(
                endpoint, actualDbName(databaseName), collectionName, timestamp);
    }

    protected String actualDbName(String overwriteName) {
        if (StringUtils.isNotEmpty(overwriteName)) {
            return overwriteName;
        }
        return StringUtils.defaultString(currentDbName);
    }

    protected void checkCollectionExist(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String collectionName) {
        HasCollectionRequest request = HasCollectionRequest.newBuilder().setCollectionName(collectionName).build();
        BoolResponse result = blockingStub.hasCollection(request);
        rpcUtils.handleResponse("", result.getStatus());
        if (!result.getValue() == Boolean.TRUE) {
            logger.error("Collection not found");
            throw new MilvusClientException(ErrorCode.COLLECTION_NOT_FOUND, "Collection not found");
        }
    }
}

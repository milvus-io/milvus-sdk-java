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
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.ConvertUtils;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.RpcUtils;
import io.milvus.v2.utils.VectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseService {
    protected static final Logger logger = LoggerFactory.getLogger(BaseService.class);
    public RpcUtils rpcUtils = new RpcUtils();
    public DataUtils dataUtils = new DataUtils();
    public VectorUtils vectorUtils = new VectorUtils();
    public ConvertUtils convertUtils = new ConvertUtils();

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

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

package io.milvus.v2.service.index;

import io.milvus.v2.BaseTest;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class IndexTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(IndexTest.class);

    @Test
    void testCreateIndex() {
        // vector index
        IndexParam indexParam = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("vector")
                .build();
        // scalar index
        IndexParam scalarIndexParam = IndexParam.builder()
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("age")
                .build();
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParam);
        indexParams.add(scalarIndexParam);
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(indexParams)
                .build();
        client_v2.createIndex(createIndexReq);
    }

    @Test
    void testDescribeIndex() {
        DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        DescribeIndexResp responseR = client_v2.describeIndex(describeIndexReq);
        logger.info(responseR.toString());
    }

    @Test
    void testDropIndex() {
        DropIndexReq dropIndexReq = DropIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        client_v2.dropIndex(dropIndexReq);
    }

    @Test
    void testListIndexes() {
        ListIndexesReq listIndexesReq = ListIndexesReq.builder()
                .collectionName("test")
                .build();
        List<String> indexNames = client_v2.listIndexes(listIndexesReq);
        logger.info(indexNames.toString());
    }
}
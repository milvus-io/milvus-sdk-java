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

package io.milvus.v2.service.collection;

import io.milvus.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollectionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Test
    void testListCollections() {
        ListCollectionsResp a = client_v2.listCollections();
    }

    @Test
    void testCreateCollection() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        client_v2.createCollection(req);
    }

    @Test
    void testEnableDynamicSchema() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .enableDynamicField(false)
                .build();
        Assertions.assertFalse(req.getEnableDynamicField());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build());

        req = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .build();
        Assertions.assertTrue(req.getEnableDynamicField());
        Assertions.assertTrue(req.getCollectionSchema().isEnableDynamicField());

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .enableDynamicField(false)
                .collectionSchema(collectionSchema)
                .build()
        );

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .enableDynamicField(false)
                .build()
        );
    }

    @Test
    void testCreateCollectionWithSchema() {

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build())
                .addField(AddFieldReq.builder().fieldName("description").dataType(DataType.VarChar).maxLength(64).build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .build();
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName("description")
                .indexType(IndexParam.IndexType.INVERTED)
                .build();


        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .indexParams(Arrays.asList(indexParam, indexParam2))
                .indexParam(IndexParam.builder() // fluent api, add index param
                        .fieldName("id")
                        .indexType(IndexParam.IndexType.INVERTED)
                        .build()
                )
                .build();
        client_v2.createCollection(request);

        AlterCollectionReq req = AlterCollectionReq.builder()
                .collectionName("test")
                .property("prop", "val")
                .build();
        assertEquals("val", req.getProperties().get("prop"));
    }

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.dropCollection(req);
    }

    @Test
    void testTruncateCollection() {
        TruncateCollectionReq req = TruncateCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.truncateCollection(req);
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.hasCollection(req);
    }

    @Test
    void testDescribeCollection() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionById() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionByNameAndId() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testBatchDescribeCollectionsByNames() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Arrays.asList("test", "test2"))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionIds(Arrays.asList(123456L, 789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByNamesAndIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Collections.singletonList("test"))
                .collectionIds(Collections.singletonList(789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testRenameCollection() {
        RenameCollectionReq req = RenameCollectionReq.builder()
                .collectionName("test2")
                .newCollectionName("test")
                .build();
        client_v2.renameCollection(req);
    }

    @Test
    void testLoadCollection() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.loadCollection(req);

    }

    @Test
    void testReleaseCollection() {
        ReleaseCollectionReq req = ReleaseCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.releaseCollection(req);
    }

    @Test
    void testGetLoadState() {
        GetLoadStateReq req = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.getLoadState(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testGetCollectionStats() {
        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
    }

}
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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class IndexDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testIndex() {
        String randomCollectionName = generator.generate(10);

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String, Object> extra = new HashMap<>();
        extra.put("M", 8);
        extra.put("efConstruction", 64);
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexName("abc")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extra)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("name")
                .indexType(IndexParam.IndexType.TRIE)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .property(Constant.TTL_SECONDS, "5")
                .property(Constant.MMAP_ENABLED, "false")
                .build());

        DescribeCollectionResp descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Map<String, String> collProps = descCollResp.getProperties();
        Assertions.assertTrue(collProps.containsKey(Constant.TTL_SECONDS));
        Assertions.assertTrue(collProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("5", collProps.get(Constant.TTL_SECONDS));
        Assertions.assertEquals("false", collProps.get(Constant.MMAP_ENABLED));

        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        // alter field properties
        client.alterCollectionField(AlterCollectionFieldReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("name")
                .property("max_length", "9")
                .build());

        // collection alter properties
        Map<String, String> properties = new HashMap<>();
        properties.put(Constant.TTL_SECONDS, "10");
        properties.put(Constant.MMAP_ENABLED, "true");
        client.alterCollectionProperties(AlterCollectionPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .properties(properties)
                .property("prop", "val")
                .build());
        descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        collProps = descCollResp.getProperties();
        Assertions.assertTrue(collProps.containsKey(Constant.TTL_SECONDS));
        Assertions.assertTrue(collProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertTrue(collProps.containsKey("prop"));
        Assertions.assertEquals("10", collProps.get(Constant.TTL_SECONDS));
        Assertions.assertEquals("true", collProps.get(Constant.MMAP_ENABLED));
        Assertions.assertEquals("val", collProps.get("prop"));

        CreateCollectionReq.FieldSchema fieldScheam = descCollResp.getCollectionSchema().getField("name");
        Assertions.assertEquals(9, fieldScheam.getMaxLength());

        client.dropCollectionProperties(DropCollectionPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .propertyKeys(Collections.singletonList("prop"))
                .build());
        descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());
        collProps = descCollResp.getProperties();
        Assertions.assertFalse(collProps.containsKey("prop"));

        // list indexes
        List<String> names = client.listIndexes(ListIndexesReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        Assertions.assertEquals(1, names.size());
        Assertions.assertEquals("abc", names.get(0));

        names = client.listIndexes(ListIndexesReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(2, names.size());

        // describe scalar index
        DescribeIndexResp descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("name")
                .build());
        DescribeIndexResp.IndexDesc desc = descResp.getIndexDescByFieldName("name");
        Assertions.assertEquals(IndexParam.IndexType.TRIE, desc.getIndexType());

        // index alter properties
        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertFalse(desc.getIndexName().isEmpty());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Map<String, String> extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey("M"));
        Assertions.assertEquals("8", extraParams.get("M"));
        Assertions.assertTrue(extraParams.containsKey("efConstruction"));
        Assertions.assertEquals("64", extraParams.get("efConstruction"));

        properties.clear();
        properties.put(Constant.MMAP_ENABLED, "false");
        client.alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .indexName(desc.getIndexName())
                .properties(properties)
                .build());

        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        Map<String, String> indexProps = desc.getProperties();
        Assertions.assertTrue(indexProps.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("false", indexProps.get(Constant.MMAP_ENABLED));
        extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey(Constant.MMAP_ENABLED));
        Assertions.assertEquals("false", extraParams.get(Constant.MMAP_ENABLED));

        client.dropIndexProperties(DropIndexPropertiesReq.builder()
                .collectionName(randomCollectionName)
                .indexName(desc.getIndexName())
                .propertyKeys(Collections.singletonList(Constant.MMAP_ENABLED))
                .build());
        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());
        desc = descResp.getIndexDescByFieldName("vector");
        indexProps = desc.getProperties();
        Assertions.assertFalse(indexProps.containsKey(Constant.MMAP_ENABLED));
        extraParams = desc.getExtraParams();
        Assertions.assertFalse(extraParams.containsKey(Constant.MMAP_ENABLED));

        // drop index
        client.dropIndex(DropIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexName("abc")
                .build());

        IndexParam param = IndexParam.builder()
                .fieldName("vector")
                .indexName("XXX")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extra)
                .build();

        client.createIndex(CreateIndexReq.builder()
                .collectionName(randomCollectionName)
                .indexParams(Collections.singletonList(param))
                .build());

        descResp = client.describeIndex(DescribeIndexReq.builder()
                .collectionName(randomCollectionName)
                .fieldName("vector")
                .build());

        desc = descResp.getIndexDescByFieldName("vector");
        Assertions.assertEquals("vector", desc.getFieldName());
        Assertions.assertEquals("XXX", desc.getIndexName());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, desc.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.COSINE, desc.getMetricType());
        extraParams = desc.getExtraParams();
        Assertions.assertTrue(extraParams.containsKey("M"));
        Assertions.assertEquals("8", extraParams.get("M"));
        Assertions.assertTrue(extraParams.containsKey("efConstruction"));
        Assertions.assertEquals("64", extraParams.get("efConstruction"));
    }

}

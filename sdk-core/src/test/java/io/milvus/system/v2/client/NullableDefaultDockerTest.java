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
import com.google.common.collect.Lists;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class NullableDefaultDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testNullableAndDefaultValue() {
        String randomCollectionName = generator.generate(10);
        int dim = 4;

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(dim)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("flag")
                .dataType(DataType.Int32)
                .isNullable(true)
                .defaultValue(10)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("desc")
                .dataType(DataType.VarChar)
                .isNullable(Boolean.TRUE)
                .maxLength(100)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .isNullable(Boolean.TRUE)
                .maxCapacity(100)
                .build());

        Assertions.assertThrows(MilvusClientException.class, () ->
                collectionSchema.addField(AddFieldReq.builder()
                        .fieldName("illegal")
                        .dataType(DataType.Bool)
                        .isNullable(false)
                        .defaultValue(null)
                        .build())
        );

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // insert by row-based
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            List<Float> vector = utils.generateFloatVector(dim);
            row.addProperty("id", i);
            row.add("vector", JsonUtils.toJsonTree(vector));
            if (i % 2 == 0) {
                row.addProperty("flag", i);
                row.add("desc", JsonNull.INSTANCE);
            } else {
                if (i == 5) {
                    row.add("flag", JsonNull.INSTANCE); // both null or unset will use the default value
                }
                row.addProperty("desc", "AAA");

                List<Integer> arr = Arrays.asList(5, 6);
                row.add("arr", JsonUtils.toJsonTree(arr));
            }

            data.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(10, insertResp.getInsertCnt());

        Function<Map<String, Object>, Void> checkFunc =
                entity -> {
                    long id = (long) entity.get("id");
                    if (id % 2 == 0) {
                        Assertions.assertEquals((int) id, entity.get("flag"));
                        Assertions.assertNull(entity.get("desc"));
                        Assertions.assertNull(entity.get("arr"));
                    } else {
                        Assertions.assertEquals(10, entity.get("flag"));
                        Assertions.assertEquals("AAA", entity.get("desc"));
                        Object obj = entity.get("arr");
                        Assertions.assertInstanceOf(List.class, obj);
                        List<Integer> arr = (List<Integer>) obj;
                        Assertions.assertEquals(2, arr.size());
                        Assertions.assertEquals(5, arr.get(0));
                        Assertions.assertEquals(6, arr.get(1));
                    }
                    return null;
                };
        // query
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(randomCollectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("desc", "flag", "arr"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        Assertions.assertEquals(10, queryResults.size());
        System.out.println("Query results:");
        for (QueryResp.QueryResult result : queryResults) {
            Map<String, Object> entity = result.getEntity();
            checkFunc.apply(entity);
            System.out.println(result);
        }

        // search
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(randomCollectionName)
                .annsField("vector")
                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                .limit(10)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        Assertions.assertEquals(1, searchResults.size());
        List<SearchResp.SearchResult> firstResults = searchResults.get(0);
        Assertions.assertEquals(10, firstResults.size());
//        System.out.println("Search results:");
        for (SearchResp.SearchResult result : firstResults) {
            long id = (long) result.getId();
            Map<String, Object> entity = result.getEntity();
            checkFunc.apply(entity);
//            System.out.println(result);
        }
    }

}

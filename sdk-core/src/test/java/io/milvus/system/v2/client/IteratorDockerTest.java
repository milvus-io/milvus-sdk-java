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
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class IteratorDockerTest extends MilvusV2DockerTestBase {
    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .dimension(DIMENSION)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bfloat16_vector")
                .dataType(DataType.BFloat16Vector)
                .dimension(DIMENSION)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(new HashMap<String, Object>() {{
                    put("drop_ratio_build", 0.1);
                }})
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("bfloat16_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 20000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        // get row count
        long rowCount = getRowCount("", randomCollectionName);
        Assertions.assertEquals(count, rowCount);

        // set rpc timeout for each call
        client.withTimeout(1000, TimeUnit.MILLISECONDS);

        // search iterator
        SearchIterator searchIterator = client.searchIterator(SearchIteratorReq.builder()
                .collectionName(randomCollectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(1L)
                .vectorFieldName("float_vector")
                .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .expr("int64_field > 500 && int64_field < 1000")
                .params("{\"range_filter\": 5.0, \"radius\": 50.0}")
                .limit(1000)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());

        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Float.class, record.get("score"));
                Assertions.assertTrue((float) record.get("score") >= 5.0);
                Assertions.assertTrue((float) record.get("score") <= 50.0);

                Assertions.assertInstanceOf(Boolean.class, record.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, record.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, record.get("float_field"));
                Assertions.assertInstanceOf(Double.class, record.get("double_field"));
                Assertions.assertInstanceOf(String.class, record.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, record.get("json_field"));
                Assertions.assertInstanceOf(List.class, record.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, record.get("float_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, record.get("sparse_vector"));

                long int64Val = (long) record.get("int64_field");
                Assertions.assertTrue(int64Val > 500L && int64Val < 1000L);

                String varcharVal = (String) record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject) record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>) record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>) record.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer) record.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit() * 8);

                ByteBuffer bfloat16Vector = (ByteBuffer) record.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION * 2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>) record.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        System.out.printf("There are %d items match score between [5.0, 50.0]%n", counter);
        Assertions.assertTrue(counter > 0);

        // query iterator
        long from = 17777;
        long to = 18000;
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(randomCollectionName)
                .expr("int64_field < " + to)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(1L)
                .offset(from)
                .limit(4000)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());

        counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.printf("query iteration finished, close, %d items fetched%n", counter);
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Long.class, record.get("id"));
                Assertions.assertInstanceOf(Boolean.class, record.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, record.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, record.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, record.get("float_field"));
                Assertions.assertInstanceOf(Double.class, record.get("double_field"));
                Assertions.assertInstanceOf(String.class, record.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, record.get("json_field"));
                Assertions.assertInstanceOf(List.class, record.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, record.get("float_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, record.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, record.get("sparse_vector"));

                long int64Val = (long) record.get("id");
                Assertions.assertTrue(int64Val >= from);
                Assertions.assertTrue(int64Val < to);

                String varcharVal = (String) record.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                JsonObject jsonObj = (JsonObject) record.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>) record.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>) record.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer) record.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit() * 8);

                ByteBuffer bfloat16Vector = (ByteBuffer) record.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION * 2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>) record.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        Assertions.assertEquals(to - from, counter);

        // search iterator V2
        SearchIteratorV2 searchIteratorV2 = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(randomCollectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(100L)
                .vectorFieldName("float_vector")
                .filter("id >= 50")
                .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector())))
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .build());
        counter = 0;
        while (true) {
            List<SearchResp.SearchResult> res = searchIteratorV2.next();
            if (res.isEmpty()) {
                System.out.printf("search iteration finished, close, %d items fetched%n", counter);
                searchIteratorV2.close();
                break;
            }

            for (SearchResp.SearchResult record : res) {
                Map<String, Object> entity = record.getEntity();
                Assertions.assertInstanceOf(Boolean.class, entity.get("bool_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int8_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int16_field"));
                Assertions.assertInstanceOf(Integer.class, entity.get("int32_field"));
                Assertions.assertInstanceOf(Long.class, entity.get("int64_field"));
                Assertions.assertInstanceOf(Float.class, entity.get("float_field"));
                Assertions.assertInstanceOf(Double.class, entity.get("double_field"));
                Assertions.assertInstanceOf(String.class, entity.get("varchar_field"));
                Assertions.assertInstanceOf(JsonObject.class, entity.get("json_field"));
                Assertions.assertInstanceOf(List.class, entity.get("arr_int_field"));
                Assertions.assertInstanceOf(List.class, entity.get("float_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, entity.get("binary_vector"));
                Assertions.assertInstanceOf(ByteBuffer.class, entity.get("bfloat16_vector"));
                Assertions.assertInstanceOf(SortedMap.class, entity.get("sparse_vector"));

                String varcharVal = (String) entity.get("varchar_field");
                Assertions.assertTrue(varcharVal.startsWith("varchar_"));

                long int64Val = (long) entity.get("int64_field");
                Assertions.assertEquals(int64Val, (long) record.getId());
                JsonObject jsonObj = (JsonObject) entity.get("json_field");
                Assertions.assertTrue(jsonObj.has(String.format("JSON_%d", int64Val)));

                List<Integer> intArr = (List<Integer>) entity.get("arr_int_field");
                Assertions.assertTrue(intArr.size() <= 50); // max capacity 50 is defined in the baseSchema()

                List<Float> floatVector = (List<Float>) entity.get("float_vector");
                Assertions.assertEquals(DIMENSION, floatVector.size());

                ByteBuffer binaryVector = (ByteBuffer) entity.get("binary_vector");
                Assertions.assertEquals(DIMENSION, binaryVector.limit() * 8);

                ByteBuffer bfloat16Vector = (ByteBuffer) entity.get("bfloat16_vector");
                Assertions.assertEquals(DIMENSION * 2, bfloat16Vector.limit());

                SortedMap<Long, Float> sparseVector = (SortedMap<Long, Float>) entity.get("sparse_vector");
                Assertions.assertTrue(sparseVector.size() >= 10 && sparseVector.size() < 20); // defined in generateSparseVector()

                counter++;
            }
        }
        // search iterator could not ensure that all the entities can be retrieved
        // expect count is 9950, but sometimes it returns 9949 or 9948
        Assertions.assertTrue(counter > ((int) count - 55) && counter <= ((int) count - 50));

        // reset rpc timeout to unlimited
        client.withTimeout(0, TimeUnit.MILLISECONDS);

        client.dropCollection(DropCollectionReq.builder().collectionName(randomCollectionName).build());
    }

}

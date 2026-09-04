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

package io.milvus.system.v1.client;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

@Tag("system")
class IteratorDockerTest extends MilvusV1DockerTestBase {

    @Test
    public void testIterator() {
        String randomCollectionName = generator.generate(10);

        CollectionSchemaParam schema = buildSchema(true, false, true,
                Arrays.asList(DataType.FloatVector, DataType.JSON));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data
        int rowCount = 1000;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", Long.toString(i));
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(utils.generateFloatVectors(1).get(0)));
            JsonObject json = new JsonObject();
            if (i % 2 == 0) {
                json.addProperty("even", true);
            }
            row.add(DataType.JSON.name(), json);
            row.addProperty("dynamic", i);
            rows.add(row);
        }

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query iterator
        QueryIteratorParam.Builder queryIteratorParamBuilder = QueryIteratorParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("dynamic < 300")
                .withOutFields(Lists.newArrayList("*"))
                .withBatchSize(100L)
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED);

        R<QueryIterator> qResponse = client.queryIterator(queryIteratorParamBuilder.build());
        Assertions.assertEquals(R.Status.Success.getCode(), qResponse.getStatus().intValue());

        QueryIterator queryIterator = qResponse.getData();
        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Long.class, record.get("dynamic"));
                Assertions.assertInstanceOf(String.class, record.get("id"));
                Object vec = record.get(DataType.FloatVector.name());
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>) vec;
                Assertions.assertEquals(DIMENSION, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(DataType.JSON.name()));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(300, counter);

        // search iterator
        List<List<Float>> vectors = utils.generateFloatVectors(1);
        SearchIteratorParam.Builder searchIteratorParamBuilder = SearchIteratorParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withOutFields(Lists.newArrayList("*"))
                .withBatchSize(10L)
                .withVectorFieldName(DataType.FloatVector.name())
                .withFloatVectors(vectors)
                .withLimit(50L)
                .withMetricType(MetricType.L2);

        R<SearchIterator> sResponse = client.searchIterator(searchIteratorParamBuilder.build());
        Assertions.assertEquals(R.Status.Success.getCode(), sResponse.getStatus().intValue());

        SearchIterator searchIterator = sResponse.getData();
        counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                Assertions.assertInstanceOf(Long.class, record.get("dynamic"));
                Assertions.assertInstanceOf(String.class, record.get("id"));
                Object vec = record.get(DataType.FloatVector.name());
                Assertions.assertInstanceOf(List.class, vec);
                List<Float> vector = (List<Float>) vec;
                Assertions.assertEquals(DIMENSION, vector.size());
                Assertions.assertInstanceOf(JsonElement.class, record.get(DataType.JSON.name()));
//                System.out.println(record);
                counter++;
            }
        }
        Assertions.assertEquals(50, counter);
    }
}

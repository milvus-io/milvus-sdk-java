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

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tag("system")
class NullableDefaultDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testNullableAndDefaultValue() {
        String randomCollectionName = generator.generate(10);

        CollectionSchemaParam.Builder builder = CollectionSchemaParam.newBuilder();
        builder.addFieldType(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName("id")
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName("vector")
                .withDimension(DIMENSION)
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.Int32)
                .withName("flag")
                .withMaxLength(100)
                .withDefaultValue(10)
                .build());
        builder.addFieldType(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName("desc")
                .withMaxLength(100)
                .withNullable(true)
                .build());
        R<RpcStatus> createR = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(builder.build())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index on scalar field
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName("vector")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        // insert by row-based
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            List<Float> vector = utils.generateFloatVector();
            row.addProperty("id", i);
            row.add("vector", JsonUtils.toJsonTree(vector));
            if (i % 2 == 0) {
                row.addProperty("flag", i);
                row.add("desc", JsonNull.INSTANCE);
            } else {
                row.addProperty("desc", "AAA");
            }
            data.add(row);
        }

        R<MutationResult> insertR = client.insert(InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(data)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // insert by column-based
        List<List<Float>> vectors = utils.generateFloatVectors(10);
        List<Long> ids = new ArrayList<>();
        List<Integer> flags = new ArrayList<>();
        List<String> descs = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            ids.add((long) i);
            if (i % 2 == 0) {
                flags.add(i);
                descs.add(null);
            } else {
                flags.add(null);
                descs.add("AAA");
            }

        }
        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field("id", ids));
        fieldsInsert.add(new InsertParam.Field("vector", vectors));
        fieldsInsert.add(new InsertParam.Field("flag", flags));
        fieldsInsert.add(new InsertParam.Field("desc", descs));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        // query
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id >= 0")
                .addOutField("flag")
                .addOutField("desc")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results:");
        for (QueryResultsWrapper.RowRecord record : records) {
            long id = (long) record.get("id");
            if (id % 2 == 0) {
                Assertions.assertEquals((int) id, record.get("flag"));
                Assertions.assertNull(record.get("desc"));
            } else {
                Assertions.assertEquals(10, record.get("flag"));
                Assertions.assertEquals("AAA", record.get("desc"));
            }
            System.out.println(record);
        }

        // search the row-based items
        List<List<Float>> searchVectors = utils.generateFloatVectors(1);
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withLimit(10L)
                .withFloatVectors(searchVectors)
                .withVectorFieldName("vector")
                .withParams("{}")
                .addOutField("flag")
                .addOutField("desc")
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = results.getIDScore(0);
//        System.out.println("Search results:");
        Assertions.assertEquals(10, scores.size());
        for (SearchResultsWrapper.IDScore score : scores) {
            long id = score.getLongID();
            Map<String, Object> fieldValues = score.getFieldValues();
            if (id % 2 == 0) {
                Assertions.assertEquals((int) id, fieldValues.get("flag"));
                Assertions.assertNull(fieldValues.get("desc"));
            } else {
                Assertions.assertEquals(10, fieldValues.get("flag"));
                Assertions.assertEquals("AAA", fieldValues.get("desc"));
            }
//            System.out.println(score);
        }
    }
}

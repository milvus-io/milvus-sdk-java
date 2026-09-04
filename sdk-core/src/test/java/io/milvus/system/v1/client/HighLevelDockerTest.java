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

import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@Tag("system")
class HighLevelDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testHighLevelGet() {
        // collection schema
        String field1Name = "id_field";
        String field2Name = "vector_field";
        FieldType int64PrimaryField = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build();

        FieldType varcharPrimaryField = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withDataType(DataType.VarChar)
                .withName(field1Name)
                .withMaxLength(128)
                .build();

        FieldType vectorField = FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(DIMENSION)
                .build();

        testCollectionHighLevelGet(int64PrimaryField, vectorField);
        testCollectionHighLevelGet(varcharPrimaryField, vectorField);
    }

//    @Test
//    void testHighLevelDelete() {
//        // collection schema
//        String field1Name = "id_field";
//        String field2Name = "vector_field";
//        FieldType int64PrimaryField = FieldType.newBuilder()
//                .withPrimaryKey(true)
//                .withAutoID(false)
//                .withDataType(DataType.Int64)
//                .withName(field1Name)
//                .build();
//
//        FieldType varcharPrimaryField = FieldType.newBuilder()
//                .withPrimaryKey(true)
//                .withDataType(DataType.VarChar)
//                .withName(field1Name)
//                .withMaxLength(128)
//                .build();
//
//        FieldType vectorField = FieldType.newBuilder()
//                .withDataType(DataType.FloatVector)
//                .withName(field2Name)
//                .withDimension(DIMENSION)
//                .build();
//
//        testCollectionHighLevelDelete(int64PrimaryField, vectorField);
//        testCollectionHighLevelDelete(varcharPrimaryField, vectorField);
//    }

    void testCollectionHighLevelGet(FieldType primaryField, FieldType vectorField) {
        // create collection
        String randomCollectionName = generator.generate(10);
        highLevelCreateCollection(primaryField, vectorField, randomCollectionName);

        // insert data
        List<String> primaryIds = new ArrayList<>();
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            if (primaryField.getDataType() == DataType.Int64) {
                row.addProperty(primaryField.getName(), i);
            } else {
                row.addProperty(primaryField.getName(), String.valueOf(i));
            }
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), JsonUtils.toJsonTree(vector));
            rows.add(row);
            primaryIds.add(String.valueOf(i));
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());

        testHighLevelGet(randomCollectionName, primaryIds);
        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    void testCollectionHighLevelDelete(FieldType primaryField, FieldType vectorField) {
        // create collection & buildIndex & loadCollection
        String randomCollectionName = generator.generate(10);
        highLevelCreateCollection(primaryField, vectorField, randomCollectionName);

        // insert data
        List<String> primaryIds = new ArrayList<>();
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            if (primaryField.getDataType() == DataType.Int64) {
                row.addProperty(primaryField.getName(), i);
            } else {
                row.addProperty(primaryField.getName(), String.valueOf(i));
            }
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(vectorField.getName(), JsonUtils.toJsonTree(vector));
            rows.add(row);
            primaryIds.add(String.valueOf(i));
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());

        // high level delete
        testHighLevelDelete(randomCollectionName, primaryIds);
        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }
}

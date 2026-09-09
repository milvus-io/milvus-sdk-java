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

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFunctionFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AddFunctionFieldReqTest {

    private CreateCollectionReq.Function buildFunction() {
        return CreateCollectionReq.Function.builder()
                .name("text_embedding")
                .functionType(io.milvus.common.clientenum.FunctionType.UNKNOWN)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("vector"))
                .build();
    }

    @Test
    void builderBuildsWithAllFields() {
        CreateCollectionReq.Function function = buildFunction();
        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .build();
        AddFunctionFieldReq request = AddFunctionFieldReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(128)
                .function(function)
                .indexParam(indexParam)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("vector", request.getFieldName());
        assertEquals(DataType.FloatVector, request.getDataType());
        assertEquals(128, request.getDimension());
        assertEquals(function, request.getFunction());
        assertEquals(indexParam, request.getIndexParam());
    }

    @Test
    void inheritedDefaultsApply() {
        AddFunctionFieldReq request = AddFunctionFieldReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertNull(request.getFunction());
        assertNull(request.getIndexParam());
        assertFalse(request.getIsPrimaryKey());
    }

    @Test
    void settersUpdateGetters() {
        AddFunctionFieldReq request = AddFunctionFieldReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        CreateCollectionReq.Function function = buildFunction();
        request.setFunction(function);
        assertEquals(function, request.getFunction());

        IndexParam indexParam = IndexParam.builder().fieldName("vector").build();
        request.setIndexParam(indexParam);
        assertEquals(indexParam, request.getIndexParam());
    }

    @Test
    void toStringContainsFields() {
        AddFunctionFieldReq request = AddFunctionFieldReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

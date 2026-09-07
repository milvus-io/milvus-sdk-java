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

import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.service.collection.request.AddCollectionFunctionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AddCollectionFunctionReqTest {

    private CreateCollectionReq.Function buildFunction() {
        return CreateCollectionReq.Function.builder()
                .name("bm25_fn")
                .description("bm25 function")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Arrays.asList("text"))
                .outputFieldNames(Arrays.asList("sparse"))
                .param("k1", "1.0")
                .build();
    }

    @Test
    void builderBuildsWithAllFields() {
        CreateCollectionReq.Function function = buildFunction();
        AddCollectionFunctionReq request = AddCollectionFunctionReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .function(function)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals(function, request.getFunction());
    }

    @Test
    void settersUpdateGetters() {
        AddCollectionFunctionReq request = AddCollectionFunctionReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());
    }

    @Test
    void defaultsAreEmptyStrings() {
        AddCollectionFunctionReq request = AddCollectionFunctionReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertNull(request.getFunction());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(AddCollectionFunctionReq.builder());
        assertNotNull(AddCollectionFunctionReq.builder().build());
    }

    @Test
    void toStringContainsFields() {
        AddCollectionFunctionReq request = AddCollectionFunctionReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

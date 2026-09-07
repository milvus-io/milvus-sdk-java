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

package io.milvus.unit.v2.service.vector.request;

import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainOp;
import io.milvus.v2.service.vector.request.FunctionParamValue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionChainOpTest {

    @Test
    void exposesOperationNameConstants() {
        assertEquals("map", FunctionChainOp.OP_MAP);
        assertEquals("sort", FunctionChainOp.OP_SORT);
        assertEquals("limit", FunctionChainOp.OP_LIMIT);
    }

    @Test
    void builderSetsAllFields() {
        FunctionChainExpr expr = FunctionChainExpr.builder().name("f").build();
        Map<String, FunctionParamValue> params = new HashMap<>();
        params.put("desc", FunctionParamValue.of(true));

        FunctionChainOp op = FunctionChainOp.builder()
                .op(FunctionChainOp.OP_SORT)
                .expr(expr)
                .inputs(Arrays.asList("$score"))
                .outputs(Collections.singletonList("$score"))
                .params(params)
                .build();

        assertEquals(FunctionChainOp.OP_SORT, op.getOp());
        assertEquals(expr, op.getExpr());
        assertEquals(Arrays.asList("$score"), op.getInputs());
        assertEquals(Collections.singletonList("$score"), op.getOutputs());
        assertEquals(params, op.getParams());
    }

    @Test
    void builderDefaults() {
        FunctionChainOp op = FunctionChainOp.builder().op("filter").build();
        assertEquals("filter", op.getOp());
        assertNull(op.getExpr());
        assertTrue(op.getInputs().isEmpty());
        assertTrue(op.getOutputs().isEmpty());
        assertTrue(op.getParams().isEmpty());
    }

    @Test
    void buildRejectsEmptyOpName() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChainOp.builder().build());
        assertThrows(MilvusClientException.class,
                () -> FunctionChainOp.builder().op("").build());
        assertThrows(MilvusClientException.class,
                () -> FunctionChainOp.builder().op(null).build());
    }

    @Test
    void rejectsNullListsAndParams() {
        FunctionChainOp.FunctionChainOpBuilder b = FunctionChainOp.builder().op("op");
        assertThrows(MilvusClientException.class, () -> b.inputs(null));
        assertThrows(MilvusClientException.class, () -> b.outputs(null));
        assertThrows(MilvusClientException.class, () -> b.params(null));
    }

    @Test
    void toGrpcMapsFields() {
        FunctionChainExpr expr = FunctionChainExpr.builder().name("f").build();
        FunctionChainOp op = FunctionChainOp.builder()
                .op("map")
                .expr(expr)
                .inputs(Arrays.asList("a", "b"))
                .outputs(Collections.singletonList("c"))
                .params(Collections.singletonMap("k", FunctionParamValue.of("v")))
                .build();

        io.milvus.grpc.FunctionChainOp grpc = op.toGrpc();
        assertEquals("map", grpc.getOp());
        assertEquals("f", grpc.getExpr().getName());
        assertEquals(2, grpc.getInputsCount());
        assertEquals(1, grpc.getOutputsCount());
        assertEquals("v", grpc.getParamsMap().get("k").getStringValue());
    }
}

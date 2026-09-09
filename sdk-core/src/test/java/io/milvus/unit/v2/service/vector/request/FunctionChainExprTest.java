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
import io.milvus.v2.service.vector.request.FunctionChainArg;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionParamValue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionChainExprTest {

    @Test
    void builderSetsNameArgsAndParams() {
        FunctionChainExpr expr = FunctionChainExpr.builder()
                .name("num_combine")
                .arg(FunctionChainArg.col("$score"))
                .arg(FunctionChainArg.literal(0.5))
                .param("mode", "weighted")
                .param("weights", Arrays.asList(0.7, 0.2, 0.1))
                .build();

        assertEquals("num_combine", expr.getName());
        assertEquals(2, expr.getArgs().size());
        assertEquals("$score", expr.getArgs().get(0).getColumnName());
        assertEquals(2, expr.getParams().size());
        assertNotNull(expr.getParams().get("mode"));
        assertNotNull(expr.getParams().get("weights"));
    }

    @Test
    void builderRejectsNullArg() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().arg(null));
    }

    @Test
    void builderRejectsEmptyParamKey() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().param("", 1));
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().param(null, 1));
    }

    @Test
    void buildRejectsEmptyName() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().build());
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().name("").build());
        assertThrows(MilvusClientException.class,
                () -> FunctionChainExpr.builder().name(null).build());
    }

    @Test
    void toGrpcMapsFields() {
        FunctionChainExpr expr = FunctionChainExpr.builder()
                .name("f")
                .arg(FunctionChainArg.col("$score"))
                .param("desc", true)
                .build();

        io.milvus.grpc.FunctionChainExpr grpc = expr.toGrpc();
        assertEquals("f", grpc.getName());
        assertEquals(1, grpc.getArgsCount());
        assertEquals("$score", grpc.getArgs(0).getColumn().getName());
        assertTrue(grpc.getParamsMap().containsKey("desc"));
        assertEquals(true, grpc.getParamsMap().get("desc").getBoolValue());
    }

    @Test
    void paramsPreserveOrder() {
        FunctionChainExpr expr = FunctionChainExpr.builder()
                .name("f")
                .param("a", 1)
                .param("b", 2)
                .build();
        assertEquals(java.util.Arrays.asList("a", "b"), new java.util.ArrayList<>(expr.getParams().keySet()));
        assertTrue(expr.getParams().get("a") instanceof FunctionParamValue);
    }
}

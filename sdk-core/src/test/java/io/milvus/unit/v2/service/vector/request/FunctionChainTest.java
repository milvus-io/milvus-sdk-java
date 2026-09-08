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
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionChainArg;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainOp;
import io.milvus.v2.service.vector.request.FunctionChainStage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionChainTest {

    @Test
    void builderSetsStageNameAndOps() {
        FunctionChainExpr expr = FunctionChainExpr.builder()
                .name("num_combine")
                .arg(FunctionChainArg.col("$score"))
                .param("mode", "weighted")
                .build();

        FunctionChain chain = FunctionChain.builder()
                .stage(FunctionChainStage.L2_RERANK)
                .name("fresh_popular_rerank")
                .map("$new_score", expr)
                .sort("$score", true, "$id")
                .limit(10)
                .limit(20, 5)
                .build();

        assertEquals(FunctionChainStage.L2_RERANK, chain.getStage());
        assertEquals("fresh_popular_rerank", chain.getName());
        assertEquals(4, chain.getOps().size());
        assertEquals(FunctionChainOp.OP_MAP, chain.getOps().get(0).getOp());
        assertEquals(FunctionChainOp.OP_SORT, chain.getOps().get(1).getOp());
        assertEquals(FunctionChainOp.OP_LIMIT, chain.getOps().get(2).getOp());
        assertEquals(FunctionChainOp.OP_LIMIT, chain.getOps().get(3).getOp());
    }

    @Test
    void builderDefaults() {
        FunctionChain chain = FunctionChain.builder().build();

        assertEquals(FunctionChainStage.UNSPECIFIED, chain.getStage());
        assertEquals("", chain.getName());
        assertTrue(chain.getOps().isEmpty());
    }

    @Test
    void nameNullNormalizesToEmpty() {
        FunctionChain chain = FunctionChain.builder().name(null).build();
        assertEquals("", chain.getName());
    }

    @Test
    void stageNullRejected() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().stage(null));
    }

    @Test
    void mapValidationErrors() {
        FunctionChainExpr expr = FunctionChainExpr.builder().name("f").build();
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map(null, expr));
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().map("out", null));
    }

    @Test
    void sortAndLimitValidationErrors() {
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().sort("", true, "$id"));
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(0));
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(-1, 0));
        assertThrows(MilvusClientException.class,
                () -> FunctionChain.builder().limit(10, -1));
    }

    @Test
    void toGrpcMapsOps() {
        FunctionChain chain = FunctionChain.builder()
                .stage(FunctionChainStage.L1_RERANK)
                .name("chain")
                .limit(10)
                .build();

        io.milvus.grpc.FunctionChain grpc = chain.toGrpc();
        assertEquals("chain", grpc.getName());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageL1Rerank, grpc.getStage());
        assertEquals(1, grpc.getOpsCount());
        assertEquals("limit", grpc.getOps(0).getOp());
        assertNotNull(chain.toString());
    }
}

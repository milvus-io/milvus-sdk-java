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

import io.milvus.v2.service.vector.request.FunctionChainStage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class FunctionChainStageTest {

    @Test
    void enumValuesAndGrpcMapping() {
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageUnspecified,
                FunctionChainStage.UNSPECIFIED.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageIngestion,
                FunctionChainStage.INGESTION.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStagePreProcess,
                FunctionChainStage.PRE_PROCESS.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageL0Rerank,
                FunctionChainStage.L0_RERANK.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageL1Rerank,
                FunctionChainStage.L1_RERANK.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStageL2Rerank,
                FunctionChainStage.L2_RERANK.toGrpc());
        assertEquals(io.milvus.grpc.FunctionChainStage.FunctionChainStagePostProcess,
                FunctionChainStage.POST_PROCESS.toGrpc());
        assertEquals(7, FunctionChainStage.values().length);
    }
}

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
import io.milvus.v2.service.vector.request.FunctionParamValue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FunctionChainArgTest {

    @Test
    void colFactoryProducesColumnArg() {
        FunctionChainArg arg = FunctionChainArg.col("$score");

        assertTrue(arg.isColumn());
        assertEquals("$score", arg.getColumnName());
        assertNull(arg.getLiteral());
    }

    @Test
    void literalFactoryProducesLiteralArg() {
        FunctionChainArg arg = FunctionChainArg.literal(42);

        assertFalse(arg.isColumn());
        assertNull(arg.getColumnName());
        assertNotNull(arg.getLiteral());
        assertTrue(arg.getLiteral() instanceof FunctionParamValue);
        assertEquals(42L, arg.getLiteral().toGrpc().getInt64Value());
    }

    @Test
    void colRejectsEmptyName() {
        assertThrows(MilvusClientException.class, () -> FunctionChainArg.col(""));
        assertThrows(MilvusClientException.class, () -> FunctionChainArg.col(null));
    }

    @Test
    void toGrpcColumnSetsColumnName() {
        io.milvus.grpc.FunctionChainExprArg grpc = FunctionChainArg.col("$id").toGrpc();
        assertTrue(grpc.hasColumn());
        assertEquals("$id", grpc.getColumn().getName());
    }

    @Test
    void toGrpcLiteralSetsLiteral() {
        io.milvus.grpc.FunctionChainExprArg grpc = FunctionChainArg.literal("hello").toGrpc();
        assertTrue(grpc.hasLiteral());
        assertEquals("hello", grpc.getLiteral().getStringValue());
    }
}

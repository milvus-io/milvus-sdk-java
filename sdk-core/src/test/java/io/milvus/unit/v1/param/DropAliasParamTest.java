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

package io.milvus.unit.v1.param;

import io.milvus.exception.ParamException;
import io.milvus.param.alias.DropAliasParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class DropAliasParamTest {
    @Test
    void buildAndGet() {
        DropAliasParam param = DropAliasParam.newBuilder()
                .withAlias("alias1")
                .withDatabaseName("db1")
                .build();

        assertEquals("alias1", param.getAlias());
        assertEquals("db1", param.getDatabaseName());
    }

    @Test
    void databaseNameIsOptional() {
        DropAliasParam param = DropAliasParam.newBuilder()
                .withAlias("alias1")
                .build();

        assertNull(param.getDatabaseName());
    }

    @Test
    void buildFailsWhenAliasMissing() {
        assertThrows(ParamException.class, () -> DropAliasParam.newBuilder().build());
    }

    @Test
    void withAliasRejectsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                DropAliasParam.newBuilder().withAlias(null));
    }
}

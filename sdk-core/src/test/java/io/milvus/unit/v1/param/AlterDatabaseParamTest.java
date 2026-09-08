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
import io.milvus.param.Constant;
import io.milvus.param.collection.AlterDatabaseParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AlterDatabaseParamTest {

    @Test
    void builderSetsAllFields() {
        AlterDatabaseParam param = AlterDatabaseParam.newBuilder()
                .withDatabaseName("db")
                .withProperty("k1", "v1")
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("v1", param.getProperties().get("k1"));
    }

    @Test
    void withReplicaNumberSetsProperty() {
        AlterDatabaseParam param = AlterDatabaseParam.newBuilder()
                .withDatabaseName("db")
                .withReplicaNumber(3)
                .build();

        assertEquals("3", param.getProperties().get(Constant.DATABASE_REPLICA_NUMBER));
    }

    @Test
    void withResourceGroupsSetsProperty() {
        AlterDatabaseParam param = AlterDatabaseParam.newBuilder()
                .withDatabaseName("db")
                .withResourceGroups(Arrays.asList("rg1", "rg2"))
                .build();

        assertEquals("rg1,rg2", param.getProperties().get(Constant.DATABASE_RESOURCE_GROUPS));
    }

    @Test
    void nullResourceGroupsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterDatabaseParam.newBuilder().withDatabaseName("db").withResourceGroups(null));
    }

    @Test
    void nullPropertyKeyIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterDatabaseParam.newBuilder().withDatabaseName("db").withProperty(null, "v"));
    }

    @Test
    void nullPropertyValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterDatabaseParam.newBuilder().withDatabaseName("db").withProperty("k", null));
    }

    @Test
    void propertiesDefaultToEmpty() {
        AlterDatabaseParam param = AlterDatabaseParam.newBuilder()
                .withDatabaseName("db")
                .build();

        assertTrue(param.getProperties().isEmpty());
    }

    @Test
    void nullDatabaseNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterDatabaseParam.newBuilder().withDatabaseName(null));
    }

    @Test
    void emptyDatabaseNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AlterDatabaseParam.newBuilder().withDatabaseName("").build());
    }

    @Test
    void missingDatabaseNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> AlterDatabaseParam.newBuilder().build());
    }

    @Test
    void propertiesAreCopiedOnConstruction() {
        AlterDatabaseParam param = AlterDatabaseParam.newBuilder()
                .withDatabaseName("db")
                .withProperty("k", "v")
                .build();

        assertNull(param.getProperties().get("missing"));
    }
}

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
import io.milvus.param.index.AlterIndexParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AlterIndexParamTest {

    @Test
    void builderSetsAllFields() {
        AlterIndexParam param = AlterIndexParam.newBuilder()
                .withCollectionName("coll")
                .withDatabaseName("db")
                .withIndexName("idx")
                .withProperty("k", "v")
                .build();

        assertEquals("coll", param.getCollectionName());
        assertEquals("db", param.getDatabaseName());
        assertEquals("idx", param.getIndexName());
        assertEquals("v", param.getProperties().get("k"));
    }

    @Test
    void withMMapEnabledSetsProperty() {
        AlterIndexParam param = AlterIndexParam.newBuilder()
                .withCollectionName("coll")
                .withIndexName("idx")
                .withMMapEnabled(true)
                .build();

        assertEquals("true", param.getProperties().get(Constant.MMAP_ENABLED));
    }

    @Test
    void databaseNameAndPropertiesDefault() {
        AlterIndexParam param = AlterIndexParam.newBuilder()
                .withCollectionName("coll")
                .withIndexName("idx")
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getProperties().isEmpty());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterIndexParam.newBuilder().withCollectionName(null));
    }

    @Test
    void nullIndexNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterIndexParam.newBuilder().withCollectionName("coll").withIndexName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AlterIndexParam.newBuilder()
                        .withCollectionName("")
                        .withIndexName("idx")
                        .build());
    }

    @Test
    void emptyIndexNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AlterIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withIndexName("")
                        .build());
    }

    @Test
    void missingIndexNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> AlterIndexParam.newBuilder().withCollectionName("coll").build());
    }

    @Test
    void nullPropertyKeyIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withIndexName("idx")
                        .withProperty(null, "v"));
    }

    @Test
    void nullPropertyValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AlterIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withIndexName("idx")
                        .withProperty("k", null));
    }
}

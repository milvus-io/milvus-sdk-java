/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.utility;

import io.milvus.v2.service.utility.response.ListAliasResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class ListAliasRespTest {
    @Test
    void builderBuildsAllFields() {
        ListAliasResp response = ListAliasResp.builder()
                .collectionName("coll")
                .alias(Arrays.asList("a1", "a2"))
                .dbName("db")
                .build();

        assertEquals("coll", response.getCollectionName());
        assertEquals(Arrays.asList("a1", "a2"), response.getAlias());
        assertEquals("db", response.getDbName());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        ListAliasResp response = ListAliasResp.builder().build();

        assertNull(response.getCollectionName());
        assertNull(response.getAlias());
        assertNull(response.getDbName());
    }

    @Test
    void settersUpdateFields() {
        ListAliasResp response = ListAliasResp.builder().build();

        response.setCollectionName("coll");
        response.setAlias(Arrays.asList("a"));
        response.setDbName("db");

        assertEquals("coll", response.getCollectionName());
        assertEquals(Arrays.asList("a"), response.getAlias());
        assertEquals("db", response.getDbName());
    }

    @Test
    void toStringContainsFields() {
        ListAliasResp response = ListAliasResp.builder()
                .collectionName("coll")
                .alias(Arrays.asList("a1"))
                .dbName("db")
                .build();

        String text = response.toString();
        assertEquals("ListAliasResp{collectionName='coll', alias=[a1], dbName='db'}", text);
    }
}

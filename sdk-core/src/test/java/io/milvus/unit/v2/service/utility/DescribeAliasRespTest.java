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

import io.milvus.v2.service.utility.response.DescribeAliasResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class DescribeAliasRespTest {
    @Test
    void builderBuildsAllFields() {
        DescribeAliasResp response = DescribeAliasResp.builder()
                .databaseName("db")
                .collectionName("coll")
                .alias("alias")
                .build();

        assertEquals("db", response.getDatabaseName());
        assertEquals("coll", response.getCollectionName());
        assertEquals("alias", response.getAlias());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        DescribeAliasResp response = DescribeAliasResp.builder().build();

        assertNull(response.getDatabaseName());
        assertNull(response.getCollectionName());
        assertNull(response.getAlias());
    }

    @Test
    void settersUpdateFields() {
        DescribeAliasResp response = DescribeAliasResp.builder().build();

        response.setDatabaseName("db");
        response.setCollectionName("coll");
        response.setAlias("alias");

        assertEquals("db", response.getDatabaseName());
        assertEquals("coll", response.getCollectionName());
        assertEquals("alias", response.getAlias());
    }

    @Test
    void toStringContainsFields() {
        DescribeAliasResp response = DescribeAliasResp.builder()
                .databaseName("db")
                .collectionName("coll")
                .alias("alias")
                .build();

        String text = response.toString();
        assertEquals("DescribeAliasResp{databaseName='db', collectionName='coll', alias='alias'}", text);
    }
}

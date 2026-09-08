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

import io.milvus.v2.service.utility.request.DescribeAliasReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class DescribeAliasReqTest {
    @Test
    void builderBuildsAllFields() {
        DescribeAliasReq request = DescribeAliasReq.builder()
                .databaseName("db")
                .alias("alias")
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("alias", request.getAlias());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        DescribeAliasReq request = DescribeAliasReq.builder().build();

        assertNull(request.getDatabaseName());
        assertNull(request.getAlias());
    }

    @Test
    void settersUpdateFields() {
        DescribeAliasReq request = DescribeAliasReq.builder().build();

        request.setDatabaseName("db");
        request.setAlias("alias");

        assertEquals("db", request.getDatabaseName());
        assertEquals("alias", request.getAlias());
    }

    @Test
    void toStringContainsFields() {
        DescribeAliasReq request = DescribeAliasReq.builder()
                .databaseName("db")
                .alias("alias")
                .build();

        String text = request.toString();
        assertEquals("DescribeAliasReq{databaseName='db', alias='alias'}", text);
    }
}

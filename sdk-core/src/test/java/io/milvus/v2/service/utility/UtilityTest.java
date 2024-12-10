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

package io.milvus.v2.service.utility;

import io.milvus.v2.BaseTest;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.DescribeAliasResp;
import io.milvus.v2.service.utility.response.ListAliasResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UtilityTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(UtilityTest.class);

    @Test
    void testCreateAlias() {
        CreateAliasReq req = CreateAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.createAlias(req);
    }

    @Test
    void testDropAlias() {
        DropAliasReq req = DropAliasReq.builder()
                .alias("test_alias")
                .build();
        client_v2.dropAlias(req);
    }
    @Test
    void testAlterAlias() {
        AlterAliasReq req = AlterAliasReq.builder()
                .collectionName("test")
                .alias("test_alias")
                .build();
        client_v2.alterAlias(req);
    }

    @Test
    void describeAlias() {
        DescribeAliasReq req = DescribeAliasReq.builder()
                .alias("test_alias")
                .build();
        DescribeAliasResp statusR = client_v2.describeAlias(req);
    }
    @Test
    void listAliases() {
        ListAliasesReq req = ListAliasesReq.builder()
                .collectionName("test")
                .build();
        ListAliasResp statusR = client_v2.listAliases(req);
    }
}
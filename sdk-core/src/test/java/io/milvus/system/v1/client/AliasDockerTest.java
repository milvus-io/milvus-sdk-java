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

package io.milvus.system.v1.client;

import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.alias.ListAliasesParam;
import io.milvus.param.collection.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

@Tag("system")
class AliasDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testAlias() {
        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection A
        R<RpcStatus> response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_A")
                .withSchema(schema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        // create collection B
        response = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName("coll_B")
                .withSchema(schema)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        // create alias
        response = client.createAlias(CreateAliasParam.newBuilder()
                .withCollectionName("coll_A")
                .withAlias("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        R<Boolean> has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), true);

        R<ListAliasesResponse> listResp = client.listAliases(ListAliasesParam.newBuilder()
                .withCollectionName("coll_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());
        Assertions.assertEquals(listResp.getData().getAliases(0), "alias_A");
        Assertions.assertEquals(listResp.getData().getCollectionName(), "coll_A");

        // alter alias
        response = client.alterAlias(AlterAliasParam.newBuilder()
                .withAlias("alias_A")
                .withCollectionName("coll_B")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());

        has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), true);

        listResp = client.listAliases(ListAliasesParam.newBuilder()
                .withCollectionName("coll_B")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), listResp.getStatus().intValue());
        Assertions.assertEquals(listResp.getData().getAliases(0), "alias_A");
        Assertions.assertEquals(listResp.getData().getCollectionName(), "coll_B");

        // drop alias
        response = client.dropAlias(DropAliasParam.newBuilder()
                .withAlias("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), response.getStatus().intValue());

        has = client.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName("alias_A")
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), has.getStatus().intValue());
        Assertions.assertEquals(has.getData(), false);
    }
}

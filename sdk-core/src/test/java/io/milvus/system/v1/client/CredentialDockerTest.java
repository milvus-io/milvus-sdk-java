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
import io.milvus.param.collection.*;
import io.milvus.param.index.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

@Tag("system")
class CredentialDockerTest extends MilvusV1DockerTestBase {

    // this case can be executed when the milvus image of version 2.1 is published.
    @Test
    void testCredential() {
        String randomCollectionName = generator.generate(10);
        // collection schema
        CollectionSchemaParam schema = buildSchema(false, true, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexType(IndexType.IVF_FLAT)
                .withIndexName("xxx")
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":256}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        client.getIndexState(GetIndexStateParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());

        R<RpcStatus> dropIndexR = client.dropIndex(DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }
}

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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import com.google.gson.JsonObject;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.DescribeReplicasResp;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class ReplicaDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testReplica() {
        String randomCollectionName = generator.generate(10);

        String vectorFieldName = "float_vector";
        CreateCollectionReq.CollectionSchema collectionSchema = baseSchema();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(vectorFieldName)
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorFieldName)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(randomCollectionName)
                .collectionSchema(collectionSchema)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 10000;
        List<JsonObject> data = generateRandomData(collectionSchema, count);
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(randomCollectionName)
                .data(data)
                .build());
        Assertions.assertEquals(count, insertResp.getInsertCnt());

        DescribeCollectionResp descCollResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(randomCollectionName)
                .build());

        DescribeReplicasResp descReplicaResp = client.describeReplicas(DescribeReplicasReq.builder()
                .collectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(1, descReplicaResp.getReplicas().size());
        io.milvus.v2.service.collection.ReplicaInfo info = descReplicaResp.getReplicas().get(0);
        Assertions.assertEquals(descCollResp.getCollectionID(), info.getCollectionID());
        Assertions.assertEquals(1, info.getNodeIDs().size());
        Assertions.assertNotEquals(0L, info.getReplicaID());
        Assertions.assertFalse(info.getResourceGroupName().isEmpty());
        Assertions.assertEquals(1, info.getShardReplicas().size());
        io.milvus.v2.service.collection.ShardReplica replica = info.getShardReplicas().get(0);
        Assertions.assertFalse(replica.getChannelName().isEmpty());
        Assertions.assertFalse(replica.getLeaderAddress().isEmpty());
        Assertions.assertNotEquals(0L, replica.getLeaderID());
    }

}

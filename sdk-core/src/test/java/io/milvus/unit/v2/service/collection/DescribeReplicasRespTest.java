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

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.service.collection.ReplicaInfo;
import io.milvus.v2.service.collection.response.DescribeReplicasResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeReplicasRespTest {

    @Test
    void builderBuildsWithAllFields() {
        List<ReplicaInfo> replicas = Arrays.asList(
                ReplicaInfo.builder().replicaID(1L).build(),
                ReplicaInfo.builder().replicaID(2L).build());
        DescribeReplicasResp response = DescribeReplicasResp.builder()
                .replicas(replicas)
                .build();

        assertEquals(replicas, response.getReplicas());
    }

    @Test
    void setterUpdatesGetter() {
        DescribeReplicasResp response = DescribeReplicasResp.builder().build();
        List<ReplicaInfo> replicas = Arrays.asList(
                ReplicaInfo.builder().replicaID(3L).build());
        response.setReplicas(replicas);
        assertEquals(replicas, response.getReplicas());
    }

    @Test
    void replicasDefaultToEmptyList() {
        DescribeReplicasResp response = DescribeReplicasResp.builder().build();
        assertNotNull(response.getReplicas());
        assertTrue(response.getReplicas().isEmpty());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DescribeReplicasResp.builder());
    }

    @Test
    void toStringContainsFields() {
        DescribeReplicasResp response = DescribeReplicasResp.builder()
                .replicas(Arrays.asList(ReplicaInfo.builder().replicaID(1L).build()))
                .build();
        assertTrue(response.toString().contains("1"));
    }
}

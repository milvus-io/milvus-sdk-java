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

package io.milvus.unit.v2.client.globalcluster;

import io.milvus.v2.client.globalcluster.ClusterCapability;
import io.milvus.v2.client.globalcluster.ClusterInfo;
import io.milvus.v2.client.globalcluster.GlobalTopology;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GlobalTopologyTest {

    @Test
    void gettersReturnConstructorValues() {
        ClusterInfo cluster = new ClusterInfo("c1", "host:19530", ClusterCapability.READABLE);
        GlobalTopology topology = new GlobalTopology(7L, Collections.singletonList(cluster));

        assertEquals(7L, topology.getVersion());
        assertEquals(Collections.singletonList(cluster), topology.getClusters());
    }

    @Test
    void getPrimaryReturnsWritableCluster() {
        ClusterInfo readOnly = new ClusterInfo("c1", "host:19530", ClusterCapability.READABLE);
        ClusterInfo primary = new ClusterInfo("c2", "host2:19530", ClusterCapability.PRIMARY);
        GlobalTopology topology = new GlobalTopology(1L, Arrays.asList(readOnly, primary));

        assertSame(primary, topology.getPrimary());
    }

    @Test
    void getPrimaryThrowsWhenNoWritableCluster() {
        ClusterInfo readOnly = new ClusterInfo("c1", "host:19530", ClusterCapability.READABLE);
        GlobalTopology topology = new GlobalTopology(1L, Collections.singletonList(readOnly));

        assertThrows(IllegalStateException.class, topology::getPrimary);
    }

    @Test
    void toStringContainsVersion() {
        GlobalTopology topology = new GlobalTopology(3L, Collections.emptyList());

        assertTrue(topology.toString().contains("3"));
    }
}

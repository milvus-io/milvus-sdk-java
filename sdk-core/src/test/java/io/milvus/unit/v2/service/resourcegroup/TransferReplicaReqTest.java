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

package io.milvus.unit.v2.service.resourcegroup;

import io.milvus.v2.service.resourcegroup.request.TransferReplicaReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class TransferReplicaReqTest {
    @Test
    void builderBuildsAllFields() {
        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("__default_resource_group")
                .targetGroupName("rg")
                .collectionName("coll")
                .databaseName("db")
                .numberOfReplicas(3L)
                .build();

        assertEquals("__default_resource_group", request.getSourceGroupName());
        assertEquals("rg", request.getTargetGroupName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals(Long.valueOf(3L), request.getNumberOfReplicas());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        TransferReplicaReq request = TransferReplicaReq.builder().build();

        assertNull(request.getSourceGroupName());
        assertNull(request.getTargetGroupName());
        assertNull(request.getCollectionName());
        assertNull(request.getDatabaseName());
        assertEquals(Long.valueOf(1L), request.getNumberOfReplicas());
    }

    @Test
    void settersUpdateFields() {
        TransferReplicaReq request = TransferReplicaReq.builder().build();

        request.setSourceGroupName("src");
        request.setTargetGroupName("tgt");
        request.setCollectionName("coll");
        request.setDatabaseName("db");
        request.setNumberOfReplicas(5L);

        assertEquals("src", request.getSourceGroupName());
        assertEquals("tgt", request.getTargetGroupName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals(Long.valueOf(5L), request.getNumberOfReplicas());
    }

    @Test
    void boundaryNumberOfReplicasIsAccepted() {
        TransferReplicaReq zero = TransferReplicaReq.builder().numberOfReplicas(0L).build();
        TransferReplicaReq negative = TransferReplicaReq.builder().numberOfReplicas(-1L).build();

        assertEquals(Long.valueOf(0L), zero.getNumberOfReplicas());
        assertEquals(Long.valueOf(-1L), negative.getNumberOfReplicas());
    }

    @Test
    void toStringContainsFields() {
        TransferReplicaReq request = TransferReplicaReq.builder()
                .sourceGroupName("src")
                .targetGroupName("tgt")
                .collectionName("coll")
                .databaseName("db")
                .numberOfReplicas(1L)
                .build();

        assertEquals("TransferReplicaReq{sourceGroupName='src', targetGroupName='tgt', collectionName='coll', databaseName='db', numberOfReplicas=1}",
                request.toString());
    }
}

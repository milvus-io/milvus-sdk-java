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

package io.milvus.unit.v2.service.snapshot;

import io.milvus.v2.service.snapshot.request.PinSnapshotDataReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class PinSnapshotDataReqTest {
    @Test
    void builderBuildsAllFields() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder()
                .snapshotName("snap")
                .databaseName("db")
                .collectionName("coll")
                .ttlSeconds(3600L)
                .build();

        assertEquals("snap", request.getSnapshotName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(Long.valueOf(3600L), request.getTtlSeconds());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder().build();

        assertNull(request.getSnapshotName());
        assertEquals("", request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertEquals(Long.valueOf(0L), request.getTtlSeconds());
    }

    @Test
    void settersUpdateFields() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder().build();

        request.setSnapshotName("snap");
        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setTtlSeconds(7200L);

        assertEquals("snap", request.getSnapshotName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(Long.valueOf(7200L), request.getTtlSeconds());
    }

    @Test
    void ttlSecondsAcceptsNullForUnlimitedPin() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder()
                .ttlSeconds(null)
                .build();

        assertNull(request.getTtlSeconds());
    }

    @Test
    void toStringContainsFields() {
        PinSnapshotDataReq request = PinSnapshotDataReq.builder()
                .snapshotName("snap")
                .databaseName("db")
                .collectionName("coll")
                .ttlSeconds(1L)
                .build();

        String text = request.toString();
        assertEquals("PinSnapshotDataReq{snapshotName='snap', databaseName='db', collectionName='coll', ttlSeconds=1}", text);
    }
}

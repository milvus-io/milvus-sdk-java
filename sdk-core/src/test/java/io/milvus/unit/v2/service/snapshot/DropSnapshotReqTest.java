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

import io.milvus.v2.service.snapshot.request.DropSnapshotReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class DropSnapshotReqTest {
    @Test
    void builderBuildsAllFields() {
        DropSnapshotReq request = DropSnapshotReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .snapshotName("snap")
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("snap", request.getSnapshotName());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        DropSnapshotReq request = DropSnapshotReq.builder().build();

        assertEquals("", request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertNull(request.getSnapshotName());
    }

    @Test
    void settersUpdateFields() {
        DropSnapshotReq request = DropSnapshotReq.builder().build();

        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setSnapshotName("snap");

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("snap", request.getSnapshotName());
    }

    @Test
    void toStringContainsFields() {
        DropSnapshotReq request = DropSnapshotReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .snapshotName("snap")
                .build();

        String text = request.toString();
        assertEquals("DropSnapshotReq{databaseName='db', collectionName='coll', snapshotName='snap'}", text);
    }
}

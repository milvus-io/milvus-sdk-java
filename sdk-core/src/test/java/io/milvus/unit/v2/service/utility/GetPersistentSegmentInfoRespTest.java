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

import io.milvus.v2.service.utility.response.GetPersistentSegmentInfoResp;
import io.milvus.v2.service.utility.response.GetPersistentSegmentInfoResp.PersistentSegmentInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class GetPersistentSegmentInfoRespTest {
    @Test
    void builderBuildsAllFields() {
        PersistentSegmentInfo info = PersistentSegmentInfo.builder()
                .segmentID(1L)
                .collectionID(2L)
                .partitionID(3L)
                .collectionName("coll")
                .numOfRows(100L)
                .state("Flushed")
                .level("L0")
                .storageVersion(5L)
                .isSorted(true)
                .build();

        assertEquals(Long.valueOf(1L), info.getSegmentID());
        assertEquals(Long.valueOf(2L), info.getCollectionID());
        assertEquals(Long.valueOf(3L), info.getPartitionID());
        assertEquals("coll", info.getCollectionName());
        assertEquals(Long.valueOf(100L), info.getNumOfRows());
        assertEquals("Flushed", info.getState());
        assertEquals("L0", info.getLevel());
        assertEquals(Long.valueOf(5L), info.getStorageVersion());
        assertEquals(Boolean.TRUE, info.getIsSorted());
    }

    @Test
    void persistentSegmentInfoUnsetFieldsDefaultToNull() {
        PersistentSegmentInfo info = PersistentSegmentInfo.builder().build();

        assertNull(info.getSegmentID());
        assertNull(info.getCollectionID());
        assertNull(info.getPartitionID());
        assertNull(info.getCollectionName());
        assertNull(info.getNumOfRows());
        assertNull(info.getState());
        assertNull(info.getLevel());
        assertNull(info.getStorageVersion());
        assertNull(info.getIsSorted());
    }

    @Test
    void persistentSegmentInfoSettersUpdateFields() {
        PersistentSegmentInfo info = PersistentSegmentInfo.builder().build();

        info.setSegmentID(10L);
        info.setCollectionID(20L);
        info.setPartitionID(30L);
        info.setCollectionName("coll2");
        info.setNumOfRows(200L);
        info.setState("Growing");
        info.setLevel("L1");
        info.setStorageVersion(9L);
        info.setIsSorted(false);

        assertEquals(Long.valueOf(10L), info.getSegmentID());
        assertEquals(Long.valueOf(20L), info.getCollectionID());
        assertEquals(Long.valueOf(30L), info.getPartitionID());
        assertEquals("coll2", info.getCollectionName());
        assertEquals(Long.valueOf(200L), info.getNumOfRows());
        assertEquals("Growing", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(Long.valueOf(9L), info.getStorageVersion());
        assertEquals(Boolean.FALSE, info.getIsSorted());
    }

    @Test
    void builderBuildsSegmentInfosList() {
        List<PersistentSegmentInfo> infos = Arrays.asList(PersistentSegmentInfo.builder().segmentID(1L).build());

        GetPersistentSegmentInfoResp response = GetPersistentSegmentInfoResp.builder()
                .segmentInfos(infos)
                .build();

        assertSame(infos, response.getSegmentInfos());
    }

    @Test
    void unsetSegmentInfosDefaultsToEmptyList() {
        GetPersistentSegmentInfoResp response = GetPersistentSegmentInfoResp.builder().build();

        assertEquals(0, response.getSegmentInfos().size());
    }

    @Test
    void setterUpdatesSegmentInfos() {
        GetPersistentSegmentInfoResp response = GetPersistentSegmentInfoResp.builder().build();

        List<PersistentSegmentInfo> infos = Arrays.asList(PersistentSegmentInfo.builder().segmentID(2L).build());
        response.setSegmentInfos(infos);

        assertSame(infos, response.getSegmentInfos());
    }

    @Test
    void nestedToStringContainsFields() {
        PersistentSegmentInfo info = PersistentSegmentInfo.builder()
                .segmentID(1L)
                .collectionID(2L)
                .partitionID(3L)
                .collectionName("coll")
                .numOfRows(100L)
                .state("Flushed")
                .level("L0")
                .storageVersion(5L)
                .isSorted(true)
                .build();

        String text = info.toString();
        assertEquals("PersistentSegmentInfo{segmentID=1, collectionID=2, partitionID=3, collectionName='coll', numOfRows=100, state='Flushed', level='L0', storageVersion=5, isSorted=true}", text);
    }
}

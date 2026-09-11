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

import io.milvus.v2.service.utility.response.GetQuerySegmentInfoResp;
import io.milvus.v2.service.utility.response.GetQuerySegmentInfoResp.QuerySegmentInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class GetQuerySegmentInfoRespTest {
    @Test
    void builderBuildsAllFields() {
        QuerySegmentInfo info = QuerySegmentInfo.builder()
                .collectionName("coll")
                .segmentID(1L)
                .collectionID(2L)
                .partitionID(3L)
                .memSize(1024L)
                .numOfRows(100L)
                .indexName("idx")
                .indexID(9L)
                .state("Sealed")
                .level("L1")
                .nodeIDs(Arrays.asList(11L, 12L))
                .storageVersion(5L)
                .isSorted(true)
                .build();

        assertEquals("coll", info.getCollectionName());
        assertEquals(Long.valueOf(1L), info.getSegmentID());
        assertEquals(Long.valueOf(2L), info.getCollectionID());
        assertEquals(Long.valueOf(3L), info.getPartitionID());
        assertEquals(Long.valueOf(1024L), info.getMemSize());
        assertEquals(Long.valueOf(100L), info.getNumOfRows());
        assertEquals("idx", info.getIndexName());
        assertEquals(Long.valueOf(9L), info.getIndexID());
        assertEquals("Sealed", info.getState());
        assertEquals("L1", info.getLevel());
        assertEquals(Arrays.asList(11L, 12L), info.getNodeIDs());
        assertEquals(Long.valueOf(5L), info.getStorageVersion());
        assertEquals(Boolean.TRUE, info.getIsSorted());
    }

    @Test
    void querySegmentInfoUnsetFieldsDefaultToNull() {
        QuerySegmentInfo info = QuerySegmentInfo.builder().build();

        assertNull(info.getCollectionName());
        assertNull(info.getSegmentID());
        assertNull(info.getCollectionID());
        assertNull(info.getPartitionID());
        assertNull(info.getMemSize());
        assertNull(info.getNumOfRows());
        assertNull(info.getIndexName());
        assertNull(info.getIndexID());
        assertNull(info.getState());
        assertNull(info.getLevel());
        assertEquals(0, info.getNodeIDs().size());
        assertNull(info.getStorageVersion());
        assertNull(info.getIsSorted());
    }

    @Test
    void querySegmentInfoSettersUpdateFields() {
        QuerySegmentInfo info = QuerySegmentInfo.builder().build();

        info.setCollectionName("coll2");
        info.setSegmentID(10L);
        info.setCollectionID(20L);
        info.setPartitionID(30L);
        info.setMemSize(2048L);
        info.setNumOfRows(200L);
        info.setIndexName("idx2");
        info.setIndexID(90L);
        info.setState("Growing");
        info.setLevel("L0");
        info.setNodeIDs(Arrays.asList(21L));
        info.setStorageVersion(6L);
        info.setIsSorted(false);

        assertEquals("coll2", info.getCollectionName());
        assertEquals(Long.valueOf(10L), info.getSegmentID());
        assertEquals(Long.valueOf(20L), info.getCollectionID());
        assertEquals(Long.valueOf(30L), info.getPartitionID());
        assertEquals(Long.valueOf(2048L), info.getMemSize());
        assertEquals(Long.valueOf(200L), info.getNumOfRows());
        assertEquals("idx2", info.getIndexName());
        assertEquals(Long.valueOf(90L), info.getIndexID());
        assertEquals("Growing", info.getState());
        assertEquals("L0", info.getLevel());
        assertEquals(Arrays.asList(21L), info.getNodeIDs());
        assertEquals(Long.valueOf(6L), info.getStorageVersion());
        assertEquals(Boolean.FALSE, info.getIsSorted());
    }

    @Test
    void builderBuildsSegmentInfosList() {
        List<QuerySegmentInfo> infos = Arrays.asList(QuerySegmentInfo.builder().segmentID(1L).build());

        GetQuerySegmentInfoResp response = GetQuerySegmentInfoResp.builder()
                .segmentInfos(infos)
                .build();

        assertSame(infos, response.getSegmentInfos());
    }

    @Test
    void unsetSegmentInfosDefaultsToEmptyList() {
        GetQuerySegmentInfoResp response = GetQuerySegmentInfoResp.builder().build();

        assertEquals(0, response.getSegmentInfos().size());
    }

    @Test
    void setterUpdatesSegmentInfos() {
        GetQuerySegmentInfoResp response = GetQuerySegmentInfoResp.builder().build();

        List<QuerySegmentInfo> infos = Arrays.asList(QuerySegmentInfo.builder().segmentID(2L).build());
        response.setSegmentInfos(infos);

        assertSame(infos, response.getSegmentInfos());
    }

    @Test
    void nestedToStringContainsFields() {
        QuerySegmentInfo info = QuerySegmentInfo.builder()
                .collectionName("coll")
                .segmentID(1L)
                .collectionID(2L)
                .partitionID(3L)
                .memSize(1024L)
                .numOfRows(100L)
                .indexName("idx")
                .indexID(9L)
                .state("Sealed")
                .level("L1")
                .nodeIDs(Arrays.asList(11L))
                .storageVersion(5L)
                .isSorted(true)
                .build();

        String text = info.toString();
        assertEquals("QuerySegmentInfo{collectionName='coll', segmentID=1, collectionID=2, partitionID=3, memSize=1024, numOfRows=100, indexName='idx', indexID=9, state='Sealed', level='L1', nodeIDs=[11], storageVersion=5, isSorted=true}", text);
    }
}

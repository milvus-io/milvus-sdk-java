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

import io.milvus.v2.service.snapshot.response.DescribeSnapshotResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class DescribeSnapshotRespTest {
    @Test
    void builderBuildsAllFields() {
        List<String> partitions = Arrays.asList("p1", "p2");

        DescribeSnapshotResp response = DescribeSnapshotResp.builder()
                .name("snap")
                .description("desc")
                .collectionName("coll")
                .partitionNames(partitions)
                .createTs(123456L)
                .s3Location("s3://bucket/snap")
                .build();

        assertEquals("snap", response.getName());
        assertEquals("desc", response.getDescription());
        assertEquals("coll", response.getCollectionName());
        assertSame(partitions, response.getPartitionNames());
        assertEquals(Long.valueOf(123456L), response.getCreateTs());
        assertEquals("s3://bucket/snap", response.getS3Location());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        DescribeSnapshotResp response = DescribeSnapshotResp.builder().build();

        assertNull(response.getName());
        assertNull(response.getDescription());
        assertNull(response.getCollectionName());
        assertNull(response.getPartitionNames());
        assertNull(response.getCreateTs());
        assertNull(response.getS3Location());
    }

    @Test
    void settersUpdateFields() {
        DescribeSnapshotResp response = DescribeSnapshotResp.builder().build();

        response.setName("snap");
        response.setDescription("desc");
        response.setCollectionName("coll");
        response.setPartitionNames(Arrays.asList("p1"));
        response.setCreateTs(99L);
        response.setS3Location("s3://b");

        assertEquals("snap", response.getName());
        assertEquals("desc", response.getDescription());
        assertEquals("coll", response.getCollectionName());
        assertEquals(Arrays.asList("p1"), response.getPartitionNames());
        assertEquals(Long.valueOf(99L), response.getCreateTs());
        assertEquals("s3://b", response.getS3Location());
    }

    @Test
    void toStringContainsFields() {
        DescribeSnapshotResp response = DescribeSnapshotResp.builder()
                .name("snap")
                .description("desc")
                .collectionName("coll")
                .partitionNames(Arrays.asList("p1"))
                .createTs(1L)
                .s3Location("s3://b")
                .build();

        String text = response.toString();
        assertEquals("DescribeSnapshotResp{name='snap', description='desc', collectionName='coll', partitionNames=[p1], createTs=1, s3Location='s3://b'}", text);
    }
}

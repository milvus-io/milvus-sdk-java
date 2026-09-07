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

package io.milvus.unit.common.resourcegroup;

import io.milvus.common.resourcegroup.ResourceGroupNodeFilter;
import io.milvus.grpc.KeyValuePair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ResourceGroupNodeFilterTest {

    @Test
    void builderCollectsNodeLabels() {
        ResourceGroupNodeFilter filter = ResourceGroupNodeFilter.newBuilder()
                .withNodeLabel("key1", "value1")
                .withNodeLabel("key2", "value2")
                .build();
        Assertions.assertEquals(2, filter.getNodeLabels().size());
        Assertions.assertEquals("value1", filter.getNodeLabels().get("key1"));
        Assertions.assertEquals("value2", filter.getNodeLabels().get("key2"));
    }

    @Test
    void builderOverwritesDuplicateKey() {
        ResourceGroupNodeFilter filter = ResourceGroupNodeFilter.newBuilder()
                .withNodeLabel("key1", "first")
                .withNodeLabel("key1", "second")
                .build();
        Assertions.assertEquals(1, filter.getNodeLabels().size());
        Assertions.assertEquals("second", filter.getNodeLabels().get("key1"));
    }

    @Test
    void builderRejectsNullKeyOrValue() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ResourceGroupNodeFilter.newBuilder().withNodeLabel(null, "v"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ResourceGroupNodeFilter.newBuilder().withNodeLabel("k", null));
    }

    @Test
    void nullGrpcRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ResourceGroupNodeFilter((io.milvus.grpc.ResourceGroupNodeFilter) null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ResourceGroupNodeFilter.fromGRPC(null));
    }

    @Test
    void fromGrpcPopulatesLabels() {
        io.milvus.grpc.ResourceGroupNodeFilter grpc = io.milvus.grpc.ResourceGroupNodeFilter.newBuilder()
                .addNodeLabels(KeyValuePair.newBuilder().setKey("k1").setValue("v1").build())
                .addNodeLabels(KeyValuePair.newBuilder().setKey("k2").setValue("v2").build())
                .build();
        ResourceGroupNodeFilter filter = ResourceGroupNodeFilter.fromGRPC(grpc);
        Assertions.assertEquals("v1", filter.getNodeLabels().get("k1"));
        Assertions.assertEquals("v2", filter.getNodeLabels().get("k2"));
    }

    @Test
    void toGrpcRoundTrip() {
        ResourceGroupNodeFilter filter = ResourceGroupNodeFilter.newBuilder()
                .withNodeLabel("k1", "v1")
                .withNodeLabel("k2", "v2")
                .build();
        io.milvus.grpc.ResourceGroupNodeFilter grpc = filter.toGRPC();
        Assertions.assertEquals(2, grpc.getNodeLabelsCount());
        Assertions.assertTrue(grpc.getNodeLabelsList().stream()
                .anyMatch(pair -> pair.getKey().equals("k1") && pair.getValue().equals("v1")));
        Assertions.assertTrue(grpc.getNodeLabelsList().stream()
                .anyMatch(pair -> pair.getKey().equals("k2") && pair.getValue().equals("v2")));

        ResourceGroupNodeFilter back = ResourceGroupNodeFilter.fromGRPC(grpc);
        Assertions.assertEquals("v1", back.getNodeLabels().get("k1"));
        Assertions.assertEquals("v2", back.getNodeLabels().get("k2"));
    }

    @Test
    void toStringContainsLabels() {
        String s = ResourceGroupNodeFilter.newBuilder().withNodeLabel("k", "v").build().toString();
        Assertions.assertTrue(s.contains("nodeLabels"));
        Assertions.assertTrue(s.contains("k=v"));
    }
}

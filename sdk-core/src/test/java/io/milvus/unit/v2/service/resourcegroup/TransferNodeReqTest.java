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

import io.milvus.v2.service.resourcegroup.request.TransferNodeReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class TransferNodeReqTest {
    @Test
    void builderBuildsAllFields() {
        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("__default_resource_group")
                .targetGroupName("rg")
                .numOfNodes(2)
                .build();

        assertEquals("__default_resource_group", request.getSourceGroupName());
        assertEquals("rg", request.getTargetGroupName());
        assertEquals(Integer.valueOf(2), request.getNumOfNodes());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        TransferNodeReq request = TransferNodeReq.builder().build();

        assertNull(request.getSourceGroupName());
        assertNull(request.getTargetGroupName());
        assertNull(request.getNumOfNodes());
    }

    @Test
    void settersUpdateFields() {
        TransferNodeReq request = TransferNodeReq.builder().build();

        request.setSourceGroupName("src");
        request.setTargetGroupName("tgt");
        request.setNumOfNodes(1);

        assertEquals("src", request.getSourceGroupName());
        assertEquals("tgt", request.getTargetGroupName());
        assertEquals(Integer.valueOf(1), request.getNumOfNodes());
    }

    @Test
    void boundaryNumOfNodesIsAccepted() {
        TransferNodeReq zero = TransferNodeReq.builder().numOfNodes(0).build();
        TransferNodeReq negative = TransferNodeReq.builder().numOfNodes(-1).build();

        assertEquals(Integer.valueOf(0), zero.getNumOfNodes());
        assertEquals(Integer.valueOf(-1), negative.getNumOfNodes());
    }

    @Test
    void toStringContainsFields() {
        TransferNodeReq request = TransferNodeReq.builder()
                .sourceGroupName("src")
                .targetGroupName("tgt")
                .numOfNodes(1)
                .build();

        assertEquals("TransferNodeReq{sourceGroupName='src', targetGroupName='tgt', numOfNodes=1}",
                request.toString());
    }
}

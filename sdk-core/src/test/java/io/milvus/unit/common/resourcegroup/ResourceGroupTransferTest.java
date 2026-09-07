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

import io.milvus.common.resourcegroup.ResourceGroupTransfer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ResourceGroupTransferTest {

    @Test
    void constructorFromName() {
        ResourceGroupTransfer transfer = new ResourceGroupTransfer("rg1");
        Assertions.assertEquals("rg1", transfer.getResourceGroupName());
    }

    @Test
    void nullNameRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ResourceGroupTransfer((String) null));
    }

    @Test
    void constructorFromGrpc() {
        io.milvus.grpc.ResourceGroupTransfer grpc = io.milvus.grpc.ResourceGroupTransfer.newBuilder()
                .setResourceGroup("rg-from-grpc")
                .build();
        ResourceGroupTransfer transfer = new ResourceGroupTransfer(grpc);
        Assertions.assertEquals("rg-from-grpc", transfer.getResourceGroupName());
    }

    @Test
    void nullGrpcRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ResourceGroupTransfer((io.milvus.grpc.ResourceGroupTransfer) null));
    }

    @Test
    void toGrpcRoundTrip() {
        ResourceGroupTransfer transfer = new ResourceGroupTransfer("rg2");
        io.milvus.grpc.ResourceGroupTransfer grpc = transfer.toGRPC();
        Assertions.assertEquals("rg2", grpc.getResourceGroup());
        Assertions.assertEquals("rg2", new ResourceGroupTransfer(grpc).getResourceGroupName());
    }
}

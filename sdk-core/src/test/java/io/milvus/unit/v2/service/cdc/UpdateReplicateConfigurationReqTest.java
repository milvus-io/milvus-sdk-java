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

package io.milvus.unit.v2.service.cdc;

import io.milvus.v2.service.cdc.request.ReplicateConfiguration;
import io.milvus.v2.service.cdc.request.UpdateReplicateConfigurationReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class UpdateReplicateConfigurationReqTest {
    @Test
    void builderBuildsAllFields() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();

        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .replicateConfiguration(configuration)
                .forcePromote(true)
                .build();

        assertSame(configuration, request.getReplicateConfiguration());
        assertTrue(request.isForcePromote());
    }

    @Test
    void unsetFieldsUseDefaults() {
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder().build();

        assertNull(request.getReplicateConfiguration());
        assertFalse(request.isForcePromote());
    }

    @Test
    void settersUpdateFields() {
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder().build();

        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();
        request.setReplicateConfiguration(configuration);
        request.setForcePromote(true);

        assertSame(configuration, request.getReplicateConfiguration());
        assertTrue(request.isForcePromote());
    }

    @Test
    void toStringContainsFields() {
        UpdateReplicateConfigurationReq request = UpdateReplicateConfigurationReq.builder()
                .forcePromote(true)
                .build();

        String text = request.toString();
        assertEquals("UpdateReplicateConfigurationReq{replicateConfiguration=null, forcePromote=true}", text);
    }
}

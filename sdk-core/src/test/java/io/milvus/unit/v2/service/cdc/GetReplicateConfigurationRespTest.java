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
import io.milvus.v2.service.cdc.response.GetReplicateConfigurationResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class GetReplicateConfigurationRespTest {
    @Test
    void builderBuildsAllFields() {
        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();

        GetReplicateConfigurationResp response = GetReplicateConfigurationResp.builder()
                .replicateConfiguration(configuration)
                .build();

        assertSame(configuration, response.getReplicateConfiguration());
    }

    @Test
    void unsetFieldDefaultsToNull() {
        GetReplicateConfigurationResp response = GetReplicateConfigurationResp.builder().build();

        assertNull(response.getReplicateConfiguration());
    }

    @Test
    void setterUpdatesField() {
        GetReplicateConfigurationResp response = GetReplicateConfigurationResp.builder().build();

        ReplicateConfiguration configuration = ReplicateConfiguration.builder().build();
        response.setReplicateConfiguration(configuration);

        assertSame(configuration, response.getReplicateConfiguration());
    }

    @Test
    void toStringContainsFields() {
        GetReplicateConfigurationResp response = GetReplicateConfigurationResp.builder().build();

        String text = response.toString();
        assertEquals("GetReplicateConfigurationResp{replicateConfiguration=null}", text);
    }
}

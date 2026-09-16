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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import io.milvus.telemetry.ClientTelemetryManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class MilvusClientV2TelemetryDockerTest extends MilvusV2DockerTestBase {

    @Test
    void getTelemetryReturnsNonNullManager() {
        ClientTelemetryManager manager = client.getTelemetry();
        Assertions.assertNotNull(manager);
        Assertions.assertNotNull(manager.getConfig());
        Assertions.assertFalse(manager.isClosed());
    }

    @Test
    void startTelemetryIsIdempotentAndDoesNotThrow() {
        ClientTelemetryManager manager = client.getTelemetry();
        Assertions.assertNotNull(manager);

        // startTelemetry must not throw and must be callable repeatedly
        client.startTelemetry();
        client.startTelemetry();

        ClientTelemetryManager afterStart = client.getTelemetry();
        Assertions.assertSame(manager, afterStart);
        Assertions.assertFalse(afterStart.isClosed());
    }
}

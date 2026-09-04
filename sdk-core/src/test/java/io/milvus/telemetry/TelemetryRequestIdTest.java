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

package io.milvus.telemetry;

import io.grpc.CallOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class TelemetryRequestIdTest {
    @Test
    void telemetryRequestIdPrefersCallOptionAndValidatesBothSources() {
        String callOptionId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String threadId = "0af7651916cd43dd8448eb211c80319c";
        ThreadLocal<String> fallback = new ThreadLocal<>();
        fallback.set(threadId);

        assertEquals(callOptionId, TelemetryInterceptor.requestId(
                CallOptions.DEFAULT.withOption(
                        io.milvus.common.interceptor.ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                        callOptionId),
                fallback));
        assertEquals(threadId, TelemetryInterceptor.requestId(CallOptions.DEFAULT, fallback));
        assertEquals("", TelemetryInterceptor.requestId(
                CallOptions.DEFAULT.withOption(
                        io.milvus.common.interceptor.ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                        ""),
                fallback));
        fallback.set("4BF92F3577B34DA6A3CE929D0E0E4736");
        assertEquals("", TelemetryInterceptor.requestId(CallOptions.DEFAULT, fallback));
    }

    @Test
    void generatesValidClientRequestId() {
        String requestId = ClientTelemetryManager.newClientRequestId();

        assertTrue(requestId.matches("[0-9a-f]{32}"));
        assertFalse(requestId.matches("0{32}"));
    }
}

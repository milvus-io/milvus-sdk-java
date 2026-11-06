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

import io.milvus.v2.service.utility.request.CancelRequestsReq;
import io.milvus.v2.service.utility.request.ListRunningRequestsReq;
import io.milvus.v2.service.utility.response.CancelRequestsResp;
import io.milvus.v2.service.utility.response.ListRunningRequestsResp;
import io.milvus.v2.service.utility.response.RunningRequestInfo;
import io.milvus.v2.service.utility.response.RunningRequestNodeResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RunningRequestsReqTest {
    @Test
    void listRequestDefaultsMeanNoFilter() {
        ListRunningRequestsReq request = ListRunningRequestsReq.builder().build();

        assertEquals("", request.getDatabaseName());
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getUser());
        assertEquals(Long.valueOf(0L), request.getMinElapsedMs());
    }

    @Test
    void listRequestBuilderSetsAllFields() {
        ListRunningRequestsReq request = ListRunningRequestsReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .user("alice")
                .minElapsedMs(5000L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("alice", request.getUser());
        assertEquals(Long.valueOf(5000L), request.getMinElapsedMs());
    }

    @Test
    void cancelRequestDefaults() {
        CancelRequestsReq request = CancelRequestsReq.builder().build();

        assertTrue(request.getRequestIds().isEmpty());
        assertEquals("", request.getReason());
        assertEquals("", request.getUser());
    }

    @Test
    void cancelRequestBuilderSetsAllFields() {
        CancelRequestsReq request = CancelRequestsReq.builder()
                .requestIds(Arrays.asList("1-to03zu-1", "1-to03zu-2"))
                .reason("too heavy")
                .user("alice")
                .build();

        assertEquals(Arrays.asList("1-to03zu-1", "1-to03zu-2"), request.getRequestIds());
        assertEquals("too heavy", request.getReason());
        assertEquals("alice", request.getUser());
    }

    @Test
    void responseDefaultsAreEmpty() {
        ListRunningRequestsResp list = ListRunningRequestsResp.builder().build();
        assertTrue(list.getRequests().isEmpty());
        assertTrue(list.getNodeResults().isEmpty());

        CancelRequestsResp cancel = CancelRequestsResp.builder().build();
        assertTrue(cancel.getCanceled().isEmpty());
        assertTrue(cancel.getNotFound().isEmpty());
        assertTrue(cancel.getUndetermined().isEmpty());
        assertTrue(cancel.getNodeResults().isEmpty());

        RunningRequestInfo info = RunningRequestInfo.builder().build();
        assertEquals("", info.getRequestId());
        assertTrue(info.getTaskIds().isEmpty());
        assertFalse(info.getCancellable());
    }

    @Test
    void nodeResultSuccessFollowsErrorCode() {
        assertTrue(RunningRequestNodeResult.builder().nodeId(1L).build().isSuccess());
        assertFalse(RunningRequestNodeResult.builder().nodeId(2L).errorCode(65535).reason("down").build().isSuccess());
    }
}

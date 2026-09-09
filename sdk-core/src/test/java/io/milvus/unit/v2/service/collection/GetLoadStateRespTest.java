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

package io.milvus.unit.v2.service.collection;

import io.milvus.grpc.LoadState;
import io.milvus.v2.service.collection.response.GetLoadStateResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetLoadStateRespTest {

    @Test
    void builderBuildsWithAllFields() {
        GetLoadStateResp response = GetLoadStateResp.builder()
                .state(LoadState.LoadStateLoaded)
                .progress(100L)
                .build();

        assertEquals(LoadState.LoadStateLoaded, response.getState());
        assertEquals("LoadStateLoaded", response.getStateName());
        assertEquals(100L, response.getProgress());
    }

    @Test
    void settersUpdateGetters() {
        GetLoadStateResp response = GetLoadStateResp.builder().build();

        response.setState(LoadState.LoadStateLoading);
        assertEquals(LoadState.LoadStateLoading, response.getState());
        assertEquals("LoadStateLoading", response.getStateName());

        response.setProgress(50L);
        assertEquals(50L, response.getProgress());
    }

    @Test
    void defaultsAreNull() {
        GetLoadStateResp response = GetLoadStateResp.builder().build();
        assertNull(response.getState());
        assertNull(response.getStateName());
        assertNull(response.getProgress());
    }

    @Test
    void stateNameFollowsState() {
        GetLoadStateResp response = GetLoadStateResp.builder().build();
        response.setState(LoadState.LoadStateNotExist);
        assertEquals("LoadStateNotExist", response.getStateName());
        response.setState(null);
        assertNull(response.getStateName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(GetLoadStateResp.builder());
    }

    @Test
    void toStringContainsFields() {
        GetLoadStateResp response = GetLoadStateResp.builder()
                .state(LoadState.LoadStateLoaded)
                .build();
        assertTrue(response.toString().contains("LoadStateLoaded"));
    }
}

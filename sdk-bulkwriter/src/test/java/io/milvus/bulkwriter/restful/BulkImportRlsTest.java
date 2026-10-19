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

package io.milvus.bulkwriter.restful;

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.common.utils.JsonUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class BulkImportRlsTest {

    @Test
    void milvusImportRequestExposesRlsFields() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(true)
                .build();
        assertEquals("alice", request.getRlsPrincipal());
        assertEquals(true, request.getSkipRls());

        request.setRlsPrincipal("bob");
        request.setSkipRls(false);
        assertEquals("bob", request.getRlsPrincipal());
        assertEquals(false, request.getSkipRls());
    }

    @Test
    void unsetRlsFieldsAreNull() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .build();
        assertNull(request.getRlsPrincipal());
        assertNull(request.getSkipRls());
    }

    @Test
    void addRlsOptionsInjectsIntoExistingOptions() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        Map<String, Object> options = new HashMap<>();
        options.put("auto_commit", "true");
        Map<String, Object> params = new HashMap<>();
        params.put("options", options);

        BulkImportUtils.addRlsOptions(params, request);

        assertEquals("alice", options.get("rls_principal"));
        assertEquals("false", options.get("skip_rls"));
        assertEquals("true", options.get("auto_commit"));
    }

    @Test
    void addRlsOptionsCreatesOptionsWhenAbsent() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(true)
                .build();
        Map<String, Object> params = new HashMap<>();

        BulkImportUtils.addRlsOptions(params, request);

        Map<String, Object> options = (Map<String, Object>) params.get("options");
        assertEquals("alice", options.get("rls_principal"));
        assertEquals("true", options.get("skip_rls"));
    }

    @Test
    void addRlsOptionsSkipsUnsetFields() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .build();
        Map<String, Object> params = new HashMap<>();
        params.put("options", new HashMap<>());

        BulkImportUtils.addRlsOptions(params, request);

        Map<String, Object> options = (Map<String, Object>) params.get("options");
        assertEquals(0, options.size());
    }

    @Test
    void rlsFieldsAreNotSerializedAsTopLevelKeys() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();

        Map<String, Object> params = JsonUtils.fromJson(
                JsonUtils.toJson(request), new TypeToken<Map<String, Object>>() {
                }.getType());

        assertFalse(params.containsKey("rlsPrincipal"));
        assertFalse(params.containsKey("skipRls"));
    }

    @Test
    void toStringIncludesRlsFields() {
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName("col")
                .rlsPrincipal("alice")
                .skipRls(false)
                .build();
        assertTrue(request.toString().contains("rlsPrincipal='alice'"));
        assertTrue(request.toString().contains("skipRls=false"));
    }
}

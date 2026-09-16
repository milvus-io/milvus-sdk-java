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

package io.milvus.bulkwriter;

import io.milvus.exception.ParamException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
public class VolumeManagerParamTest {

    @Test
    void testBuildWithAllOptions() {
        VolumeManagerParam param = VolumeManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .withApiKey("api-key")
                .build();

        assertEquals("https://api.cloud.zilliz.com", param.getCloudEndpoint());
        assertEquals("api-key", param.getApiKey());
        assertTrue(param.toString().contains("https://api.cloud.zilliz.com"));
    }

    @Test
    void testBuildRejectsEmptyCloudEndpoint() {
        assertThrows(ParamException.class, () -> VolumeManagerParam.newBuilder()
                .withApiKey("api-key")
                .build());
    }

    @Test
    void testBuildRejectsEmptyApiKey() {
        assertThrows(ParamException.class, () -> VolumeManagerParam.newBuilder()
                .withCloudEndpoint("https://api.cloud.zilliz.com")
                .build());
    }
}

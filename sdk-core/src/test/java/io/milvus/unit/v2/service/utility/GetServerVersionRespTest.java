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

import io.milvus.v2.service.utility.response.GetServerVersionResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class GetServerVersionRespTest {
    @Test
    void builderBuildsAllFields() {
        GetServerVersionResp response = GetServerVersionResp.builder()
                .version("v2.6.0")
                .buildTime("2026-01-01")
                .gitCommit("abc123")
                .goVersion("go1.24")
                .deployMode("standalone")
                .build();

        assertEquals("v2.6.0", response.getVersion());
        assertEquals("2026-01-01", response.getBuildTime());
        assertEquals("abc123", response.getGitCommit());
        assertEquals("go1.24", response.getGoVersion());
        assertEquals("standalone", response.getDeployMode());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        GetServerVersionResp response = GetServerVersionResp.builder().build();

        assertNull(response.getVersion());
        assertNull(response.getBuildTime());
        assertNull(response.getGitCommit());
        assertNull(response.getGoVersion());
        assertNull(response.getDeployMode());
    }

    @Test
    void settersUpdateFields() {
        GetServerVersionResp response = GetServerVersionResp.builder().build();

        response.setVersion("v3.0.0");
        response.setBuildTime("2027-01-01");
        response.setGitCommit("def456");
        response.setGoVersion("go1.25");
        response.setDeployMode("cluster");

        assertEquals("v3.0.0", response.getVersion());
        assertEquals("2027-01-01", response.getBuildTime());
        assertEquals("def456", response.getGitCommit());
        assertEquals("go1.25", response.getGoVersion());
        assertEquals("cluster", response.getDeployMode());
    }

    @Test
    void toStringContainsFields() {
        GetServerVersionResp response = GetServerVersionResp.builder()
                .version("v2.6.0")
                .buildTime("t")
                .gitCommit("c")
                .goVersion("g")
                .deployMode("d")
                .build();

        String text = response.toString();
        assertEquals("GetServerVersionResp{version='v2.6.0', buildTime='t', gitCommit='c', goVersion='g', deployMode='d'}", text);
    }
}

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

package io.milvus.unit.v2.service.rbac;

import io.milvus.v2.service.rbac.request.DropUserReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class DropUserReqTest {
    @Test
    void builderBuildsAllFields() {
        DropUserReq request = DropUserReq.builder()
                .userName("alice")
                .build();

        assertEquals("alice", request.getUserName());
    }

    @Test
    void unsetFieldsDefaultToNull() {
        DropUserReq request = DropUserReq.builder().build();

        assertNull(request.getUserName());
    }

    @Test
    void setterUpdatesField() {
        DropUserReq request = DropUserReq.builder().build();

        request.setUserName("bob");

        assertEquals("bob", request.getUserName());
    }

    @Test
    void toStringContainsFields() {
        DropUserReq request = DropUserReq.builder().userName("alice").build();

        assertEquals("DropUserReq{userName='alice'}", request.toString());
    }
}

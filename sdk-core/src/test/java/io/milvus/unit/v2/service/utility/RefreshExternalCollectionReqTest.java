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

import com.google.gson.JsonObject;
import io.milvus.v2.service.utility.request.RefreshExternalCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("unit")
class RefreshExternalCollectionReqTest {
    @Test
    void builderBuildsAllFields() {
        JsonObject spec = new JsonObject();
        spec.addProperty("format", "parquet");

        RefreshExternalCollectionReq request = RefreshExternalCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .externalSource("s3")
                .externalSpec(spec)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("s3", request.getExternalSource());
        assertSame(spec, request.getExternalSpec());
    }

    @Test
    void unsetExternalSourceDefaultsToEmptyString() {
        RefreshExternalCollectionReq request = RefreshExternalCollectionReq.builder()
                .collectionName("coll")
                .build();

        assertNull(request.getDatabaseName());
        assertEquals("", request.getExternalSource());
        assertNull(request.getExternalSpec());
    }

    @Test
    void toStringContainsFields() {
        RefreshExternalCollectionReq request = RefreshExternalCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .externalSource("s3")
                .build();

        String text = request.toString();
        assertEquals("RefreshExternalCollectionReq{databaseName='db', collectionName='coll', externalSource='s3', externalSpec=null}", text);
    }
}

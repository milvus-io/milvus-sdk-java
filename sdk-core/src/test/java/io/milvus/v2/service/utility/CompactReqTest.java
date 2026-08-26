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

package io.milvus.v2.service.utility;

import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.request.CompactReq;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompactReqTest {
    @Test
    void targetSizeDefaultsToMegabytes() {
        CompactReq request = CompactReq.builder()
                .collectionName("test")
                .targetSize(512L)
                .build();

        assertEquals(Long.valueOf(512L), request.getTargetSizeInMB());
    }

    @Test
    void targetSizeSupportsBinarySizeUnits() {
        assertEquals(Long.valueOf(1L), buildTargetSize(1048576L, "B").getTargetSizeInMB());
        assertEquals(Long.valueOf(1L), buildTargetSize(1024L, "KB").getTargetSizeInMB());
        assertEquals(Long.valueOf(1024L), buildTargetSize(1L, "GB").getTargetSizeInMB());
        assertEquals(Long.valueOf(1024L * 1024L), buildTargetSize(1L, "TB").getTargetSizeInMB());
        assertEquals(Long.valueOf(1024L * 1024L * 1024L), buildTargetSize(1L, "PB").getTargetSizeInMB());
    }

    @Test
    void targetSizeAcceptsCaseAndWhitespace() {
        assertEquals(Long.valueOf(2048L), buildTargetSize(2L, "gb").getTargetSizeInMB());
        assertEquals(Long.valueOf(2048L), buildTargetSize(2L, " GB ").getTargetSizeInMB());
    }

    @Test
    void nullTargetSizeUsesServerDefault() {
        CompactReq request = CompactReq.builder()
                .collectionName("test")
                .build();

        assertNull(request.getTargetSizeInMB());
    }

    @Test
    void zeroTargetSizeUsesServerDefault() {
        CompactReq request = CompactReq.builder()
                .collectionName("test")
                .targetSize(0L)
                .build();

        assertNull(request.getTargetSizeInMB());
    }

    @Test
    void targetSizeRejectsInvalidValues() {
        assertThrows(MilvusClientException.class, () -> buildTargetSize(-1L, "MB").getTargetSizeInMB());
        assertThrows(MilvusClientException.class, () -> buildTargetSize(1023L, "KB").getTargetSizeInMB());
        assertThrows(MilvusClientException.class, () -> buildTargetSize(1L, "XB").getTargetSizeInMB());
        assertThrows(MilvusClientException.class, () -> CompactReq.builder()
                .collectionName("test")
                .targetSize(1L)
                .targetSizeUnit(null)
                .build()
                .getTargetSizeInMB());
    }

    private CompactReq buildTargetSize(Long targetSize, String unit) {
        return CompactReq.builder()
                .collectionName("test")
                .targetSize(targetSize)
                .targetSizeUnit(unit)
                .build();
    }
}

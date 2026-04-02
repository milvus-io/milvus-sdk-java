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

package io.milvus.v2.service.utility;

import io.milvus.v2.exception.MilvusClientException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OptimizeTaskTest {

    @Test
    void parseTargetSize_megabytes() {
        assertEquals(512L, OptimizeTask.parseTargetSize("512MB"));
        assertEquals(512L, OptimizeTask.parseTargetSize("512mb"));
        assertEquals(512L, OptimizeTask.parseTargetSize("512Mb"));
        assertEquals(100L, OptimizeTask.parseTargetSize("100MB"));
    }

    @Test
    void parseTargetSize_gigabytes() {
        assertEquals(1024L, OptimizeTask.parseTargetSize("1GB"));
        assertEquals(1024L, OptimizeTask.parseTargetSize("1gb"));
        assertEquals(2048L, OptimizeTask.parseTargetSize("2GB"));
    }

    @Test
    void parseTargetSize_fractionalGigabytes() {
        assertEquals(1536L, OptimizeTask.parseTargetSize("1.5GB"));
        assertEquals(1228L, OptimizeTask.parseTargetSize("1.2gb"));
    }

    @Test
    void parseTargetSize_kilobytes() {
        assertEquals(1L, OptimizeTask.parseTargetSize("1024KB"));
        assertEquals(1L, OptimizeTask.parseTargetSize("1024kb"));
        assertEquals(10L, OptimizeTask.parseTargetSize("10240KB"));
    }

    @Test
    void parseTargetSize_bytes() {
        assertEquals(1L, OptimizeTask.parseTargetSize("1048576B"));
        assertEquals(1L, OptimizeTask.parseTargetSize("1048576b"));
        // bare number treated as bytes
        assertEquals(1L, OptimizeTask.parseTargetSize("1048576"));
    }

    @Test
    void parseTargetSize_terabytes() {
        assertEquals(1024L * 1024, OptimizeTask.parseTargetSize("1TB"));
        assertEquals(1024L * 1024, OptimizeTask.parseTargetSize("1tb"));
    }

    @Test
    void parseTargetSize_petabytes() {
        assertEquals(1024L * 1024 * 1024, OptimizeTask.parseTargetSize("1PB"));
    }

    @Test
    void parseTargetSize_null() {
        assertNull(OptimizeTask.parseTargetSize(null));
    }

    @Test
    void parseTargetSize_withWhitespace() {
        assertEquals(512L, OptimizeTask.parseTargetSize("  512MB  "));
        assertEquals(1024L, OptimizeTask.parseTargetSize(" 1GB "));
    }

    @Test
    void parseTargetSize_withSpaceBetweenNumberAndUnit() {
        assertEquals(512L, OptimizeTask.parseTargetSize("512 mb"));
    }

    @Test
    void parseTargetSize_tooSmall() {
        MilvusClientException e = assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize("100KB"));
        assertTrue(e.getMessage().contains("too small"));
    }

    @Test
    void parseTargetSize_zeroBytes() {
        assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize("0MB"));
    }

    @Test
    void parseTargetSize_invalidFormat() {
        assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize("abc"));
    }

    @Test
    void parseTargetSize_invalidUnit() {
        assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize("100XB"));
    }

    @Test
    void parseTargetSize_emptyString() {
        assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize(""));
    }

    @Test
    void parseTargetSize_negativeValue() {
        assertThrows(MilvusClientException.class,
                () -> OptimizeTask.parseTargetSize("-100MB"));
    }
}

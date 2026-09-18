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

import io.milvus.bulkwriter.common.clientenum.UploadPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class UploadPolicyTest {
    @Test
    void existenceAndOverwriteIgnoreOptionalMetadata() {
        assertTrue(UploadPolicy.SKIP_IF_EXISTS.shouldSkip(5, 1000, null, null));
        assertFalse(UploadPolicy.OVERWRITE.shouldSkip(5, 1000, 5L, 1000L));
    }

    @Test
    void sizePolicyIgnoresTimeButRequiresKnownMatchingSize() {
        assertTrue(UploadPolicy.SKIP_IF_SAME_SIZE.shouldSkip(5, 1000, 5L, null));
        assertTrue(UploadPolicy.SKIP_IF_SAME_SIZE.shouldSkip(0, 1000, 0L, null));
        assertFalse(UploadPolicy.SKIP_IF_SAME_SIZE.shouldSkip(0, 1000, null, null));
        assertFalse(UploadPolicy.SKIP_IF_SAME_SIZE.shouldSkip(0, 1000, -1L, null));
        assertFalse(UploadPolicy.SKIP_IF_SAME_SIZE.shouldSkip(5, 1000, 6L, 2000L));
    }

    @Test
    void timePolicyUsesLessThanOrEqualWithoutRounding() {
        assertTrue(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, 5L, 1001L));
        assertTrue(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, 5L, 1000L));
        assertFalse(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, 5L, 999L));
        assertFalse(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, 6L, 2000L));
        assertFalse(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, 5L, null));
        assertFalse(UploadPolicy.SIZE_AND_MTIME.shouldSkip(5, 1000, null, 2000L));
    }
}

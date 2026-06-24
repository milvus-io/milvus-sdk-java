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

package io.milvus.bulkwriter.storage.client;

import io.milvus.bulkwriter.storage.StorageClient;
import io.milvus.exception.ParamException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProgressInputStreamTest {

    @Test
    public void testProgressCallbackCountsReadBytes() throws Exception {
        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        AtomicLong uploadedBytes = new AtomicLong(0);
        StorageClient.UploadProgressListener listener = uploadedBytes::addAndGet;

        try (ProgressInputStream inputStream = new ProgressInputStream(new ByteArrayInputStream(bytes), listener)) {
            byte[] buffer = new byte[2];
            assertEquals(2, inputStream.read(buffer));
            assertEquals(2, uploadedBytes.get());
            assertEquals(2, inputStream.read(buffer));
            assertEquals(4, uploadedBytes.get());
            assertEquals(5, inputStream.read());
            assertEquals(5, uploadedBytes.get());
            assertEquals(-1, inputStream.read(buffer));
            assertEquals(5, uploadedBytes.get());
        }
    }

    @Test
    public void testCalculateUploadPartSizeAutoAndExplicit() {
        assertEquals(5L * 1024L * 1024L, MinioStorageClient.calculateUploadPartSize(1L, 0L));
        assertEquals(7L * 1024L * 1024L,
                MinioStorageClient.calculateUploadPartSize(1L, 7L * 1024L * 1024L));
        long partSize = MinioStorageClient.calculateUploadPartSize(20L * 1024L * 1024L * 1024L, 0L);
        assertEquals(0L, partSize % (1024L * 1024L));
        assertTrue(partSize > 5L * 1024L * 1024L);
    }

    @Test
    public void testCalculateUploadPartSizeRejectsTooSmallExplicitValue() {
        assertThrows(ParamException.class, () -> MinioStorageClient.calculateUploadPartSize(1L, 1L));
    }
}

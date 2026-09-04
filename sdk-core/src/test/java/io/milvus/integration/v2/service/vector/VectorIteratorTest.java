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

package io.milvus.integration.v2.service.vector;

import io.milvus.support.v2.BaseTest;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class VectorIteratorTest extends BaseTest {

    @Test
    void testQueryIteratorRejectsBatchSizeOutOfBounds() {
        VectorService vectorService = new VectorService();
        MilvusClientException tooLarge = Assertions.assertThrows(MilvusClientException.class,
                () -> vectorService.queryIterator(null, QueryIteratorReq.builder().collectionName("book").batchSize(20000).build(), null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, tooLarge.getErrorCode());

        MilvusClientException zero = Assertions.assertThrows(MilvusClientException.class,
                () -> vectorService.queryIterator(null, QueryIteratorReq.builder().collectionName("book").batchSize(0).build(), null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, zero.getErrorCode());

        MilvusClientException negative = Assertions.assertThrows(MilvusClientException.class,
                () -> vectorService.queryIterator(null, QueryIteratorReq.builder().collectionName("book").batchSize(-1).build(), null));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, negative.getErrorCode());
    }
}

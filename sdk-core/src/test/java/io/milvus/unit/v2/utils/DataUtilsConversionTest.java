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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.DataUtils;

import io.milvus.grpc.*;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.DeleteReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Tag("unit")
class DataUtilsConversionTest {

    @Test
    void testConvertGrpcDeleteRequest() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("min_id", 10L);
        templateValues.put("tags", Arrays.asList("a", "b"));
        DeleteRequest request = new DataUtils().ConvertToGrpcDeleteRequest(DeleteReq.builder()
                .databaseName("db")
                .collectionName("collection")
                .partitionName("partition")
                .filter("id > {min_id} and tag in {tags}")
                .filterTemplateValues(templateValues)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assertions.assertEquals("db", request.getDbName());
        Assertions.assertEquals("collection", request.getCollectionName());
        Assertions.assertEquals("partition", request.getPartitionName());
        Assertions.assertEquals("id > {min_id} and tag in {tags}", request.getExpr());
        Assertions.assertEquals(new HashSet<>(Arrays.asList("min_id", "tags")),
                request.getExprTemplateValuesMap().keySet());
        Assertions.assertEquals(io.milvus.grpc.ConsistencyLevel.Strong_VALUE, request.getConsistencyLevelValue());
    }

    @Test
    void testConvertGrpcDeleteRequestWithoutConsistencyLevel() {
        DeleteRequest request = new DataUtils().ConvertToGrpcDeleteRequest(DeleteReq.builder()
                .databaseName("db")
                .collectionName("collection")
                .filter("id > 0")
                .build());

        Assertions.assertEquals(0, request.getConsistencyLevelValue());
    }
}

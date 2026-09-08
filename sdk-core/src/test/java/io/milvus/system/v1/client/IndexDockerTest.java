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

package io.milvus.system.v1.client;

import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.index.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

@Tag("system")
class IndexDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testFloatVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.FloatVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        Map<IndexType, String> indexTypes = new HashMap<>();
        indexTypes.put(IndexType.FLAT, "{}");
        indexTypes.put(IndexType.IVF_FLAT, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_SQ8, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_PQ, "{\"nlist\":128, \"m\":16, \"nbits\":8}");
        indexTypes.put(IndexType.HNSW, "{\"M\":16,\"efConstruction\":64}");

        List<MetricType> metricTypes = new ArrayList<>();
        metricTypes.add(MetricType.L2);
        metricTypes.add(MetricType.IP);

        for (IndexType type : indexTypes.keySet()) {
            for (MetricType metric : metricTypes) {
                testIndex(randomCollectionName, DataType.FloatVector.name(), type, metric, indexTypes.get(type), Boolean.TRUE);
                testIndex(randomCollectionName, DataType.FloatVector.name(), type, metric, indexTypes.get(type), Boolean.FALSE);
            }
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testBinaryVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, false,
                Collections.singletonList(DataType.BinaryVector));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        List<MetricType> flatMetricTypes = new ArrayList<>();
        flatMetricTypes.add(MetricType.HAMMING);
        flatMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : flatMetricTypes) {
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_FLAT, metric, "{}", Boolean.TRUE);
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_FLAT, metric, "{}", Boolean.FALSE);
        }

        List<MetricType> ivfMetricTypes = new ArrayList<>();
        ivfMetricTypes.add(MetricType.HAMMING);
        ivfMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : ivfMetricTypes) {
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.TRUE);
            testIndex(randomCollectionName, DataType.BinaryVector.name(), IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.FALSE);
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }
}

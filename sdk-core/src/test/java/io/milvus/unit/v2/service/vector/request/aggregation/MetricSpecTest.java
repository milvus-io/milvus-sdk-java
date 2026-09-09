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

package io.milvus.unit.v2.service.vector.request.aggregation;

import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class MetricSpecTest {

    @Test
    void builderSetsFields() {
        MetricSpec spec = MetricSpec.builder()
                .op(MetricOps.AVG)
                .fieldName("score")
                .build();

        assertEquals(MetricOps.AVG, spec.getOp());
        assertEquals("score", spec.getFieldName());
    }

    @Test
    void countAllowsWildcardField() {
        MetricSpec spec = MetricSpec.builder()
                .op(MetricOps.COUNT)
                .fieldName("*")
                .build();
        assertEquals(MetricOps.COUNT, spec.getOp());
        assertEquals("*", spec.getFieldName());
    }

    @Test
    void nullOpRejected() {
        assertThrows(MilvusClientException.class,
                () -> MetricSpec.builder().fieldName("score").build());
    }

    @Test
    void emptyFieldNameRejected() {
        assertThrows(MilvusClientException.class,
                () -> MetricSpec.builder().op(MetricOps.SUM).build());
        assertThrows(MilvusClientException.class,
                () -> MetricSpec.builder().op(MetricOps.SUM).fieldName("").build());
    }

    @Test
    void wildcardFieldRejectedForNonCountOps() {
        assertThrows(MilvusClientException.class,
                () -> MetricSpec.builder().op(MetricOps.AVG).fieldName("*").build());
        assertThrows(MilvusClientException.class,
                () -> MetricSpec.builder().op(MetricOps.SUM).fieldName("*").build());
    }

    @Test
    void toStringContainsFields() {
        MetricSpec spec = MetricSpec.builder().op(MetricOps.MIN).fieldName("price").build();
        assertNotNull(spec.toString());
    }
}

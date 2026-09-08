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
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class OrderByFieldTest {

    @Test
    void builderSetsFields() {
        OrderByField field = OrderByField.builder()
                .fieldName("color")
                .direction(AggDirection.DESC)
                .build();

        assertEquals("color", field.getFieldName());
        assertEquals(AggDirection.DESC, field.getDirection());
    }

    @Test
    void builderDefaultDirectionIsAsc() {
        OrderByField field = OrderByField.builder().fieldName("color").build();
        assertEquals(AggDirection.ASC, field.getDirection());
    }

    @Test
    void emptyFieldNameRejected() {
        assertThrows(MilvusClientException.class, () -> OrderByField.builder().build());
        assertThrows(MilvusClientException.class, () -> OrderByField.builder().fieldName("").build());
        assertThrows(MilvusClientException.class, () -> OrderByField.builder().fieldName(null).build());
    }

    @Test
    void nullDirectionRejected() {
        assertThrows(MilvusClientException.class,
                () -> OrderByField.builder().fieldName("color").direction(null).build());
    }

    @Test
    void toStringContainsFields() {
        OrderByField field = OrderByField.builder().fieldName("color").build();
        assertNotNull(field.toString());
    }
}

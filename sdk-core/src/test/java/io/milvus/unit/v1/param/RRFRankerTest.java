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

package io.milvus.unit.v1.param;

import io.milvus.exception.ParamException;
import io.milvus.param.dml.ranker.RRFRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class RRFRankerTest {

    @Test
    void builderSetsK() {
        RRFRanker ranker = RRFRanker.newBuilder()
                .withK(30)
                .build();

        assertEquals(30, ranker.getK());
    }

    @Test
    void kDefaultsToSixty() {
        RRFRanker ranker = RRFRanker.newBuilder().build();

        assertEquals(60, ranker.getK());
    }

    @Test
    void nullKIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> RRFRanker.newBuilder().withK(null));
    }

    @Test
    void negativeKIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> RRFRanker.newBuilder().withK(-1).build());
    }

    @Test
    void zeroKIsAllowed() {
        RRFRanker ranker = RRFRanker.newBuilder().withK(0).build();

        assertEquals(0, ranker.getK());
    }

    @Test
    void getPropertiesUsesRrfStrategy() {
        RRFRanker ranker = RRFRanker.newBuilder().withK(45).build();

        Map<String, String> properties = ranker.getProperties();
        assertEquals("rrf", properties.get("strategy"));
        assertEquals("{\"k\":45}", properties.get("params"));
    }
}

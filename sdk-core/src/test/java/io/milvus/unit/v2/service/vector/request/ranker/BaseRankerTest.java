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

package io.milvus.unit.v2.service.vector.request.ranker;

import io.milvus.param.dml.ranker.BaseRanker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class BaseRankerTest {

    @Test
    void abstractRankerExposesPropertiesMap() {
        BaseRanker ranker = new BaseRanker() {
            @Override
            public Map<String, String> getProperties() {
                Map<String, String> props = new HashMap<>();
                props.put("strategy", "rrf");
                props.put("k", "60");
                return props;
            }
        };

        Map<String, String> props = ranker.getProperties();
        assertNotNull(props);
        assertEquals("rrf", props.get("strategy"));
        assertEquals("60", props.get("k"));
        assertTrue(props.containsKey("strategy"));
    }
}

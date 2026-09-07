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

package io.milvus.unit.v2.service.vector.request.data;

import io.milvus.grpc.PlaceholderType;
import io.milvus.v2.service.vector.request.data.EmbeddedText;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class EmbeddedTextTest {

    @Test
    void constructorPreservesText() {
        EmbeddedText text = new EmbeddedText("search this text");
        assertEquals("search this text", text.getData());
    }

    @Test
    void placeholderTypeIsVarChar() {
        assertEquals(PlaceholderType.VarChar, new EmbeddedText("x").getPlaceholderType());
    }

    @Test
    void emptyStringSupported() {
        EmbeddedText text = new EmbeddedText("");
        assertEquals("", text.getData());
    }
}

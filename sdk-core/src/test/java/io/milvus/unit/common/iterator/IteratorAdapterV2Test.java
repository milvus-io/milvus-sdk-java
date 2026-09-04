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

package io.milvus.unit.common.iterator;
import io.milvus.orm.iterator.IteratorAdapterV2;

import io.milvus.param.collection.FieldType;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class IteratorAdapterV2Test {
    @Test
    void convertV2FieldUsesArrayMaxCapacity() {
        CreateCollectionReq.FieldSchema schema = CreateCollectionReq.FieldSchema.builder()
                .name("varchar_array")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxLength(65535)
                .maxCapacity(128)
                .build();

        FieldType fieldType = IteratorAdapterV2.convertV2Field(schema);

        assertEquals(65535, fieldType.getMaxLength());
        assertEquals(128, fieldType.getMaxCapacity());
    }
}

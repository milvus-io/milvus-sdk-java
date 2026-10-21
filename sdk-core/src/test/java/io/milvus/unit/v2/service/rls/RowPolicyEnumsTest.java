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

package io.milvus.unit.v2.service.rls;

import io.milvus.v2.common.RowPolicyAction;
import io.milvus.v2.common.RowPolicyType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RowPolicyEnumsTest {

    @Test
    void rowPolicyActionExposesNameAndCode() {
        assertEquals("UNRECOGNIZED", RowPolicyAction.UNKNOWN.getName());
        assertEquals(-1, RowPolicyAction.UNKNOWN.getCode());
        assertEquals("Query", RowPolicyAction.QUERY.getName());
        assertEquals(0, RowPolicyAction.QUERY.getCode());
        assertEquals("Search", RowPolicyAction.SEARCH.getName());
        assertEquals(1, RowPolicyAction.SEARCH.getCode());
        assertEquals("Insert", RowPolicyAction.INSERT.getName());
        assertEquals(2, RowPolicyAction.INSERT.getCode());
        assertEquals("Delete", RowPolicyAction.DELETE.getName());
        assertEquals(3, RowPolicyAction.DELETE.getCode());
        assertEquals("Upsert", RowPolicyAction.UPSERT.getName());
        assertEquals(4, RowPolicyAction.UPSERT.getCode());
        assertEquals("QueryIterator", RowPolicyAction.QUERY_ITERATOR.getName());
        assertEquals(5, RowPolicyAction.QUERY_ITERATOR.getCode());
        assertEquals("SearchIterator", RowPolicyAction.SEARCH_ITERATOR.getName());
        assertEquals(6, RowPolicyAction.SEARCH_ITERATOR.getCode());
        assertEquals("HybridSearch", RowPolicyAction.HYBRID_SEARCH.getName());
        assertEquals(7, RowPolicyAction.HYBRID_SEARCH.getCode());
    }

    @Test
    void rowPolicyActionFromNameAndCode() {
        assertEquals(RowPolicyAction.HYBRID_SEARCH, RowPolicyAction.fromName("HybridSearch"));
        assertEquals(RowPolicyAction.SEARCH_ITERATOR, RowPolicyAction.fromCode(6));
        assertEquals(RowPolicyAction.UNKNOWN, RowPolicyAction.fromCode(-1));
        assertNull(RowPolicyAction.fromName("unknown"));
        assertNull(RowPolicyAction.fromCode(99));
    }

    @Test
    void rowPolicyTypeExposesNameAndCode() {
        assertEquals("RowPolicyTypeUnknown", RowPolicyType.UNKNOWN.getName());
        assertEquals(0, RowPolicyType.UNKNOWN.getCode());
        assertEquals("Permissive", RowPolicyType.PERMISSIVE.getName());
        assertEquals(1, RowPolicyType.PERMISSIVE.getCode());
        assertEquals("Restrictive", RowPolicyType.RESTRICTIVE.getName());
        assertEquals(2, RowPolicyType.RESTRICTIVE.getCode());
    }

    @Test
    void rowPolicyTypeFromNameAndCode() {
        assertEquals(RowPolicyType.PERMISSIVE, RowPolicyType.fromName("Permissive"));
        assertEquals(RowPolicyType.RESTRICTIVE, RowPolicyType.fromCode(2));
        assertEquals(RowPolicyType.UNKNOWN, RowPolicyType.fromCode(0));
        assertNull(RowPolicyType.fromName("unknown"));
    }
}

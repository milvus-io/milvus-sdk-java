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
import io.milvus.param.highlevel.dml.response.SearchResponse;
import io.milvus.response.QueryResultsWrapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SearchResponseTest {
    @Test
    void buildAndGet() {
        List<QueryResultsWrapper.RowRecord> target0 = Arrays.asList(new QueryResultsWrapper.RowRecord());
        List<QueryResultsWrapper.RowRecord> target1 = Arrays.asList(
                new QueryResultsWrapper.RowRecord(), new QueryResultsWrapper.RowRecord());

        SearchResponse response = SearchResponse.builder()
                .rowRecords(Arrays.asList(target0, target1))
                .build();

        assertEquals(2, response.getRowRecords().size());
        assertEquals(target0, response.getRowRecords(0));
        assertEquals(target1, response.getRowRecords(1));
        assertEquals(target0, response.getRowRecordsForFirstTarget());
    }

    @Test
    void setRowRecords() {
        SearchResponse response = SearchResponse.builder().build();
        List<List<QueryResultsWrapper.RowRecord>> records = new ArrayList<>();
        records.add(Arrays.asList(new QueryResultsWrapper.RowRecord()));

        response.setRowRecords(records);

        assertEquals(records, response.getRowRecords());
    }

    @Test
    void getRowRecordsOutOfRangeThrows() {
        SearchResponse response = SearchResponse.builder()
                .rowRecords(new ArrayList<>())
                .build();

        assertNotNull(response.toString());
        assertThrows(ParamException.class, () -> response.getRowRecords(0));
    }
}

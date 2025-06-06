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

package io.milvus.v2.service.vector.response;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@SuperBuilder
public class SearchResp {
    @Builder.Default
    private List<List<SearchResult>> searchResults = new ArrayList<>();
    @Builder.Default
    private long sessionTs = 1L; // default eventually ts
    @Builder.Default
    private List<Float> recalls = new ArrayList<>();

    @Data
    @SuperBuilder
    public static class SearchResult {
        @Builder.Default
        private Map<String, Object> entity = new HashMap<>();
        private Float score;
        private Object id;
        @Builder.Default
        private String primaryKey = "id";

        @Override
        public String toString() {
            return "{" + getPrimaryKey() + ": " + getId() + ", Score: " + getScore() + ", OutputFields: " + entity + "}";
        }
    }
}

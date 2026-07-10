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

package io.milvus.v2.service.vector.response.aggregation;

import java.util.LinkedHashMap;
import java.util.Map;

public class AggregationHit {
    private final Object id;
    private final Float score;
    private final Map<String, Object> fields;
    private final Map<String, Long> fieldIds;

    private AggregationHit(AggregationHitBuilder builder) {
        this.id = builder.id;
        this.score = builder.score;
        this.fields = builder.fields;
        this.fieldIds = builder.fieldIds;
    }

    public static AggregationHitBuilder builder() {
        return new AggregationHitBuilder();
    }

    public Object getId() {
        return id;
    }

    public Float getScore() {
        return score;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public Map<String, Long> getFieldIds() {
        return fieldIds;
    }

    @Override
    public String toString() {
        return "AggregationHit{" +
                "id=" + id +
                ", score=" + score +
                ", fields=" + fields +
                ", fieldIds=" + fieldIds +
                '}';
    }

    public static class AggregationHitBuilder {
        private Object id;
        private Float score;
        private Map<String, Object> fields = new LinkedHashMap<>();
        private Map<String, Long> fieldIds = new LinkedHashMap<>();

        private AggregationHitBuilder() {
        }

        public AggregationHitBuilder id(Object id) {
            this.id = id;
            return this;
        }

        public AggregationHitBuilder score(Float score) {
            this.score = score;
            return this;
        }

        public AggregationHitBuilder fields(Map<String, Object> fields) {
            this.fields = fields;
            return this;
        }

        public AggregationHitBuilder fieldIds(Map<String, Long> fieldIds) {
            this.fieldIds = fieldIds;
            return this;
        }

        public AggregationHitBuilder addField(String fieldName, Object value, Long fieldId) {
            this.fields.put(fieldName, value);
            if (fieldId != null) {
                this.fieldIds.put(fieldName, fieldId);
            }
            return this;
        }

        public AggregationHit build() {
            return new AggregationHit(this);
        }
    }
}

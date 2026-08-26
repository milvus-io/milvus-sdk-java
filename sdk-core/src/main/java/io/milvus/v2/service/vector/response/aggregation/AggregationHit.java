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

/**
 * A single hit of a {@code top_hits} search aggregation, containing the entity ID,
 * score, and the projected field values of the hit.
 */
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

    /**
     * Creates a new builder for {@link AggregationHit}.
     *
     * @return a new builder
     */
    public static AggregationHitBuilder builder() {
        return new AggregationHitBuilder();
    }

    /**
     * Returns the ID of the entity this hit refers to.
     *
     * @return the entity ID
     */
    public Object getId() {
        return id;
    }

    /**
     * Returns the score of the hit.
     *
     * @return the hit score
     */
    public Float getScore() {
        return score;
    }

    /**
     * Returns the field values of the hit keyed by field name.
     *
     * @return the hit field values
     */
    public Map<String, Object> getFields() {
        return fields;
    }

    /**
     * Returns the IDs of the returned fields keyed by field name.
     *
     * @return the hit field IDs
     */
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

    /**
     * Builder for {@link AggregationHit}.
     */
    public static class AggregationHitBuilder {
        private Object id;
        private Float score;
        private Map<String, Object> fields = new LinkedHashMap<>();
        private Map<String, Long> fieldIds = new LinkedHashMap<>();

        private AggregationHitBuilder() {
        }

        /**
         * Sets the ID of the entity this hit refers to.
         *
         * @param id the entity ID
         * @return this builder
         */
        public AggregationHitBuilder id(Object id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the score of the hit.
         *
         * @param score the hit score
         * @return this builder
         */
        public AggregationHitBuilder score(Float score) {
            this.score = score;
            return this;
        }

        /**
         * Sets the field values of the hit keyed by field name.
         *
         * @param fields the hit field values
         * @return this builder
         */
        public AggregationHitBuilder fields(Map<String, Object> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Sets the IDs of the returned fields keyed by field name.
         *
         * @param fieldIds the hit field IDs
         * @return this builder
         */
        public AggregationHitBuilder fieldIds(Map<String, Long> fieldIds) {
            this.fieldIds = fieldIds;
            return this;
        }

        /**
         * Adds a single field value and, optionally, its field ID to the hit.
         *
         * @param fieldName the field name
         * @param value     the field value
         * @param fieldId   the field ID, or {@code null} to omit it
         * @return this builder
         */
        public AggregationHitBuilder addField(String fieldName, Object value, Long fieldId) {
            this.fields.put(fieldName, value);
            if (fieldId != null) {
                this.fieldIds.put(fieldName, fieldId);
            }
            return this;
        }

        /**
         * Builds the {@link AggregationHit}.
         *
         * @return the built hit
         */
        public AggregationHit build() {
            return new AggregationHit(this);
        }
    }
}

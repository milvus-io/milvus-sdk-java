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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A single bucket in the result of a {@code group-by} search aggregation. Each bucket
 * represents one group of matching entities identified by its {@link KeyEntry} key values,
 * together with the entity count, aggregated metric values, top hits, and any nested
 * sub-aggregation buckets.
 */
public class AggregationBucket {
    private final List<KeyEntry> key;
    private final long count;
    private final Map<String, Object> metrics;
    private final List<AggregationHit> hits;
    private final List<AggregationBucket> subGroups;

    private AggregationBucket(AggregationBucketBuilder builder) {
        this.key = builder.key;
        this.count = builder.count;
        this.metrics = builder.metrics;
        this.hits = builder.hits;
        this.subGroups = builder.subGroups;
    }

    /**
     * Creates a new builder for {@link AggregationBucket}.
     *
     * @return a new builder
     */
    public static AggregationBucketBuilder builder() {
        return new AggregationBucketBuilder();
    }

    /**
     * Returns the list of key entries that identify this bucket within its group.
     *
     * @return the bucket key entries
     */
    public List<KeyEntry> getKey() {
        return key;
    }

    /**
     * Returns the number of entities grouped into this bucket.
     *
     * @return the entity count
     */
    public long getCount() {
        return count;
    }

    /**
     * Returns the aggregated metric values keyed by their metric aliases.
     *
     * @return the metric values
     */
    public Map<String, Object> getMetrics() {
        return metrics;
    }

    /**
     * Returns the top hits of this bucket, if a {@code top_hits} aggregation was requested.
     *
     * @return the top hits, or an empty list if none were requested
     */
    public List<AggregationHit> getHits() {
        return hits;
    }

    /**
     * Returns the nested sub-aggregation buckets of this bucket.
     *
     * @return the sub-aggregation buckets
     */
    public List<AggregationBucket> getSubGroups() {
        return subGroups;
    }

    @Override
    public String toString() {
        return "AggregationBucket{" +
                "key=" + key +
                ", count=" + count +
                ", metrics=" + metrics +
                ", hits=" + hits +
                ", subGroups=" + subGroups +
                '}';
    }

    /**
     * Builder for {@link AggregationBucket}.
     */
    public static class AggregationBucketBuilder {
        private List<KeyEntry> key = new ArrayList<>();
        private long count;
        private Map<String, Object> metrics = new LinkedHashMap<>();
        private List<AggregationHit> hits = new ArrayList<>();
        private List<AggregationBucket> subGroups = new ArrayList<>();

        private AggregationBucketBuilder() {
        }

        /**
         * Sets the list of key entries that identify this bucket within its group.
         *
         * @param key the bucket key entries
         * @return this builder
         */
        public AggregationBucketBuilder key(List<KeyEntry> key) {
            this.key = key;
            return this;
        }

        /**
         * Sets the number of entities grouped into this bucket.
         *
         * @param count the entity count
         * @return this builder
         */
        public AggregationBucketBuilder count(long count) {
            this.count = count;
            return this;
        }

        /**
         * Sets the aggregated metric values keyed by their metric aliases.
         *
         * @param metrics the metric values
         * @return this builder
         */
        public AggregationBucketBuilder metrics(Map<String, Object> metrics) {
            this.metrics = metrics;
            return this;
        }

        /**
         * Sets the top hits of this bucket.
         *
         * @param hits the top hits
         * @return this builder
         */
        public AggregationBucketBuilder hits(List<AggregationHit> hits) {
            this.hits = hits;
            return this;
        }

        /**
         * Sets the nested sub-aggregation buckets of this bucket.
         *
         * @param subGroups the sub-aggregation buckets
         * @return this builder
         */
        public AggregationBucketBuilder subGroups(List<AggregationBucket> subGroups) {
            this.subGroups = subGroups;
            return this;
        }

        /**
         * Builds the {@link AggregationBucket}.
         *
         * @return the built bucket
         */
        public AggregationBucket build() {
            return new AggregationBucket(this);
        }
    }

    /**
     * A single field entry of a bucket key, describing one grouped field of the
     * aggregation together with its value.
     */
    public static class KeyEntry {
        private final long fieldId;
        private final String fieldName;
        private final Object value;

        private KeyEntry(KeyEntryBuilder builder) {
            this.fieldId = builder.fieldId;
            this.fieldName = builder.fieldName;
            this.value = builder.value;
        }

        /**
         * Creates a new builder for {@link KeyEntry}.
         *
         * @return a new builder
         */
        public static KeyEntryBuilder builder() {
            return new KeyEntryBuilder();
        }

        /**
         * Returns the ID of the field this key entry refers to.
         *
         * @return the field ID
         */
        public long getFieldId() {
            return fieldId;
        }

        /**
         * Returns the name of the field this key entry refers to.
         *
         * @return the field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * Returns the grouped field value of this key entry.
         *
         * @return the field value
         */
        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "KeyEntry{" +
                    "fieldId=" + fieldId +
                    ", fieldName='" + fieldName + '\'' +
                    ", value=" + value +
                    '}';
        }

        /**
         * Builder for {@link KeyEntry}.
         */
        public static class KeyEntryBuilder {
            private long fieldId;
            private String fieldName;
            private Object value;

            private KeyEntryBuilder() {
            }

            /**
             * Sets the ID of the field this key entry refers to.
             *
             * @param fieldId the field ID
             * @return this builder
             */
            public KeyEntryBuilder fieldId(long fieldId) {
                this.fieldId = fieldId;
                return this;
            }

            /**
             * Sets the name of the field this key entry refers to.
             *
             * @param fieldName the field name
             * @return this builder
             */
            public KeyEntryBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            /**
             * Sets the grouped field value of this key entry.
             *
             * @param value the field value
             * @return this builder
             */
            public KeyEntryBuilder value(Object value) {
                this.value = value;
                return this;
            }

            /**
             * Builds the {@link KeyEntry}.
             *
             * @return the built key entry
             */
            public KeyEntry build() {
                return new KeyEntry(this);
            }
        }
    }
}

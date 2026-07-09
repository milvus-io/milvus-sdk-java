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

    public static AggregationBucketBuilder builder() {
        return new AggregationBucketBuilder();
    }

    public List<KeyEntry> getKey() {
        return key;
    }

    public long getCount() {
        return count;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public List<AggregationHit> getHits() {
        return hits;
    }

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

    public static class AggregationBucketBuilder {
        private List<KeyEntry> key = new ArrayList<>();
        private long count;
        private Map<String, Object> metrics = new LinkedHashMap<>();
        private List<AggregationHit> hits = new ArrayList<>();
        private List<AggregationBucket> subGroups = new ArrayList<>();

        private AggregationBucketBuilder() {
        }

        public AggregationBucketBuilder key(List<KeyEntry> key) {
            this.key = key;
            return this;
        }

        public AggregationBucketBuilder count(long count) {
            this.count = count;
            return this;
        }

        public AggregationBucketBuilder metrics(Map<String, Object> metrics) {
            this.metrics = metrics;
            return this;
        }

        public AggregationBucketBuilder hits(List<AggregationHit> hits) {
            this.hits = hits;
            return this;
        }

        public AggregationBucketBuilder subGroups(List<AggregationBucket> subGroups) {
            this.subGroups = subGroups;
            return this;
        }

        public AggregationBucket build() {
            return new AggregationBucket(this);
        }
    }

    public static class KeyEntry {
        private final long fieldId;
        private final String fieldName;
        private final Object value;

        private KeyEntry(KeyEntryBuilder builder) {
            this.fieldId = builder.fieldId;
            this.fieldName = builder.fieldName;
            this.value = builder.value;
        }

        public static KeyEntryBuilder builder() {
            return new KeyEntryBuilder();
        }

        public long getFieldId() {
            return fieldId;
        }

        public String getFieldName() {
            return fieldName;
        }

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

        public static class KeyEntryBuilder {
            private long fieldId;
            private String fieldName;
            private Object value;

            private KeyEntryBuilder() {
            }

            public KeyEntryBuilder fieldId(long fieldId) {
                this.fieldId = fieldId;
                return this;
            }

            public KeyEntryBuilder fieldName(String fieldName) {
                this.fieldName = fieldName;
                return this;
            }

            public KeyEntryBuilder value(Object value) {
                this.value = value;
                return this;
            }

            public KeyEntry build() {
                return new KeyEntry(this);
            }
        }
    }
}

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

package io.milvus.v2.service.vector.request.aggregation;

import io.milvus.grpc.SearchAggregationSpec;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SearchAggregation {
    private static final List<String> SPECIAL_ORDER_KEYS = Arrays.asList("_count", "_key");

    private final List<String> fields;
    private final long size;
    private final Map<String, MetricSpec> metrics;
    private final List<OrderSpec> order;
    private final TopHitsSpec topHits;
    private final SearchAggregation subAggregation;

    private SearchAggregation(SearchAggregationBuilder builder) {
        if (builder.fields.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation.fields must be a non-empty list.");
        }
        for (String field : builder.fields) {
            if (field == null || field.isEmpty()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "SearchAggregation.fields must not contain empty values.");
            }
        }
        if (builder.size <= 0) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "SearchAggregation.size must be a positive integer.");
        }
        validateOrder(builder.order, builder.metrics);

        this.fields = Collections.unmodifiableList(new ArrayList<>(builder.fields));
        this.size = builder.size;
        this.metrics = Collections.unmodifiableMap(new LinkedHashMap<>(builder.metrics));
        this.order = Collections.unmodifiableList(new ArrayList<>(builder.order));
        this.topHits = builder.topHits;
        this.subAggregation = builder.subAggregation;
    }

    public static SearchAggregationBuilder builder() {
        return new SearchAggregationBuilder();
    }

    public List<String> getFields() {
        return fields;
    }

    public long getSize() {
        return size;
    }

    public Map<String, MetricSpec> getMetrics() {
        return metrics;
    }

    public List<OrderSpec> getOrder() {
        return order;
    }

    public TopHitsSpec getTopHits() {
        return topHits;
    }

    public SearchAggregation getSubAggregation() {
        return subAggregation;
    }

    public SearchAggregationSpec toProto() {
        SearchAggregationSpec.Builder builder = SearchAggregationSpec.newBuilder()
                .addAllFields(fields)
                .setSize(size);
        for (Map.Entry<String, MetricSpec> entry : metrics.entrySet()) {
            builder.putMetrics(entry.getKey(), entry.getValue().toProto());
        }
        for (OrderSpec item : order) {
            builder.addOrder(item.toProto());
        }
        if (topHits != null) {
            builder.setTopHits(topHits.toProto());
        }
        if (subAggregation != null) {
            builder.setSubAggregation(subAggregation.toProto());
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "SearchAggregation{" +
                "fields=" + fields +
                ", size=" + size +
                ", metrics=" + metrics +
                ", order=" + order +
                ", topHits=" + topHits +
                ", subAggregation=" + subAggregation +
                '}';
    }

    private static void validateOrder(List<OrderSpec> order, Map<String, MetricSpec> metrics) {
        for (OrderSpec item : order) {
            String key = item.getKey();
            if (!metrics.containsKey(key) && !SPECIAL_ORDER_KEYS.contains(key)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "SearchAggregation.order key must be a metric alias or one of _count/_key.");
            }
        }
    }

    public static class SearchAggregationBuilder {
        private final List<String> fields = new ArrayList<>();
        private long size;
        private final Map<String, MetricSpec> metrics = new LinkedHashMap<>();
        private final List<OrderSpec> order = new ArrayList<>();
        private TopHitsSpec topHits;
        private SearchAggregation subAggregation;

        private SearchAggregationBuilder() {
        }

        public SearchAggregationBuilder fields(List<String> fields) {
            this.fields.clear();
            if (fields != null) {
                this.fields.addAll(fields);
            }
            return this;
        }

        public SearchAggregationBuilder addField(String field) {
            this.fields.add(field);
            return this;
        }

        public SearchAggregationBuilder size(long size) {
            this.size = size;
            return this;
        }

        public SearchAggregationBuilder metrics(Map<String, MetricSpec> metrics) {
            this.metrics.clear();
            if (metrics != null) {
                metrics.forEach(this::addMetric);
            }
            return this;
        }

        public SearchAggregationBuilder addMetric(String alias, MetricSpec metric) {
            if (alias == null || alias.isEmpty()) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "SearchAggregation metric alias must not be empty.");
            }
            if (metric == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "SearchAggregation metric must not be null.");
            }
            this.metrics.put(alias, metric);
            return this;
        }

        public SearchAggregationBuilder order(List<OrderSpec> order) {
            this.order.clear();
            if (order != null) {
                order.forEach(this::addOrder);
            }
            return this;
        }

        public SearchAggregationBuilder addOrder(OrderSpec order) {
            if (order == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "SearchAggregation.order entry must not be null.");
            }
            this.order.add(order);
            return this;
        }

        public SearchAggregationBuilder topHits(TopHitsSpec topHits) {
            this.topHits = topHits;
            return this;
        }

        public SearchAggregationBuilder subAggregation(SearchAggregation subAggregation) {
            this.subAggregation = subAggregation;
            return this;
        }

        public SearchAggregation build() {
            return new SearchAggregation(this);
        }
    }


}

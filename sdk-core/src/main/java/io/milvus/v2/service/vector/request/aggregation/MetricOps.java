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

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

/**
 * The metric aggregation operations supported by a {@code group-by} search aggregation.
 */
public enum MetricOps {
    /**
     * Average of the values of the field.
     */
    AVG("avg"),
    /**
     * Sum of the values of the field.
     */
    SUM("sum"),
    /**
     * Count of the entities, typically applied to the special field {@code "*"}.
     */
    COUNT("count"),
    /**
     * Minimum value of the field.
     */
    MIN("min"),
    /**
     * Maximum value of the field.
     */
    MAX("max");

    private final String value;

    MetricOps(String value) {
        this.value = value;
    }

    /**
     * Returns the string value of this operation.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    /**
     * Converts a string value into a {@link MetricOps}.
     *
     * @param value the string value
     * @return the matching operation
     * @throws MilvusClientException if the value is not one of {@code avg}, {@code sum},
     *                               {@code count}, {@code min}, or {@code max}
     */
    public static MetricOps fromValue(String value) {
        for (MetricOps op : values()) {
            if (op.value.equals(value)) {
                return op;
            }
        }
        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                "SearchAggregation metric op must be one of [avg, sum, count, min, max].");
    }
}

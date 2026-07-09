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

public enum MetricOps {
    AVG("avg"),
    SUM("sum"),
    COUNT("count"),
    MIN("min"),
    MAX("max");

    private final String value;

    MetricOps(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

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

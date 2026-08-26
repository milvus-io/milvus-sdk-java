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
 * The sort direction used by a {@code group-by} search aggregation ordering rule.
 */
public enum AggDirection {
    /**
     * Ascending sort order.
     */
    ASC("asc"),
    /**
     * Descending sort order.
     */
    DESC("desc");

    private final String value;

    AggDirection(String value) {
        this.value = value;
    }

    /**
     * Returns the string value of this direction.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    /**
     * Converts a string value into an {@link AggDirection}.
     *
     * @param value     the string value, either {@code asc} or {@code desc}
     * @param fieldName the field name used in the error message when the value is invalid
     * @return the matching direction
     * @throws MilvusClientException if the value is neither {@code asc} nor {@code desc}
     */
    public static AggDirection fromValue(String value, String fieldName) {
        for (AggDirection direction : values()) {
            if (direction.value.equals(value)) {
                return direction;
            }
        }
        throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                fieldName + " must be 'asc' or 'desc'.");
    }
}

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

package io.milvus.v2.common;

/**
 * Type of a row policy.
 */

public enum RowPolicyType {
    /**
     * Unknown policy type. Returned when the server reports a type that has no
     * mapped value (for example proto code 0).
     */
    UNKNOWN("RowPolicyTypeUnknown", 0),
    /**
     * Permissive policy type. A row matches when any permissive policy applies.
     */
    PERMISSIVE("Permissive", 1),
    /**
     * Restrictive policy type. A row must match all restrictive policies.
     */
    RESTRICTIVE("Restrictive", 2),
    ;
    private final String name;
    private final int code;

    RowPolicyType(String name, int code) {
        this.name = name;
        this.code = code;
    }

    /**
     * Returns the display name of the row policy type.
     *
     * @return the display name
     */

    public String getName() {
        return name;
    }

    /**
     * Returns the numeric code of the row policy type.
     *
     * @return the numeric code
     */

    public int getCode() {
        return code;
    }

    /**
     * Returns the row policy type that matches the given display name.
     *
     * @param name the display name of the row policy type
     * @return the matching row policy type, or {@code null} if none matches
     */

    public static RowPolicyType fromName(String name) {
        for (RowPolicyType type : RowPolicyType.values()) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    /**
     * Returns the row policy type that matches the given numeric code.
     *
     * @param code the numeric code of the row policy type
     * @return the matching row policy type, or {@code null} if none matches
     */

    public static RowPolicyType fromCode(int code) {
        for (RowPolicyType type : RowPolicyType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return null;
    }
}

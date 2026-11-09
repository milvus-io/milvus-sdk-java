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
 * Operation action governed by a row policy.
 */

public enum RowPolicyAction {
    /**
     * Unknown action. Returned when the server reports an action code that has no
     * mapped value (for example proto UNRECOGNIZED = -1 or a future action code).
     */
    UNKNOWN("UNRECOGNIZED", -1),
    /**
     * Query action.
     */
    QUERY("Query", 0),
    /**
     * Search action.
     */
    SEARCH("Search", 1),
    /**
     * Insert action.
     */
    INSERT("Insert", 2),
    /**
     * Delete action.
     */
    DELETE("Delete", 3),
    /**
     * Upsert action.
     */
    UPSERT("Upsert", 4),
    /**
     * Query iterator action.
     */
    QUERY_ITERATOR("QueryIterator", 5),
    /**
     * Search iterator action.
     */
    SEARCH_ITERATOR("SearchIterator", 6),
    /**
     * Hybrid search action.
     */
    HYBRID_SEARCH("HybridSearch", 7),
    ;
    private final String name;
    private final int code;

    RowPolicyAction(String name, int code) {
        this.name = name;
        this.code = code;
    }

    /**
     * Returns the display name of the row policy action.
     *
     * @return the display name
     */

    public String getName() {
        return name;
    }

    /**
     * Returns the numeric code of the row policy action.
     *
     * @return the numeric code
     */

    public int getCode() {
        return code;
    }

    /**
     * Returns the row policy action that matches the given display name.
     *
     * @param name the display name of the row policy action
     * @return the matching row policy action, or {@code null} if none matches
     */

    public static RowPolicyAction fromName(String name) {
        for (RowPolicyAction action : RowPolicyAction.values()) {
            if (action.getName().equals(name)) {
                return action;
            }
        }
        return null;
    }

    /**
     * Returns the row policy action that matches the given numeric code.
     *
     * @param code the numeric code of the row policy action
     * @return the matching row policy action, or {@code null} if none matches
     */

    public static RowPolicyAction fromCode(int code) {
        for (RowPolicyAction action : RowPolicyAction.values()) {
            if (action.getCode() == code) {
                return action;
            }
        }
        return null;
    }
}

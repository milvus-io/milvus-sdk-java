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
 * Consistency level for search and query operations on a collection.
 */
public enum ConsistencyLevel {
    /**
     * Reads always see the latest data after a write completes.
     */
    STRONG("Strong", 0),
    /**
     * Reads within the same session see the effects of writes from that session.
     */
    SESSION("Session", 1),
    /**
     * Reads see acknowledged writes and tolerate bounded staleness.
     */
    BOUNDED("Bounded", 2),
    /**
     * Reads are eventually consistent and may serve stale data.
     */
    EVENTUALLY("Eventually", 3),
    ;
    private final String name;
    private final int code;

    ConsistencyLevel(String name, int code) {
        this.name = name;
        this.code = code;
    }

    /**
     * Returns the display name of the consistency level.
     *
     * @return the display name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the numeric code of the consistency level.
     *
     * @return the numeric code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the consistency level that matches the given display name.
     *
     * @param name the display name of the consistency level
     * @return the matching consistency level, or {@code null} if none matches
     */
    public static ConsistencyLevel fromName(String name) {
        for (ConsistencyLevel level : ConsistencyLevel.values()) {
            if (level.getName().equals(name)) {
                return level;
            }
        }
        return null;
    }
}

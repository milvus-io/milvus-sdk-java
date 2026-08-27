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

package io.milvus.common.clientenum;

/**
 * The consistency level used by Milvus read operations such as search and query.
 *
 * <p>It controls the visibility of recently written data during reads and trades off between
 * consistency guarantees and read latency. Values map to the protocol-defined consistency level
 * codes, with {@code 0} as the strongest and {@code 3} as the weakest.
 */
public enum ConsistencyLevelEnum {

    /**
     * The strongest consistency level, guaranteeing that reads always see the latest writes.
     */
    STRONG("Strong", 0),
    /**
     * A per-session consistency level, guaranteeing the same session always reads the data it has
     * written.
     */
    SESSION("Session", 1),
    /**
     * A bounded consistency level, guaranteeing that reads see writes that are within a bounded
     * time interval.
     */
    BOUNDED("Bounded", 2),
    /**
     * The eventual consistency level, without any strict guarantee on the freshness of reads.
     */
    EVENTUALLY("Eventually", 3),
    ;

    private final String name;
    private final int code;

    ConsistencyLevelEnum(String name, int code) {
        this.name = name;
        this.code = code;
    }

    // Getter methods to replace @Getter annotations
    /**
     * Returns the name of the consistency level.
     *
     * @return the consistency level name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the protocol code of the consistency level.
     *
     * @return the consistency level code
     */
    public int getCode() {
        return code;
    }

    private static final ConsistencyLevelEnum[] CONSISTENCY_LEVELS = values();

    /**
     * Returns the consistency level whose code matches the given value.
     *
     * @param code the consistency level code
     * @return the matching consistency level, or {@code null} if no level has the given code
     */
    public static ConsistencyLevelEnum getNameByCode(int code) {
        if (code >= 0 && code < CONSISTENCY_LEVELS.length) {
            return CONSISTENCY_LEVELS[code];
        }
        return null;
    }
}

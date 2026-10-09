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
 * State of a compaction job as reported by the {@code getCompactionState} API.
 */


public enum CompactionState {
    /**
     * The compaction state is undefined.
     */
    UndefinedState(0),
    /**
     * Deprecated alias of {@link #UndefinedState}, kept for backward compatibility.
     *
     * @deprecated use {@link #UndefinedState} instead.
     */
    @Deprecated
    UndefiedState(0),
    /**
     * The compaction job is being executed.
     */
    Executing(1),
    /**
     * The compaction job has completed.
     */
    Completed(2);

    private final int code;

    CompactionState(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code of the compaction state.
     *
     * @return the numeric code
     */


    public int getCode() {
        return code;
    }

    /**
     * Returns the canonical compaction state for the given numeric code.
     *
     * <p>The proto enum spells the undefined value {@code UndefiedState}; this lookup maps it to
     * {@link #UndefinedState} so consumers comparing against the corrected constant always match.
     *
     * @param code the numeric code
     * @return the canonical compaction state
     * @throws IllegalArgumentException if the code is unknown
     */
    public static CompactionState forNumber(int code) {
        for (CompactionState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown CompactionState code: " + code);
    }
}

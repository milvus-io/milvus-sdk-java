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
 * State of a compaction task as reported per plan by the {@code getCompactionPlans} and
 * {@code listCompactionTasks} APIs.
 */


public enum CompactionTaskState {
    /**
     * The task state is unknown.
     */
    Unknown(0),
    /**
     * The task is being executed.
     */
    Executing(1),
    /**
     * The task is waiting in the pipeline.
     */
    Pipelining(2),
    /**
     * The task has completed.
     */
    Completed(3),
    /**
     * The task has failed.
     */
    Failed(4),
    /**
     * The task has timed out.
     */
    Timeout(5),
    /**
     * The task is analyzing.
     */
    Analyzing(6),
    /**
     * The task is indexing.
     */
    Indexing(7),
    /**
     * The task has been cleaned up.
     */
    Cleaned(8),
    /**
     * The task metadata has been saved.
     */
    MetaSaved(9),
    /**
     * The task is collecting statistics.
     */
    Statistic(10);

    private final int code;

    CompactionTaskState(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code of the compaction task state.
     *
     * @return the numeric code
     */


    public int getCode() {
        return code;
    }
}

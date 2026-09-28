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
 * Type of a compaction task as reported per plan by the {@code getCompactionPlans} and
 * {@code listCompactionTasks} APIs.
 */


public enum CompactionType {
    /**
     * The compaction type is undefined.
     */
    Undefined(0),
    /**
     * The compaction merges segments.
     */
    Merge(2),
    /**
     * The compaction mixes multiple segment operations.
     */
    Mix(3),
    /**
     * The compaction acts on a single segment.
     */
    Single(4),
    /**
     * The compaction is a minor compaction.
     */
    Minor(5),
    /**
     * The compaction is a major compaction.
     */
    Major(6),
    /**
     * The compaction removes level-0 delete records.
     */
    Level0Delete(7),
    /**
     * The compaction is a clustering compaction.
     */
    Clustering(8),
    /**
     * The compaction sorts the segment.
     */
    Sort(9),
    /**
     * The compaction sorts the segment by partition key.
     */
    PartitionKeySort(10),
    /**
     * The compaction is a clustering and partition-key sort compaction.
     */
    ClusteringPartitionKeySort(11),
    /**
     * The compaction bumps the schema version.
     */
    BumpSchemaVersion(12);

    private final int code;

    CompactionType(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code of the compaction type.
     *
     * @return the numeric code
     */


    public int getCode() {
        return code;
    }
}

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

import java.util.ArrayList;
import java.util.List;

/**
 * A compaction plan describing which segments are compacted into a target segment, as returned by
 * the {@code getCompactionPlans} and {@code listCompactionTasks} APIs. Each plan carries the
 * per-task metadata of one compaction task (plan id, trigger id, state, failure reason, ...).
 */


public class CompactionPlan {
    private Long planId;
    private Long triggerId;
    private Long collectionId;
    private Long partitionId;
    private String channel;
    private CompactionType compactionType;
    private CompactionTaskState state;
    private String failureReason;
    private Long target;
    private List<Long> targets;
    private List<Long> sources;

    private CompactionPlan(CompactionPlanBuilder builder) {
        this.planId = builder.planId;
        this.triggerId = builder.triggerId;
        this.collectionId = builder.collectionId;
        this.partitionId = builder.partitionId;
        this.channel = builder.channel;
        this.compactionType = builder.compactionType;
        this.state = builder.state;
        this.failureReason = builder.failureReason;
        this.target = builder.target;
        this.targets = builder.targets;
        this.sources = builder.sources;
    }

    /**
     * Creates a new {@code CompactionPlan} builder.
     *
     * @return the builder
     */


    public static CompactionPlanBuilder builder() {
        return new CompactionPlanBuilder();
    }

    /**
     * Returns the ID of the compaction plan.
     *
     * @return the plan ID
     */


    public Long getPlanId() {
        return planId;
    }

    /**
     * Returns the ID of the compaction that triggered this plan.
     *
     * @return the trigger ID
     */


    public Long getTriggerId() {
        return triggerId;
    }

    /**
     * Returns the ID of the collection being compacted.
     *
     * @return the collection ID
     */


    public Long getCollectionId() {
        return collectionId;
    }

    /**
     * Returns the ID of the partition being compacted.
     *
     * @return the partition ID
     */


    public Long getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the channel on which the compaction was scheduled.
     *
     * @return the channel name
     */


    public String getChannel() {
        return channel;
    }

    /**
     * Returns the type of the compaction task.
     *
     * @return the compaction type
     */


    public CompactionType getCompactionType() {
        return compactionType;
    }

    /**
     * Returns the state of the compaction task.
     *
     * @return the compaction task state
     */


    public CompactionTaskState getState() {
        return state;
    }

    /**
     * Returns the failure reason of the compaction task, empty when it did not fail.
     *
     * @return the failure reason
     */


    public String getFailureReason() {
        return failureReason;
    }

    /**
     * Returns the ID of the target segment produced by the compaction.
     *
     * @return the target segment ID
     */


    public Long getTarget() {
        return this.target;
    }

    /**
     * Returns the complete set of target segments produced by the compaction, which may be larger
     * than the single legacy {@link #getTarget()}.
     *
     * @return the target segment IDs
     */


    public List<Long> getTargets() {
        return this.targets;
    }

    /**
     * Returns the IDs of the source segments that are compacted.
     *
     * @return the source segment IDs
     */


    public List<Long> getSources() {
        return this.sources;
    }

    @Override
    public String toString() {
        return "CompactionPlan{" +
                "planId=" + planId +
                ", triggerId=" + triggerId +
                ", collectionId=" + collectionId +
                ", partitionId=" + partitionId +
                ", channel='" + channel + '\'' +
                ", compactionType=" + compactionType +
                ", state=" + state +
                ", failureReason='" + failureReason + '\'' +
                ", target=" + target +
                ", targets=" + targets +
                ", sources=" + sources +
                '}';
    }

    /**
     * Builder for {@link CompactionPlan}.
     */


    public static class CompactionPlanBuilder {
        private Long planId;
        private Long triggerId;
        private Long collectionId;
        private Long partitionId;
        private String channel;
        private CompactionType compactionType;
        private CompactionTaskState state;
        private String failureReason;
        private Long target = 0L;
        private List<Long> targets = new ArrayList<>();
        private List<Long> sources = new ArrayList<>();

        /**
         * Sets the ID of the compaction plan.
         *
         * @param planId the plan ID
         * @return this builder
         */


        public CompactionPlanBuilder planId(long planId) {
            this.planId = planId;
            return this;
        }

        /**
         * Sets the ID of the compaction that triggered this plan.
         *
         * @param triggerId the trigger ID
         * @return this builder
         */


        public CompactionPlanBuilder triggerId(long triggerId) {
            this.triggerId = triggerId;
            return this;
        }

        /**
         * Sets the ID of the collection being compacted.
         *
         * @param collectionId the collection ID
         * @return this builder
         */


        public CompactionPlanBuilder collectionId(long collectionId) {
            this.collectionId = collectionId;
            return this;
        }

        /**
         * Sets the ID of the partition being compacted.
         *
         * @param partitionId the partition ID
         * @return this builder
         */


        public CompactionPlanBuilder partitionId(long partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        /**
         * Sets the channel on which the compaction was scheduled.
         *
         * @param channel the channel name
         * @return this builder
         */


        public CompactionPlanBuilder channel(String channel) {
            this.channel = channel;
            return this;
        }

        /**
         * Sets the type of the compaction task.
         *
         * @param compactionType the compaction type
         * @return this builder
         */


        public CompactionPlanBuilder compactionType(CompactionType compactionType) {
            this.compactionType = compactionType;
            return this;
        }

        /**
         * Sets the state of the compaction task.
         *
         * @param state the compaction task state
         * @return this builder
         */


        public CompactionPlanBuilder state(CompactionTaskState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the failure reason of the compaction task.
         *
         * @param failureReason the failure reason
         * @return this builder
         */


        public CompactionPlanBuilder failureReason(String failureReason) {
            this.failureReason = failureReason;
            return this;
        }

        /**
         * Sets the target segment ID produced by the compaction.
         *
         * @param target the target segment ID
         * @return this builder
         */


        public CompactionPlanBuilder target(long target) {
            this.target = target;
            return this;
        }

        /**
         * Sets the complete set of target segments produced by the compaction.
         *
         * @param targets the target segment IDs
         * @return this builder
         */


        public CompactionPlanBuilder targets(List<Long> targets) {
            this.targets = targets;
            return this;
        }

        /**
         * Sets the source segment IDs to compact.
         *
         * @param sources the source segment IDs
         * @return this builder
         */


        public CompactionPlanBuilder sources(List<Long> sources) {
            this.sources = sources;
            return this;
        }

        /**
         * Builds the {@link CompactionPlan}.
         *
         * @return the compaction plan
         */


        public CompactionPlan build() {
            return new CompactionPlan(this);
        }
    }
}

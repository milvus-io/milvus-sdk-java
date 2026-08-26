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
 * the {@code getCompactionPlans} API.
 */
public class CompactionPlan {
    private Long target;
    private List<Long> sources;

    private CompactionPlan(CompactionPlanBuilder builder) {
        this.target = builder.target;
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
     * Returns the ID of the target segment produced by the compaction.
     *
     * @return the target segment ID
     */
    public Long getTarget() {
        return this.target;
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
                "target=" + target +
                ", sources=" + sources +
                '}';
    }

    /**
     * Builder for {@link CompactionPlan}.
     */
    public static class CompactionPlanBuilder {
        private Long target = 0L;
        private List<Long> sources = new ArrayList<>();

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

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

public class CompactionPlan {
    private Long target;
    private List<Long> sources;

    private CompactionPlan(CompactionPlanBuilder builder) {
        this.target = builder.target;
        this.sources = builder.sources;
    }

    public static CompactionPlanBuilder builder() {
        return new CompactionPlanBuilder();
    }

    public Long getTarget() {
        return this.target;
    }

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

    public static class CompactionPlanBuilder {
        private Long target = 0L;
        private List<Long> sources = new ArrayList<>();

        public CompactionPlanBuilder target(long target) {
            this.target = target;
            return this;
        }

        public CompactionPlanBuilder sources(List<Long> sources) {
            this.sources = sources;
            return this;
        }

        public CompactionPlan build() {
            return new CompactionPlan(this);
        }
    }
}

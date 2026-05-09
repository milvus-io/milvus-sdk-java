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

package io.milvus.v2.service.snapshot.response;

import java.util.ArrayList;
import java.util.List;

public class ListRestoreSnapshotJobsResp {
    private List<RestoreSnapshotJobInfo> jobs;

    private ListRestoreSnapshotJobsResp(ListRestoreSnapshotJobsRespBuilder builder) {
        this.jobs = builder.jobs == null ? new ArrayList<>() : builder.jobs;
    }

    public static ListRestoreSnapshotJobsRespBuilder builder() {
        return new ListRestoreSnapshotJobsRespBuilder();
    }

    public List<RestoreSnapshotJobInfo> getJobs() {
        return jobs;
    }

    public void setJobs(List<RestoreSnapshotJobInfo> jobs) {
        this.jobs = jobs;
    }

    @Override
    public String toString() {
        return "ListRestoreSnapshotJobsResp{" +
                "jobs=" + jobs +
                '}';
    }

    public static class ListRestoreSnapshotJobsRespBuilder {
        private List<RestoreSnapshotJobInfo> jobs = new ArrayList<>();

        public ListRestoreSnapshotJobsRespBuilder jobs(List<RestoreSnapshotJobInfo> jobs) {
            this.jobs = jobs;
            return this;
        }

        public ListRestoreSnapshotJobsResp build() {
            return new ListRestoreSnapshotJobsResp(this);
        }
    }
}

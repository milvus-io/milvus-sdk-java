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

package io.milvus.v2.service.utility.response;

import java.util.List;

public class ListRefreshExternalCollectionJobsResp {
    private final List<RefreshExternalCollectionJobInfo> jobs;

    private ListRefreshExternalCollectionJobsResp(ListRefreshExternalCollectionJobsRespBuilder builder) {
        this.jobs = builder.jobs;
    }

    public static ListRefreshExternalCollectionJobsRespBuilder builder() {
        return new ListRefreshExternalCollectionJobsRespBuilder();
    }

    public List<RefreshExternalCollectionJobInfo> getJobs() {
        return jobs;
    }

    @Override
    public String toString() {
        return "ListRefreshExternalCollectionJobsResp{" +
                "jobs=" + jobs +
                '}';
    }

    public static class ListRefreshExternalCollectionJobsRespBuilder {
        private List<RefreshExternalCollectionJobInfo> jobs;

        public ListRefreshExternalCollectionJobsRespBuilder jobs(List<RefreshExternalCollectionJobInfo> jobs) {
            this.jobs = jobs;
            return this;
        }

        public ListRefreshExternalCollectionJobsResp build() {
            return new ListRefreshExternalCollectionJobsResp(this);
        }
    }
}

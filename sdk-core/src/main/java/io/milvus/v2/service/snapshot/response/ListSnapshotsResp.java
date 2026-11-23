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

/**
 * Response returned by the {@code listSnapshots} API.
 */


public class ListSnapshotsResp {
    private List<String> snapshots;
    private List<SnapshotSummaryInfo> snapshotSummaries;

    private ListSnapshotsResp(ListSnapshotsRespBuilder builder) {
        this.snapshots = builder.snapshots == null ? new ArrayList<>() : builder.snapshots;
        this.snapshotSummaries = builder.snapshotSummaries == null ? new ArrayList<>() : builder.snapshotSummaries;
    }

    /**
     * Creates a new builder for {@code ListSnapshotsResp}.
     *
     * @return the builder
     */


    public static ListSnapshotsRespBuilder builder() {
        return new ListSnapshotsRespBuilder();
    }

    /**
     * Returns the list of snapshot names.
     *
     * @return the list of snapshot names
     */


    public List<String> getSnapshots() {
        return snapshots;
    }

    /**
     * Sets the list of snapshot names.
     *
     * @param snapshots the list of snapshot names
     */


    public void setSnapshots(List<String> snapshots) {
        this.snapshots = snapshots;
    }

    /**
     * Returns the lightweight snapshot summaries in server-provided order
     * ({@code createTs} descending, then {@code name} ascending).
     *
     * <p>When connected to an older server that returns snapshot names only, this list is empty.</p>
     *
     * @return the list of snapshot summaries
     */


    public List<SnapshotSummaryInfo> getSnapshotSummaries() {
        return snapshotSummaries;
    }

    /**
     * Sets the list of lightweight snapshot summaries.
     *
     * @param snapshotSummaries the list of snapshot summaries
     */


    public void setSnapshotSummaries(List<SnapshotSummaryInfo> snapshotSummaries) {
        this.snapshotSummaries = snapshotSummaries;
    }

    @Override
    public String toString() {
        return "ListSnapshotsResp{" +
                "snapshots=" + snapshots +
                ", snapshotSummaries=" + snapshotSummaries +
                '}';
    }

    /**
     * Builder for {@link ListSnapshotsResp} class.
     */


    public static class ListSnapshotsRespBuilder {
        private List<String> snapshots = new ArrayList<>();
        private List<SnapshotSummaryInfo> snapshotSummaries = new ArrayList<>();

        /**
         * Sets the list of snapshot names.
         *
         * @param snapshots the list of snapshot names
         * @return this builder
         */


        public ListSnapshotsRespBuilder snapshots(List<String> snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        /**
         * Sets the list of lightweight snapshot summaries.
         *
         * @param snapshotSummaries the list of snapshot summaries
         * @return this builder
         */


        public ListSnapshotsRespBuilder snapshotSummaries(List<SnapshotSummaryInfo> snapshotSummaries) {
            this.snapshotSummaries = snapshotSummaries;
            return this;
        }

        /**
         * Builds a {@link ListSnapshotsResp}.
         *
         * @return the built response
         */


        public ListSnapshotsResp build() {
            return new ListSnapshotsResp(this);
        }
    }
}

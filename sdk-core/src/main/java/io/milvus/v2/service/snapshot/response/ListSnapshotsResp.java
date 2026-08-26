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

    private ListSnapshotsResp(ListSnapshotsRespBuilder builder) {
        this.snapshots = builder.snapshots == null ? new ArrayList<>() : builder.snapshots;
    }

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

    @Override
    public String toString() {
        return "ListSnapshotsResp{" +
                "snapshots=" + snapshots +
                '}';
    }

    public static class ListSnapshotsRespBuilder {
        private List<String> snapshots = new ArrayList<>();

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
         * Builds a {@link ListSnapshotsResp}.
         *
         * @return the built response
         */
        public ListSnapshotsResp build() {
            return new ListSnapshotsResp(this);
        }
    }
}

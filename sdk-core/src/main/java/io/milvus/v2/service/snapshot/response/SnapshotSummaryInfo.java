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

/**
 * Lightweight metadata for a snapshot returned by {@code listSnapshots}.
 *
 * <p>{@code createTs} preserves the Milvus hybrid timestamp returned by {@code describeSnapshot}.
 * {@code createUtcTs} is the physical Unix time in milliseconds extracted from {@code createTs}
 * (the WAL boundary time, not snapshot completion time); {@code 0} means unknown or unsupported
 * by the server.</p>
 */


public class SnapshotSummaryInfo {
    private String name;
    private Long createTs;
    private String description;
    private Long createUtcTs;

    private SnapshotSummaryInfo(SnapshotSummaryInfoBuilder builder) {
        this.name = builder.name;
        this.createTs = builder.createTs;
        this.description = builder.description;
        this.createUtcTs = builder.createUtcTs;
    }

    /**
     * Creates a new builder for {@code SnapshotSummaryInfo}.
     *
     * @return the builder
     */


    public static SnapshotSummaryInfoBuilder builder() {
        return new SnapshotSummaryInfoBuilder();
    }

    /**
     * Returns the snapshot name.
     *
     * @return the snapshot name
     */


    public String getName() {
        return name;
    }

    /**
     * Sets the snapshot name.
     *
     * @param name the snapshot name
     */


    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the Milvus hybrid creation timestamp of the snapshot.
     *
     * @return the Milvus hybrid creation timestamp
     */


    public Long getCreateTs() {
        return createTs;
    }

    /**
     * Sets the Milvus hybrid creation timestamp of the snapshot.
     *
     * @param createTs the Milvus hybrid creation timestamp
     */


    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }

    /**
     * Returns the snapshot description.
     *
     * @return the snapshot description
     */


    public String getDescription() {
        return description;
    }

    /**
     * Sets the snapshot description.
     *
     * @param description the snapshot description
     */


    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the physical Unix timestamp in milliseconds extracted from {@code createTs},
     * or {@code 0} if unknown or unsupported by the server.
     *
     * @return the creation UTC timestamp in Unix milliseconds
     */


    public Long getCreateUtcTs() {
        return createUtcTs;
    }

    /**
     * Sets the physical Unix timestamp in milliseconds extracted from {@code createTs}.
     *
     * @param createUtcTs the creation UTC timestamp in Unix milliseconds
     */


    public void setCreateUtcTs(Long createUtcTs) {
        this.createUtcTs = createUtcTs;
    }

    @Override
    public String toString() {
        return "SnapshotSummaryInfo{" +
                "name='" + name + '\'' +
                ", createTs=" + createTs +
                ", description='" + description + '\'' +
                ", createUtcTs=" + createUtcTs +
                '}';
    }

    /**
     * Builder for {@link SnapshotSummaryInfo} class.
     */


    public static class SnapshotSummaryInfoBuilder {
        private String name;
        private Long createTs;
        private String description;
        private Long createUtcTs;

        /**
         * Sets the snapshot name.
         *
         * @param name the snapshot name
         * @return this builder
         */


        public SnapshotSummaryInfoBuilder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the Milvus hybrid creation timestamp of the snapshot.
         *
         * @param createTs the Milvus hybrid creation timestamp
         * @return this builder
         */


        public SnapshotSummaryInfoBuilder createTs(Long createTs) {
            this.createTs = createTs;
            return this;
        }

        /**
         * Sets the snapshot description.
         *
         * @param description the snapshot description
         * @return this builder
         */


        public SnapshotSummaryInfoBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the physical Unix timestamp in milliseconds extracted from {@code createTs}.
         *
         * @param createUtcTs the creation UTC timestamp in Unix milliseconds
         * @return this builder
         */


        public SnapshotSummaryInfoBuilder createUtcTs(Long createUtcTs) {
            this.createUtcTs = createUtcTs;
            return this;
        }

        /**
         * Builds a {@link SnapshotSummaryInfo}.
         *
         * @return the built snapshot summary
         */


        public SnapshotSummaryInfo build() {
            return new SnapshotSummaryInfo(this);
        }
    }
}

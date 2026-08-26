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

import java.util.List;

/**
 * Response returned by the {@code describeSnapshot} API.
 */
public class DescribeSnapshotResp {
    private String name;
    private String description;
    private String collectionName;
    private List<String> partitionNames;
    private Long createTs;
    private String s3Location;

    private DescribeSnapshotResp(DescribeSnapshotRespBuilder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.createTs = builder.createTs;
        this.s3Location = builder.s3Location;
    }

    public static DescribeSnapshotRespBuilder builder() {
        return new DescribeSnapshotRespBuilder();
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
     * Returns the collection name the snapshot was created from.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name the snapshot was created from.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns the partition names included in the snapshot.
     *
     * @return the partition names
     */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    /**
     * Sets the partition names included in the snapshot.
     *
     * @param partitionNames the partition names
     */
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    /**
     * Returns the creation timestamp of the snapshot.
     *
     * @return the creation timestamp
     */
    public Long getCreateTs() {
        return createTs;
    }

    /**
     * Sets the creation timestamp of the snapshot.
     *
     * @param createTs the creation timestamp
     */
    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }

    /**
     * Returns the S3 location where the snapshot data is stored.
     *
     * @return the S3 location
     */
    public String getS3Location() {
        return s3Location;
    }

    /**
     * Sets the S3 location where the snapshot data is stored.
     *
     * @param s3Location the S3 location
     */
    public void setS3Location(String s3Location) {
        this.s3Location = s3Location;
    }

    @Override
    public String toString() {
        return "DescribeSnapshotResp{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionNames=" + partitionNames +
                ", createTs=" + createTs +
                ", s3Location='" + s3Location + '\'' +
                '}';
    }

    public static class DescribeSnapshotRespBuilder {
        private String name;
        private String description;
        private String collectionName;
        private List<String> partitionNames;
        private Long createTs;
        private String s3Location;

        /**
         * Sets the snapshot name.
         *
         * @param name the snapshot name
         * @return this builder
         */
        public DescribeSnapshotRespBuilder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the snapshot description.
         *
         * @param description the snapshot description
         * @return this builder
         */
        public DescribeSnapshotRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the collection name the snapshot was created from.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public DescribeSnapshotRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition names included in the snapshot.
         *
         * @param partitionNames the partition names
         * @return this builder
         */
        public DescribeSnapshotRespBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        /**
         * Sets the creation timestamp of the snapshot.
         *
         * @param createTs the creation timestamp
         * @return this builder
         */
        public DescribeSnapshotRespBuilder createTs(Long createTs) {
            this.createTs = createTs;
            return this;
        }

        /**
         * Sets the S3 location where the snapshot data is stored.
         *
         * @param s3Location the S3 location
         * @return this builder
         */
        public DescribeSnapshotRespBuilder s3Location(String s3Location) {
            this.s3Location = s3Location;
            return this;
        }

        /**
         * Builds a {@link DescribeSnapshotResp}.
         *
         * @return the built response
         */
        public DescribeSnapshotResp build() {
            return new DescribeSnapshotResp(this);
        }
    }
}

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public Long getCreateTs() {
        return createTs;
    }

    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }

    public String getS3Location() {
        return s3Location;
    }

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

        public DescribeSnapshotRespBuilder name(String name) {
            this.name = name;
            return this;
        }

        public DescribeSnapshotRespBuilder description(String description) {
            this.description = description;
            return this;
        }

        public DescribeSnapshotRespBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public DescribeSnapshotRespBuilder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public DescribeSnapshotRespBuilder createTs(Long createTs) {
            this.createTs = createTs;
            return this;
        }

        public DescribeSnapshotRespBuilder s3Location(String s3Location) {
            this.s3Location = s3Location;
            return this;
        }

        public DescribeSnapshotResp build() {
            return new DescribeSnapshotResp(this);
        }
    }
}

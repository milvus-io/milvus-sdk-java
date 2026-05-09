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

package io.milvus.v2.service.snapshot.request;

public class RestoreSnapshotReq {
    private String snapshotName;
    private String sourceCollectionName;
    private String targetCollectionName;
    private String sourceDbName;
    private String targetDbName;

    private RestoreSnapshotReq(RestoreSnapshotReqBuilder builder) {
        this.snapshotName = builder.snapshotName;
        this.sourceCollectionName = builder.sourceCollectionName;
        this.targetCollectionName = builder.targetCollectionName;
        this.sourceDbName = builder.sourceDbName;
        this.targetDbName = builder.targetDbName;
    }

    public static RestoreSnapshotReqBuilder builder() {
        return new RestoreSnapshotReqBuilder();
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getSourceCollectionName() {
        return sourceCollectionName;
    }

    public void setSourceCollectionName(String sourceCollectionName) {
        this.sourceCollectionName = sourceCollectionName;
    }

    public String getTargetCollectionName() {
        return targetCollectionName;
    }

    public void setTargetCollectionName(String targetCollectionName) {
        this.targetCollectionName = targetCollectionName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getTargetDbName() {
        return targetDbName;
    }

    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotReq{" +
                "snapshotName='" + snapshotName + '\'' +
                ", sourceCollectionName='" + sourceCollectionName + '\'' +
                ", targetCollectionName='" + targetCollectionName + '\'' +
                ", sourceDbName='" + sourceDbName + '\'' +
                ", targetDbName='" + targetDbName + '\'' +
                '}';
    }

    public static class RestoreSnapshotReqBuilder {
        private String snapshotName;
        private String sourceCollectionName;
        private String targetCollectionName;
        private String sourceDbName = "";
        private String targetDbName = "";

        public RestoreSnapshotReqBuilder snapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        public RestoreSnapshotReqBuilder sourceCollectionName(String sourceCollectionName) {
            this.sourceCollectionName = sourceCollectionName;
            return this;
        }

        public RestoreSnapshotReqBuilder targetCollectionName(String targetCollectionName) {
            this.targetCollectionName = targetCollectionName;
            return this;
        }

        public RestoreSnapshotReqBuilder sourceDbName(String sourceDbName) {
            this.sourceDbName = sourceDbName;
            return this;
        }

        public RestoreSnapshotReqBuilder targetDbName(String targetDbName) {
            this.targetDbName = targetDbName;
            return this;
        }

        public RestoreSnapshotReq build() {
            return new RestoreSnapshotReq(this);
        }
    }
}

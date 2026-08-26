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

package io.milvus.v2.service.utility.request;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.OptimizeTask;

public class CompactReq {
    private String databaseName;
    private String collectionName;
    private Boolean isClustering = Boolean.FALSE;
    private Boolean isL0 = Boolean.FALSE;
    private Long targetSize; // value in targetSizeUnit, null means server default
    private String targetSizeUnit = "mb";

    private CompactReq(CompactReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.isClustering = builder.isClustering;
        this.isL0 = builder.isL0;
        this.targetSize = builder.targetSize;
        this.targetSizeUnit = builder.targetSizeUnit;
    }

    public static CompactReqBuilder builder() {
        return new CompactReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Boolean getIsClustering() {
        return isClustering;
    }

    public void setIsClustering(Boolean isClustering) {
        this.isClustering = isClustering;
    }

    public Boolean getIsL0() {
        return isL0;
    }

    public void setIsL0(Boolean isL0) {
        this.isL0 = isL0;
    }

    public Long getTargetSize() {
        return targetSize;
    }

    public void setTargetSize(Long targetSize) {
        this.targetSize = targetSize;
    }

    public String getTargetSizeUnit() {
        return targetSizeUnit;
    }

    public void setTargetSizeUnit(String targetSizeUnit) {
        this.targetSizeUnit = targetSizeUnit;
    }

    public Long getTargetSizeInMB() {
        return convertTargetSizeToMB(targetSize, targetSizeUnit);
    }

    @Override
    public String toString() {
        return "CompactReq{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", isClustering=" + isClustering +
                ", isL0=" + isL0 +
                ", targetSize=" + targetSize +
                ", targetSizeUnit='" + targetSizeUnit + '\'' +
                '}';
    }

    private static Long convertTargetSizeToMB(Long targetSize, String targetSizeUnit) {
        // null and 0 both mean "server default": the server reads an unset target_size as 0
        // and skips force-merge, so mapping 0 to null preserves the wire behavior that existed
        // before this change (0 was previously sent verbatim and treated as server default).
        if (targetSize == null || targetSize == 0) {
            return null;
        }
        if (targetSize < 0) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "targetSize must be a positive integer: " + targetSize);
        }
        if (targetSizeUnit == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "targetSizeUnit cannot be null when targetSize is set");
        }
        // Reuse the shared size-to-MB parser from OptimizeTask so the unit table and the
        // 1MB floor live in a single place instead of two sibling parsers that can drift.
        return OptimizeTask.parseTargetSize(targetSize + targetSizeUnit);
    }

    public static class CompactReqBuilder {
        private String databaseName;
        private String collectionName;
        private Boolean isClustering = Boolean.FALSE;
        private Boolean isL0 = Boolean.FALSE;
        private Long targetSize;
        private String targetSizeUnit = "mb";

        public CompactReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public CompactReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public CompactReqBuilder isClustering(Boolean isClustering) {
            this.isClustering = isClustering;
            return this;
        }

        public CompactReqBuilder isL0(Boolean isL0) {
            this.isL0 = isL0;
            return this;
        }

        public CompactReqBuilder targetSize(Long targetSize) {
            this.targetSize = targetSize;
            return this;
        }

        public CompactReqBuilder targetSizeUnit(String targetSizeUnit) {
            this.targetSizeUnit = targetSizeUnit;
            return this;
        }

        public CompactReq build() {
            return new CompactReq(this);
        }
    }
}

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

/**
 * Request parameters for the {@code compact} API.
 */
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

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets the collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Returns whether the compaction is a clustering compaction.
     *
     * @return {@code true} to trigger a clustering compaction, {@code false} otherwise
     */
    public Boolean getIsClustering() {
        return isClustering;
    }

    /**
     * Sets whether the compaction is a clustering compaction.
     *
     * @param isClustering {@code true} to trigger a clustering compaction, {@code false} otherwise
     */
    public void setIsClustering(Boolean isClustering) {
        this.isClustering = isClustering;
    }

    /**
     * Returns whether the compaction targets the L0 segment level.
     *
     * @return {@code true} to compact L0 segments, {@code false} otherwise
     */
    public Boolean getIsL0() {
        return isL0;
    }

    /**
     * Sets whether the compaction targets the L0 segment level.
     *
     * @param isL0 {@code true} to compact L0 segments, {@code false} otherwise
     */
    public void setIsL0(Boolean isL0) {
        this.isL0 = isL0;
    }

    /**
     * Returns the target segment size, expressed in the {@link #getTargetSizeUnit()} unit.
     *
     * @return the target segment size, or {@code null} for the server default
     */
    public Long getTargetSize() {
        return targetSize;
    }

    /**
     * Sets the target segment size, expressed in the {@link #getTargetSizeUnit()} unit.
     *
     * @param targetSize the target segment size, or {@code null} for the server default
     */
    public void setTargetSize(Long targetSize) {
        this.targetSize = targetSize;
    }

    /**
     * Returns the unit of the {@link #getTargetSize()} value.
     *
     * @return the target size unit, e.g. {@code "mb"}
     */
    public String getTargetSizeUnit() {
        return targetSizeUnit;
    }

    /**
     * Sets the unit of the {@link #getTargetSize()} value.
     *
     * @param targetSizeUnit the target size unit, e.g. {@code "mb"}
     */
    public void setTargetSizeUnit(String targetSizeUnit) {
        this.targetSizeUnit = targetSizeUnit;
    }

    /**
     * Returns the target segment size converted to megabytes (MB).
     *
     * @return the target segment size in MB, or {@code null} when the server default applies
     */
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

        /**
         * Sets the database name.
         *
         * @param databaseName the database name
         * @return this builder
         */
        public CompactReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collectionName the collection name
         * @return this builder
         */
        public CompactReqBuilder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets whether the compaction is a clustering compaction.
         *
         * @param isClustering {@code true} to trigger a clustering compaction, {@code false} otherwise
         * @return this builder
         */
        public CompactReqBuilder isClustering(Boolean isClustering) {
            this.isClustering = isClustering;
            return this;
        }

        /**
         * Sets whether the compaction targets the L0 segment level.
         *
         * @param isL0 {@code true} to compact L0 segments, {@code false} otherwise
         * @return this builder
         */
        public CompactReqBuilder isL0(Boolean isL0) {
            this.isL0 = isL0;
            return this;
        }

        /**
         * Sets the target segment size, expressed in the configured {@code targetSizeUnit}.
         *
         * @param targetSize the target segment size, or {@code null} for the server default
         * @return this builder
         */
        public CompactReqBuilder targetSize(Long targetSize) {
            this.targetSize = targetSize;
            return this;
        }

        /**
         * Sets the unit of the {@code targetSize} value.
         *
         * @param targetSizeUnit the target size unit, e.g. {@code "mb"}
         * @return this builder
         */
        public CompactReqBuilder targetSizeUnit(String targetSizeUnit) {
            this.targetSizeUnit = targetSizeUnit;
            return this;
        }

        /**
         * Builds the {@code CompactReq}.
         *
         * @return the constructed {@code CompactReq}
         */
        public CompactReq build() {
            return new CompactReq(this);
        }
    }
}

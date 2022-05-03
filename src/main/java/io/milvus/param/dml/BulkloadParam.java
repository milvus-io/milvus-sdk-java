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

package io.milvus.param.dml;

import com.google.common.collect.Lists;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>bulkload</code> interface.
 */
@Getter
public class BulkloadParam {
    private final String collectionName;
    private final String partitionName;
    private final boolean rowBased;
    private final List<String> files;
    private final Map<String, String> options = new HashMap<>();

    private BulkloadParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.rowBased = builder.rowBased;
        this.files = builder.files;
        this.options.put(Constant.BUCKET, builder.bucketName);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link BulkloadParam} class.
     */
    public static class Builder {
        private String collectionName;
        private String partitionName = "";
        private Boolean rowBased = Boolean.TRUE;
        private final List<String> files = Lists.newArrayList();
        private String bucketName = "";

        private Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the partition name (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Row-based or column-based data
         *
         * @param rowBased true: row-based, false: column-based
         * @return <code>Builder</code>
         */
        public Builder withRowBased(@NonNull Boolean rowBased) {
            this.rowBased = rowBased;
            return this;
        }

        /**
         * Sets bucket name where the files come from MinIO/S3 storage.
         * If bucket is not specified, the server will use the default bucket to explore.
         *
         * @param bucketName bucket name
         * @return <code>Builder</code>
         */
        public Builder withBucket(@NonNull String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        /**
         * Specifies file paths to import. Each path is a relative path to the target bucket.
         *
         * @param files file paths list
         * @return <code>Builder</code>
         */
        public Builder withFiles(@NonNull List<String> files) {
            files.forEach(this::addFile);
            return this;
        }

        /**
         * Specifies a file paths to import. The path is a relative path to the target bucket.
         *
         * @param filePath file relative path
         * @return <code>Builder</code>
         */
        public Builder addFile(@NonNull String filePath) {
            if (!this.files.contains(filePath)) {
                this.files.add(filePath);
            }
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link BulkloadParam} instance.
         *
         * @return {@link BulkloadParam}
         */
        public BulkloadParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new BulkloadParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link BulkloadParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "BulkloadParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", files='" + files.toString() + '\'' +
                ", rowBased='" + rowBased + '\'' +
                ", options=" + options.toString() +
                '}';
    }
}

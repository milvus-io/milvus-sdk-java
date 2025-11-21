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

package io.milvus.bulkwriter;

import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for <code>volumeBulkWriter</code> interface.
 */
public class VolumeBulkWriterParam {
    private final CreateCollectionReq.CollectionSchema collectionSchema;
    private final String remotePath;
    private final long chunkSize;
    private final BulkFileType fileType;
    private final Map<String, Object> config;

    private final String cloudEndpoint;
    private final String apiKey;
    private final String volumeName;

    private VolumeBulkWriterParam(Builder builder) {
        this.collectionSchema = builder.collectionSchema;
        this.remotePath = builder.remotePath;
        this.chunkSize = builder.chunkSize;
        this.fileType = builder.fileType;
        this.config = builder.config;

        this.cloudEndpoint = builder.cloudEndpoint;
        this.apiKey = builder.apiKey;
        this.volumeName = builder.volumeName;
    }

    public CreateCollectionReq.CollectionSchema getCollectionSchema() {
        return collectionSchema;
    }

    public String getRemotePath() {
        return remotePath;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public BulkFileType getFileType() {
        return fileType;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public String getCloudEndpoint() {
        return cloudEndpoint;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getVolumeName() {
        return volumeName;
    }

    @Override
    public String toString() {
        return "VolumeBulkWriterParam{" +
                "collectionSchema=" + collectionSchema +
                ", remotePath='" + remotePath + '\'' +
                ", chunkSize=" + chunkSize +
                ", fileType=" + fileType +
                ", cloudEndpoint='" + cloudEndpoint + '\'' +
                ", volumeName='" + volumeName + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link VolumeBulkWriterParam} class.
     */
    public static final class Builder {
        private CreateCollectionReq.CollectionSchema collectionSchema;
        private String remotePath;
        private long chunkSize = 128 * 1024 * 1024;
        private BulkFileType fileType = BulkFileType.PARQUET;
        private final Map<String, Object> config = new HashMap<>();

        private String cloudEndpoint;
        private String apiKey;

        private String volumeName;

        private Builder() {
        }

        /**
         * Sets the collection info.
         *
         * @param collectionSchema collection info
         * @return <code>Builder</code>
         */
        public Builder withCollectionSchema(CollectionSchemaParam collectionSchema) {
            this.collectionSchema = V2AdapterUtils.convertV1Schema(collectionSchema);
            return this;
        }

        /**
         * Sets the collection schema by V2.
         *
         * @param collectionSchema collection schema
         * @return <code>Builder</code>
         */
        public Builder withCollectionSchema(CreateCollectionReq.CollectionSchema collectionSchema) {
            this.collectionSchema = collectionSchema;
            return this;
        }

        /**
         * Sets the remotePath.
         *
         * @param remotePath remote path
         * @return <code>Builder</code>
         */
        public Builder withRemotePath(String remotePath) {
            this.remotePath = remotePath;
            return this;
        }

        public Builder withChunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder withFileType(BulkFileType fileType) {
            this.fileType = fileType;
            return this;
        }

        public Builder withConfig(String key, Object val) {
            this.config.put(key, val);
            return this;
        }

        public Builder withCloudEndpoint(String cloudEndpoint) {
            this.cloudEndpoint = cloudEndpoint;
            return this;
        }

        public Builder withApiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        public Builder withVolumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link VolumeBulkWriterParam} instance.
         *
         * @return {@link VolumeBulkWriterParam}
         */
        public VolumeBulkWriterParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(remotePath, "localPath");

            if (collectionSchema == null) {
                throw new ParamException("collectionSchema cannot be null");
            }

            return new VolumeBulkWriterParam(this);
        }
    }

}

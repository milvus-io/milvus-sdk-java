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
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Parameters for <code>bulkWriter</code> interface.
 */
@Getter
@ToString
public class LocalBulkWriterParam {
    private final CollectionSchemaParam collectionSchema;
    private final String localPath;
    private final int chunkSize;
    private final BulkFileType fileType;

    private LocalBulkWriterParam(@NonNull Builder builder) {
        this.collectionSchema = builder.collectionSchema;
        this.localPath = builder.localPath;
        this.chunkSize = builder.chunkSize;
        this.fileType = builder.fileType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link LocalBulkWriterParam} class.
     */
    public static final class Builder {
        private CollectionSchemaParam collectionSchema;
        private String localPath;
        private int chunkSize = 128 * 1024 * 1024;
        private BulkFileType fileType = BulkFileType.PARQUET;

        private Builder() {
        }

        /**
         * Sets the collection schema.
         *
         * @param collectionSchema collection schema
         * @return <code>Builder</code>
         */
        public Builder withCollectionSchema(@NonNull CollectionSchemaParam collectionSchema) {
            this.collectionSchema = collectionSchema;
            return this;
        }

        /**
         * Sets the collection schema by V2.
         *
         * @param collectionSchema collection schema
         * @return <code>Builder</code>
         */
        public Builder withCollectionSchema(@NonNull CreateCollectionReq.CollectionSchema collectionSchema) {
            this.collectionSchema = V2AdapterUtils.convertV2Schema(collectionSchema);
            return this;
        }

        /**
         * Sets the localPath.
         *
         * @param localPath collection name
         * @return <code>Builder</code>
         */
        public Builder withLocalPath(@NonNull String localPath) {
            this.localPath = localPath;
            return this;
        }

        public Builder withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder withFileType(BulkFileType fileType) {
            this.fileType = fileType;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link LocalBulkWriterParam} instance.
         *
         * @return {@link LocalBulkWriterParam}
         */
        public LocalBulkWriterParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(localPath, "localPath");

            if (collectionSchema == null) {
                throw new ParamException("collectionParam cannot be null");
            }

            return new LocalBulkWriterParam(this);
        }
    }

}

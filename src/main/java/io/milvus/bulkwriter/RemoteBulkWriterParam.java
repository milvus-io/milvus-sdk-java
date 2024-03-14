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

import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

/**
 * Parameters for <code>bulkWriter</code> interface.
 */
@Getter
@ToString
public class RemoteBulkWriterParam {
    private final CollectionSchemaParam collectionSchema;
    private final StorageConnectParam connectParam;
    private final String remotePath;
    private final int chunkSize;
    private final BulkFileType fileType;

    private RemoteBulkWriterParam(@NonNull Builder builder) {
        this.collectionSchema = builder.collectionSchema;
        this.connectParam = builder.connectParam;
        this.remotePath = builder.remotePath;
        this.chunkSize = builder.chunkSize;
        this.fileType = builder.fileType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link RemoteBulkWriterParam} class.
     */
    public static final class Builder {
        private CollectionSchemaParam collectionSchema;
        private StorageConnectParam connectParam;
        private String remotePath;
        private int chunkSize = 1024 * 1024 * 1024;
        private BulkFileType fileType = BulkFileType.PARQUET;

        private Builder() {
        }

        /**
         * Sets the collection info.
         *
         * @param collectionSchema collection info
         * @return <code>Builder</code>
         */
        public Builder withCollectionSchema(@NonNull CollectionSchemaParam collectionSchema) {
            this.collectionSchema = collectionSchema;
            return this;
        }

        public Builder withConnectParam(@NotNull StorageConnectParam connectParam) {
            this.connectParam = connectParam;
            return this;
        }

        /**
         * Sets the remotePath.
         *
         * @param remotePath remote path
         * @return <code>Builder</code>
         */
        public Builder withRemotePath(@NonNull String remotePath) {
            this.remotePath = remotePath;
            return this;
        }

        public Builder withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder withFileType(@NonNull BulkFileType fileType) {
            this.fileType = fileType;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link RemoteBulkWriterParam} instance.
         *
         * @return {@link RemoteBulkWriterParam}
         */
        public RemoteBulkWriterParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(remotePath, "localPath");

            if (collectionSchema == null) {
                throw new ParamException("collectionSchema cannot be null");
            }

            if (connectParam == null) {
                throw new ParamException("connectParam cannot be null");
            }

            return new RemoteBulkWriterParam(this);
        }
    }

}

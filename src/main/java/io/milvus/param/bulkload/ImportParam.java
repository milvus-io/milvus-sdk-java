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

package io.milvus.param.bulkload;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.FieldType;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>import</code> interface.
 */
@Getter
public class ImportParam {
    private final String collectionName;
    private final String partitionName;
    private final boolean rowBased;
    private final List<String> files;

    private ImportParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.rowBased = builder.rowBased;
        this.files = builder.files;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link ImportParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String partitionName;
        private Boolean rowBased = false;
        private final List<String> files = new ArrayList<>();
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
         * Sets the partition name. partition name can be null.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the file description. The description can be empty. The default is false.
         *
         * @param rowBased description of the file
         * @return <code>Builder</code>
         */
        public Builder withRowBased(@NonNull Boolean rowBased) {
            this.rowBased = rowBased;
            return this;
        }

        /**
         * Sets the path of the files. The files cannot be empty or null.
         *
         * @param files a <code>List</code> of {@link String}
         * @return <code>Builder</code>
         */
        public Builder withFiles(@NonNull List<String> files) {
            this.files.addAll(files);
            return this;
        }

        /**
         * Adds a file path.
         * @see String
         *
         * @param file a {@link String}
         * @return <code>Builder</code>
         */
        public Builder addFile(@NonNull String file) {
            this.files.add(file);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link ImportParam} instance.
         *
         * @return {@link ImportParam}
         */
        public ImportParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");


            if (files.isEmpty()) {
                throw new ParamException("Field numbers must be larger than 0");
            }

            for (String file : files) {
                if (StringUtils.isEmpty(file)) {
                    throw new ParamException("file field cannot be empty");
                }
            }

            return new ImportParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link ImportParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "ImportParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName=" + partitionName +
                ", rowBased='" + rowBased + '\'' +
                ", files=" + files.toString() +
                '}';
    }
}

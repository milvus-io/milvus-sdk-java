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

package io.milvus.param.bulkinsert;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>bulkInsert</code> interface.
 */
public class BulkInsertParam {
    private final String databaseName;
    private final String collectionName;
    private final String partitionName;
    private final List<String> files;
    private final Map<String, String> options;

    private BulkInsertParam(Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.files = builder.files;
        this.options = builder.options;
    }

    // Getter methods to replace @Getter annotation
    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<String> getFiles() {
        return files;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    // toString method to replace @ToString annotation
    @Override
    public String toString() {
        return "BulkInsertParam{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", files=" + files +
                ", options=" + options +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link BulkInsertParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;
        private String partitionName;
        private final List<String> files = new ArrayList<>();
        private final Map<String, String> options = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
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
         * Sets the path of the files. The paths cannot be empty or null.
         * Each file path must be a relative path under the Milvus storage root path.
         *
         * @param files a <code>List</code> of {@link String}
         * @return <code>Builder</code>
         */
        public Builder withFiles(List<String> files) {
            if (files == null) {
                throw new IllegalArgumentException("files cannot be null");
            }
            this.files.addAll(files);
            return this;
        }

        /**
         * Adds a file path. The path cannot be empty or null.
         * The file path must be a relative path under the Milvus storage root path.
         *
         * @param file a {@link String}
         * @return <code>Builder</code>
         */
        public Builder addFile(String file) {
            if (file == null) {
                throw new IllegalArgumentException("file cannot be null");
            }
            this.files.add(file);
            return this;
        }

        /**
         * Sets the options of the request
         *
         * @param key   a <code>List</code> of {@link String}
         * @param value a <code>List</code> of {@link String}
         * @return <code>Builder</code>
         */
        public Builder withOption(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link BulkInsertParam} instance.
         *
         * @return {@link BulkInsertParam}
         */
        public BulkInsertParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (files.isEmpty()) {
                throw new ParamException("File path is required");
            }

            for (String file : files) {
                if (StringUtils.isEmpty(file)) {
                    throw new ParamException("File path cannot be empty or null");
                }
            }

            return new BulkInsertParam(this);
        }
    }

}

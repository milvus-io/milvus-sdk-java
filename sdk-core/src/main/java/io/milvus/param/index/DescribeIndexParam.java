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

package io.milvus.param.index;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/**
 * Parameters for <code>describeIndex</code> interface.
 */
@Getter
@ToString
public class DescribeIndexParam {
    private final String databaseName;
    private final String collectionName;
    private final String indexName;
    private final String fieldName;

    private DescribeIndexParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.indexName = builder.indexName;
        this.fieldName = builder.fieldName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link DescribeIndexParam} class.
     */
    public static final class Builder {
        private String databaseName;
        private String collectionName;
        private String indexName = "";
        private String fieldName = "";

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
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the target index name. Index name can be empty or null.
         * If no index name is specified, then return all this collection indexes.
         * @param indexName field name
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the target field name. Field name can be empty or null.
         * If no field name is specified, then return all this collection indexes.
         * @param fieldName field name
         * @return <code>Builder</code>
         */
        public Builder withFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link DescribeIndexParam} instance.
         *
         * @return {@link DescribeIndexParam}
         */
        public DescribeIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            // if indexName is empty, describeIndex will return all indexes information

            return new DescribeIndexParam(this);
        }
    }

}

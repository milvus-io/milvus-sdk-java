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

package io.milvus.param.highlevel.dml;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import io.milvus.param.dml.QueryParam;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>query</code> interface.
 */
@Getter
@ToString
public class QuerySimpleParam {
    private final String collectionName;
    private final List<String> outputFields;
    private final String filter;
    private final Long offset;
    private final Long limit;
    private final ConsistencyLevelEnum consistencyLevel;

    private QuerySimpleParam(@NotNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.outputFields = builder.outputFields;
        this.filter = builder.filter;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.consistencyLevel = builder.consistencyLevel;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link QuerySimpleParam} class.
     */
    public static class Builder {
        private String collectionName;
        private final List<String> outputFields = new ArrayList<>();
        private String filter = "";
        private Long offset = 0L;
        private Long limit = 0L;
        private ConsistencyLevelEnum consistencyLevel = null;

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
         * Specifies output fields (Optional).
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        @Deprecated
        public Builder withOutFields(@NonNull List<String> outFields) {
            this.outputFields.addAll(outFields);
            return this;
        }

        /**
         * Specifies output fields (Optional).
         *
         * @param outputFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutputFields(@NonNull List<String> outputFields) {
            this.outputFields.addAll(outputFields);
            return this;
        }

        /**
         * Sets the expression to query entities.
         * @see <a href="https://milvus.io/docs/v2.0.0/boolean.md">Boolean Expression Rules</a>
         *
         * @param filter filtering expression
         * @return <code>Builder</code>
         */
        public Builder withFilter(@NonNull String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Specify a position to return results. Only take effect when the 'limit' value is specified.
         * Default value is 0, start from begin.
         *
         * @param offset a value to define the position
         * @return <code>Builder</code>
         */
        public Builder withOffset(@NonNull Long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Specify a value to control the returned number of entities. Must be a positive value.
         * Default value is 0, will return without limit.
         *
         * @param limit a value to define the limit of returned entities
         * @return <code>Builder</code>
         */
        public Builder withLimit(@NonNull Long limit) {
            this.limit = limit;
            return this;
        }

        /**
         * ConsistencyLevel of consistency level.
         *
         * @param consistencyLevel consistency level
         * @return <code>Builder</code>
         */
        public Builder withConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link QuerySimpleParam} instance.
         *
         * @return {@link QuerySimpleParam}
         */
        public QuerySimpleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(filter, "Filter");

            if (offset < 0) {
                throw new ParamException("The offset value cannot be less than 0");
            }

            if (limit < 0) {
                throw new ParamException("The limit value cannot be less than 0");
            }
            return new QuerySimpleParam(this);
        }
    }
}

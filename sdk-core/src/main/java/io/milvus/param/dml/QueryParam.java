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
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>query</code> interface.
 */
@Getter
@ToString
public class QueryParam {
    @Setter
    private String databaseName;
    private final String collectionName;
    private final List<String> partitionNames;
    private final List<String> outFields;
    private final String expr;
    private final long travelTimestamp;
    private final long guaranteeTimestamp;
    private final long gracefulTime;
    private final ConsistencyLevelEnum consistencyLevel;
    private final long offset;
    private final long limit;
    private final boolean ignoreGrowing;

    private QueryParam(@NonNull Builder builder) {
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.outFields = builder.outFields;
        this.expr = builder.expr;
        this.travelTimestamp = builder.travelTimestamp;
        this.guaranteeTimestamp = builder.guaranteeTimestamp;
        this.consistencyLevel = builder.consistencyLevel;
        this.gracefulTime = builder.gracefulTime;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.ignoreGrowing = builder.ignoreGrowing;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link QueryParam} class.
     */
    public static class Builder {
        private String databaseName;
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private final List<String> outFields = new ArrayList<>();
        private String expr = "";
        private Long travelTimestamp = 0L; // deprecated
        private Long gracefulTime = 5000L; // deprecated
        private Long guaranteeTimestamp = Constant.GUARANTEE_EVENTUALLY_TS; // deprecated
        private ConsistencyLevelEnum consistencyLevel = null;
        private Long offset = 0L;
        private Long limit = 0L;
        private Boolean ignoreGrowing = Boolean.FALSE;

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
         * Sets partition names list to specify query scope (Optional).
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * Adds a partition to specify query scope (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(@NonNull String partitionName) {
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
        }

        /**
         * Specifies output fields (Optional).
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutFields(@NonNull List<String> outFields) {
            outFields.forEach(this::addOutField);
            return this;
        }

        /**
         * Specifies an output field (Optional).
         *
         * @param fieldName field name
         * @return <code>Builder</code>
         */
        public Builder addOutField(@NonNull String fieldName) {
            if (!this.outFields.contains(fieldName)) {
                this.outFields.add(fieldName);
            }
            return this;
        }

        /**
         * Sets the expression to query entities.
         * @see <a href="https://milvus.io/docs/v2.0.0/boolean.md">Boolean Expression Rules</a>
         *
         * @param expr filtering expression
         * @return <code>Builder</code>
         */
        public Builder withExpr(@NonNull String expr) {
            this.expr = expr;
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
         * Ignore the growing segments to get best query performance. Default is False.
         * For the user case that don't require data visibility.
         *
         * @param ignoreGrowing <code>Boolean.TRUE</code> ignore, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withIgnoreGrowing(@NonNull Boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link QueryParam} instance.
         *
         * @return {@link QueryParam}
         */
        public QueryParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullString(expr, "Expression");

            if (travelTimestamp < 0) {
                throw new ParamException("The travel timestamp must be greater than 0");
            }

            if (guaranteeTimestamp < 0) {
                throw new ParamException("The guarantee timestamp must be greater than 0");
            }

            if (offset < 0) {
                throw new ParamException("The offset value cannot be less than 0");
            }

            if (limit < 0) {
                throw new ParamException("The limit value cannot be less than 0");
            }

            return new QueryParam(this);
        }
    }

}

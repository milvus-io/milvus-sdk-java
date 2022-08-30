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

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>query</code> interface.
 */
@Getter
public class QueryParam {
    private final String collectionName;
    private final List<String> partitionNames;
    private final List<String> outFields;
    private final String expr;
    private final long travelTimestamp;
    private final ConsistencyLevelEnum consistencyLevel;

    private QueryParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.outFields = builder.outFields;
        this.expr = builder.expr;
        this.travelTimestamp = builder.travelTimestamp;
        this.consistencyLevel = builder.consistencyLevel;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link QueryParam} class.
     */
    public static class Builder {
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private final List<String> outFields = new ArrayList<>();
        private String expr = "";
        private Long travelTimestamp = 0L;
        private ConsistencyLevelEnum consistencyLevel = ConsistencyLevelEnum.BOUNDED;

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
         * ConsistencyLevel of consistency level.
         * Default value is ConsistencyLevelEnum.BOUNDED.
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
         * Specify an absolute timestamp in a query to get results based on a data view at a specified point in time.
         * Default value is 0, server executes query on a full data view.
         *
         * @param ts a timestamp value
         * @return <code>Builder</code>
         */
        public Builder withTravelTimestamp(@NonNull Long ts) {
            this.travelTimestamp = ts;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link QueryParam} instance.
         *
         * @return {@link QueryParam}
         */
        public QueryParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(expr, "Expression");

            if (travelTimestamp < 0) {
                throw new ParamException("The travel timestamp must be greater than 0");
            }

            return new QueryParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link QueryParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "QueryParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionNames='" + partitionNames.toString() + '\'' +
                ", outFields='" + outFields.toString() + '\'' +
                ", expr='" + expr + '\'' +
                ", consistencyLevel='" + consistencyLevel + '\'' +
                '}';
    }
}

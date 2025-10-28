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

import com.google.common.collect.Lists;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.param.dml.SearchParam;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameters for <code>search</code> interface.
 */
public class SearchSimpleParam {
    private final String collectionName;
    private final List<?> vectors;
    private final List<String> outputFields;
    private final String filter;
    private final Long offset;
    private final int limit;

    private final Map<String, Object> params;
    private final ConsistencyLevelEnum consistencyLevel;

    private SearchSimpleParam(@NotNull Builder builder) {
        // Replace @NonNull logic with explicit null check
        if (builder == null) {
            throw new IllegalArgumentException("builder cannot be null");
        }
        this.collectionName = builder.collectionName;
        this.vectors = builder.vectors;
        this.outputFields = builder.outputFields;
        this.filter = builder.filter;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.params = builder.params;
        this.consistencyLevel = builder.consistencyLevel;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    // Getter methods to replace @Getter annotation
    public String getCollectionName() {
        return collectionName;
    }

    public List<?> getVectors() {
        return vectors;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public String getFilter() {
        return filter;
    }

    public Long getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public ConsistencyLevelEnum getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Builder for {@link SearchSimpleParam} class.
     */
    public static class Builder {
        private String collectionName;
        private List<?> vectors;
        private final List<String> outputFields = Lists.newArrayList();
        private String filter = "";
        private Long offset = 0L;
        private int limit = 10;
        private ConsistencyLevelEnum consistencyLevel = null;

        private final Map<String, Object> params = new HashMap<>();

        Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(String collectionName) {
            // Replace @NonNull logic with explicit null check
            if (collectionName == null) {
                throw new IllegalArgumentException("collectionName cannot be null");
            }
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets expression to filter out entities before searching (Optional).
         *
         * @param filter filtering expression
         * @return <code>Builder</code>
         * @see <a href="https://milvus.io/docs/v2.0.0/boolean.md">Boolean Expression Rules</a>
         */
        public Builder withFilter(String filter) {
            // Replace @NonNull logic with explicit null check
            if (filter == null) {
                throw new IllegalArgumentException("filter cannot be null");
            }
            this.filter = filter;
            return this;
        }

        /**
         * Specifies output fields (Optional).
         *
         * @param outputFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutputFields(List<String> outputFields) {
            // Replace @NonNull logic with explicit null check
            if (outputFields == null) {
                throw new IllegalArgumentException("outputFields cannot be null");
            }
            this.outputFields.addAll(outputFields);
            return this;
        }

        /**
         * Sets the target vectors.
         *
         * @param vectors list of target vectors: List of List Float;
         * @return <code>Builder</code>
         */
        public Builder withVectors(List<?> vectors) {
            // Replace @NonNull logic with explicit null check
            if (vectors == null) {
                throw new IllegalArgumentException("vectors cannot be null");
            }
            this.vectors = vectors;
            return this;
        }

        /**
         * Specify a position to return results. Only take effect when the 'limit' value is specified.
         * Default value is 0, start from begin.
         *
         * @param offset a value to define the position
         * @return <code>Builder</code>
         */
        public Builder withOffset(Long offset) {
            // Replace @NonNull logic with explicit null check
            if (offset == null) {
                throw new IllegalArgumentException("offset cannot be null");
            }
            this.offset = offset;
            return this;
        }

        /**
         * Specify a value to control the returned number of entities. Must be a positive value.
         * Default value is 10, will return without limit.
         * To maintain consistency with the parameter type of the query interface, the field is declared as Long. In reality, the field is of type int.
         *
         * @param limit a value to define the limit of returned entities
         * @return <code>Builder</code>
         */
        public Builder withLimit(Long limit) {
            // Replace @NonNull logic with explicit null check
            if (limit == null) {
                throw new IllegalArgumentException("limit cannot be null");
            }
            this.limit = Math.toIntExact(limit);
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
         * Verifies parameters and creates a new {@link SearchSimpleParam} instance.
         *
         * @return {@link SearchSimpleParam}
         */
        public SearchSimpleParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            if (CollectionUtils.isEmpty(vectors)) {
                throw new ParamException("vector cannot be empty");
            }

            if (offset < 0) {
                throw new ParamException("The offset value cannot be less than 0");
            }

            if (limit < 0) {
                throw new ParamException("The limit value cannot be less than 0");
            }

            params.put(Constant.OFFSET, offset);
            return new SearchSimpleParam(this);
        }
    }

    /**
     *
     * Warning: don't use lombok@ToString to annotate this class
     * because large number of vectors will waste time in toString() method.
     *
     */
    @Override
    public String toString() {
        return "SearchSimpleParam{" +
                "collectionName='" + collectionName + '\'' +
                ", params='" + params + '\'' +
                ", filter='" + filter + '\'' +
                ", NQ=" + vectors.size() +
                ", limit=" + limit +
                ", offset=" + offset +
                ", outputFields=" + outputFields +
                ", consistencyLevel='" + consistencyLevel + '\'' +
                '}';
    }
}

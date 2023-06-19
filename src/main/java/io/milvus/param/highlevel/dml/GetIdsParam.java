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
import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * Parameters for <code>get</code> interface.
 */
@Getter
@ToString
public class GetIdsParam {
    private final String collectionName;
    private final List<?> primaryIds;
    private final List<String> outputFields;

    private GetIdsParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.primaryIds = builder.primaryIds;
        this.outputFields = builder.outputFields;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link GetIdsParam} class.
     */
    public static class Builder<T> {
        private String collectionName;
        private List<T> primaryIds;
        private final List<String> outputFields = Lists.newArrayList();

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
         * @param outputFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutputFields(@NonNull List<String> outputFields) {
            this.outputFields.addAll(outputFields);
            return this;
        }

        /**
         * Specifies primaryIds fields. PrimaryIds cannot be empty or null.
         *
         * @param primaryIds input primary key list
         * @return <code>Builder</code>
         */
        public Builder withPrimaryIds(@NonNull List<T> primaryIds) {
            this.primaryIds = primaryIds;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link GetIdsParam} instance.
         *
         * @return {@link GetIdsParam}
         */
        public GetIdsParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            if (CollectionUtils.isEmpty(primaryIds)) {
                throw new ParamException("PrimaryIds cannot be empty");
            }

            return new GetIdsParam(this);
        }
    }
}

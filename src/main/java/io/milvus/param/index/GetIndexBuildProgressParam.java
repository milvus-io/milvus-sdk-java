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
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;

/**
 * @author changzechuan
 */
public class GetIndexBuildProgressParam {
    private final String collectionName;

    private GetIndexBuildProgressParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public static final class Builder {
        private String collectionName;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetIndexBuildProgressParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetIndexBuildProgressParam(this);
        }
    }
}

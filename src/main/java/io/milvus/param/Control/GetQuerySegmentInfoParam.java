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

package io.milvus.param.Control;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;

/**
 * @author:weilongzhao
 * @time:2021/9/4 23:01
 */
public class GetQuerySegmentInfoParam {
    private final String collectionName;

    private GetQuerySegmentInfoParam(@Nonnull GetQuerySegmentInfoParam.Builder builder) {
        this.collectionName = builder.collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public static final class Builder {
        private String collectionName;

        private Builder() {
        }

        public static GetQuerySegmentInfoParam.Builder newBuilder() {
            return new GetQuerySegmentInfoParam.Builder();
        }

        public GetQuerySegmentInfoParam.Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public GetQuerySegmentInfoParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            return new GetQuerySegmentInfoParam(this);
        }
    }
}

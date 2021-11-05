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
 * @time:2021/9/4 23:15
 */
public class GetMetricsRequestParam {
    private final String request;

    private GetMetricsRequestParam(@Nonnull Builder builder) {
        this.request = builder.request;
    }

    public String getRequest() {
        return request;
    }

    public static final class Builder {
        private String request;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String request) {
            this.request = request;
            return this;
        }

        public GetMetricsRequestParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(request, "Request string");

            // TODO: check the request string is json format

            return new GetMetricsRequestParam(this);
        }
    }
}

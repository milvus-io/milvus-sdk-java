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

import com.google.api.Context;
import com.google.api.Metric;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author changzechuan
 */
public class CreateIndexParam {
    private final String collectionName;
    private final String fieldName;
    private final Map<String, String> extraParam = new HashMap<>();

    private CreateIndexParam(@Nonnull Builder builder) {
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.extraParam.put(Constant.INDEX_TYPE, builder.indexType.name());
        this.extraParam.put(Constant.METRIC_TYPE, builder.metricType.name());
        this.extraParam.put(Constant.PARAMS, builder.extraParam);
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Map<String, String> getExtraParam() {
        return extraParam;
    }

    public static final class Builder {
        private String collectionName;
        private String fieldName;
        private IndexType indexType;
        private MetricType metricType;
        private String extraParam;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withCollectionName(@Nonnull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder withFieldName(@Nonnull String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder withIndexType(@Nonnull IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder withMetricType(@Nonnull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder withExtraParam(@Nonnull String extraParam) {
            this.extraParam = extraParam;
            return this;
        }

        public CreateIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(fieldName, "Field name");

            if (indexType == IndexType.INVALID) {
                throw new ParamException("Index type is required");
            }

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is required");
            }

            ParamUtils.CheckNullEmptyString(extraParam, "Index extra param");

            return new CreateIndexParam(this);
        }
    }
}

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

package io.milvus.bulkwriter;

import io.milvus.exception.ParamException;
import io.milvus.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

/**
 * Parameters for <code>stageManager</code> interface.
 */
@Getter
@ToString
public class StageManagerParam {
    private final String cloudEndpoint;
    private final String apiKey;

    private StageManagerParam(@NonNull Builder builder) {
        this.cloudEndpoint = builder.cloudEndpoint;
        this.apiKey = builder.apiKey;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link StageManagerParam} class.
     */
    public static final class Builder {
        private String cloudEndpoint;

        private String apiKey;

        private Builder() {
        }

        /**
         * The value of the URL is fixed.
         * For overseas regions, it is: https://api.cloud.zilliz.com
         * For regions in China, it is: https://api.cloud.zilliz.com.cn
         */
        public Builder withCloudEndpoint(@NotNull String cloudEndpoint) {
            this.cloudEndpoint = cloudEndpoint;
            return this;
        }

        public Builder withApiKey(@NotNull String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link StageManagerParam} instance.
         *
         * @return {@link StageManagerParam}
         */
        public StageManagerParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(cloudEndpoint, "cloudEndpoint");
            ParamUtils.CheckNullEmptyString(apiKey, "apiKey");

            return new StageManagerParam(this);
        }
    }

}

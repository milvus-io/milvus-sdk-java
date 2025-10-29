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

package io.milvus.bulkwriter.request.stage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BaseStageRequest implements Serializable {
    private static final long serialVersionUID = 8192049841043084620L;
    /**
     * If you are calling the cloud API, this parameter needs to be filled in; otherwise, you can ignore it.
     */
    private String apiKey;
    private Map<String, Object> options;

    protected BaseStageRequest() {
    }

    protected BaseStageRequest(String apiKey, Map<String, Object> options) {
        this.apiKey = apiKey;
        this.options = options;
    }

    protected BaseStageRequest(BaseStageRequestBuilder<?> builder) {
        this.apiKey = builder.apiKey;
        this.options = builder.options;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "BaseStageRequest{" +
                "apiKey='" + apiKey + '\'' +
                "options=" + options +
                '}';
    }

    public static BaseStageRequestBuilder<?> builder() {
        return new BaseStageRequestBuilder<>();
    }

    public static class BaseStageRequestBuilder<T extends BaseStageRequestBuilder<T>> {
        private String apiKey;
        private Map<String, Object> options;

        protected BaseStageRequestBuilder() {
            this.apiKey = "";
            this.options = new HashMap<>();
        }

        public T apiKey(String apiKey) {
            this.apiKey = apiKey;
            return (T) this;
        }

        public T options(Map<String, Object> options) {
            this.options = options;
            return (T) this;
        }

        public BaseStageRequest build() {
            return new BaseStageRequest(this);
        }
    }
}

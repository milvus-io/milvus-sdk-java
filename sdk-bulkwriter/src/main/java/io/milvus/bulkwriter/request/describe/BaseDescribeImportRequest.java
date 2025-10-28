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

package io.milvus.bulkwriter.request.describe;

import java.io.Serializable;

public class BaseDescribeImportRequest implements Serializable {
    private static final long serialVersionUID = -787626534606813089L;

    /**
     * If you are calling the cloud API, this parameter should be set to your API_KEY.
     * If you are using Milvus directly, this parameter should be set to your userName:password.
     */
    private String apiKey;

    public BaseDescribeImportRequest() {
    }

    public BaseDescribeImportRequest(String apiKey) {
        this.apiKey = apiKey;
    }

    protected BaseDescribeImportRequest(BaseDescribeImportRequestBuilder<?> builder) {
        this.apiKey = builder.apiKey;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String toString() {
        return "BaseDescribeImportRequest{" +
                "apiKey='" + apiKey + '\'' +
                '}';
    }

    public static BaseDescribeImportRequestBuilder<?> builder() {
        return new BaseDescribeImportRequestBuilder<>();
    }

    public static class BaseDescribeImportRequestBuilder<T extends BaseDescribeImportRequestBuilder<T>> {
        private String apiKey = "";

        protected BaseDescribeImportRequestBuilder() {
            this.apiKey = "";
        }

        public T apiKey(String apiKey) {
            this.apiKey = apiKey;
            return (T) this;
        }

        public BaseDescribeImportRequest build() {
            return new BaseDescribeImportRequest(this);
        }
    }
}

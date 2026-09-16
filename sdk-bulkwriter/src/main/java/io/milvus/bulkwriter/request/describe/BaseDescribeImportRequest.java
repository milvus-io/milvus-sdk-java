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

/**
 * Base request class for describing an import job.
 *
 * <p>Holds the API key required to query the status of an import job.</p>
 */


public class BaseDescribeImportRequest implements Serializable {
    private static final long serialVersionUID = -787626534606813089L;

    /**
     * If you are calling the cloud API, this parameter should be set to your API_KEY.
     * If you are using Milvus directly, this parameter should be set to your userName:password.
     */
    private String apiKey;
    /**
     * Creates a new BaseDescribeImportRequest.
     */


    public BaseDescribeImportRequest() {
    }
    /**
     * Creates a new BaseDescribeImportRequest.
     *
     * @param apiKey the apiKey
     */


    public BaseDescribeImportRequest(String apiKey) {
        this.apiKey = apiKey;
    }

    protected BaseDescribeImportRequest(BaseDescribeImportRequestBuilder<?> builder) {
        this.apiKey = builder.apiKey;
    }
    /**
     * Returns the apiKey.
     *
     * @return the apiKey
     */


    public String getApiKey() {
        return apiKey;
    }
    /**
     * Sets the apiKey.
     *
     * @param apiKey the apiKey
     */


    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String toString() {
        return "BaseDescribeImportRequest{" +
                "apiKey='" + apiKey + '\'' +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static BaseDescribeImportRequestBuilder<?> builder() {
        return new BaseDescribeImportRequestBuilder<>();
    }

    /**
     * Builder for {@link BaseDescribeImportRequest} class.
     */


    public static class BaseDescribeImportRequestBuilder<T extends BaseDescribeImportRequestBuilder<T>> {
        private String apiKey = "";

        protected BaseDescribeImportRequestBuilder() {
            this.apiKey = "";
        }
        /**
         * Sets the apiKey.
         *
         * @param apiKey the apiKey
         * @return this builder
         */


        public T apiKey(String apiKey) {
            this.apiKey = apiKey;
            return (T) this;
        }
        /**
         * Builds the BaseDescribeImportRequest.
         *
         * @return the built BaseDescribeImportRequest
         */


        public BaseDescribeImportRequest build() {
            return new BaseDescribeImportRequest(this);
        }
    }
}

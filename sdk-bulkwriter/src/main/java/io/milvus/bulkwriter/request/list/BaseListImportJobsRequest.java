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

package io.milvus.bulkwriter.request.list;

import java.io.Serializable;

/**
 * Base request class for listing import jobs.
 *
 * <p>Holds the API key required to query the list of import jobs.</p>
 */


public class BaseListImportJobsRequest implements Serializable {
    private static final long serialVersionUID = -1890380396466908530L;
    /**
     * If you are calling the cloud API, this parameter should be set to your API_KEY.
     * If you are using Milvus directly, this parameter should be set to your userName:password.
     */
    private String apiKey;

    protected BaseListImportJobsRequest() {
    }

    protected BaseListImportJobsRequest(String apiKey) {
        this.apiKey = apiKey;
    }

    protected BaseListImportJobsRequest(BaseListImportJobsRequestBuilder<?> builder) {
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
        return "BaseListImportJobsRequest{" +
                "apiKey='" + apiKey + '\'' +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static BaseListImportJobsRequestBuilder<?> builder() {
        return new BaseListImportJobsRequestBuilder<>();
    }

    /**
     * Builder for {@link BaseListImportJobsRequest} class.
     */


    public static class BaseListImportJobsRequestBuilder<T extends BaseListImportJobsRequestBuilder<T>> {
        private String apiKey = "";

        protected BaseListImportJobsRequestBuilder() {
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
         * Builds the BaseListImportJobsRequest.
         *
         * @return the built BaseListImportJobsRequest
         */


        public BaseListImportJobsRequest build() {
            return new BaseListImportJobsRequest(this);
        }
    }
}

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

package io.milvus.bulkwriter.request.volume;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Base request class for volume operations.
 *
 * <p>Holds the API key and additional options shared by volume requests. The API key needs
 * to be filled in when calling the cloud API.</p>
 */


public class BaseVolumeRequest implements Serializable {
    private static final long serialVersionUID = 8192049841043084620L;
    /**
     * If you are calling the cloud API, this parameter needs to be filled in; otherwise, you can ignore it.
     */
    private String apiKey;
    private Map<String, Object> options;

    protected BaseVolumeRequest() {
    }

    protected BaseVolumeRequest(String apiKey, Map<String, Object> options) {
        this.apiKey = apiKey;
        this.options = options;
    }

    protected BaseVolumeRequest(BaseVolumeRequestBuilder<?> builder) {
        this.apiKey = builder.apiKey;
        this.options = builder.options;
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
    /**
     * Returns the options.
     *
     * @return the options
     */


    public Map<String, Object> getOptions() {
        return options;
    }
    /**
     * Sets the options.
     *
     * @param options the options
     */


    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "BaseVolumeRequest{" +
                "apiKey='" + apiKey + '\'' +
                "options=" + options +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static BaseVolumeRequestBuilder<?> builder() {
        return new BaseVolumeRequestBuilder<>();
    }

    /**
     * Builder for {@link BaseVolumeRequest} class.
     */


    public static class BaseVolumeRequestBuilder<T extends BaseVolumeRequestBuilder<T>> {
        private String apiKey;
        private Map<String, Object> options;

        protected BaseVolumeRequestBuilder() {
            this.apiKey = "";
            this.options = new HashMap<>();
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
         * Sets the options.
         *
         * @param options the options
         * @return this builder
         */


        public T options(Map<String, Object> options) {
            this.options = options;
            return (T) this;
        }
        /**
         * Builds the BaseVolumeRequest.
         *
         * @return the built BaseVolumeRequest
         */


        public BaseVolumeRequest build() {
            return new BaseVolumeRequest(this);
        }
    }
}

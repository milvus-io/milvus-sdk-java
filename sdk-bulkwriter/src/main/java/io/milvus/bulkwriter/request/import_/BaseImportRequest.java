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

package io.milvus.bulkwriter.request.import_;

import io.milvus.common.utils.RedactCredential;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Base request class for data import operations.
 *
 * <p>Holds the API key and additional import options shared by all import requests.</p>
 */


public class BaseImportRequest implements Serializable {
    private static final long serialVersionUID = 8192049841043084620L;
    /**
     * If you are calling the cloud API, this parameter should be set to your API_KEY.
     * If you are using Milvus directly, this parameter should be set to your userName:password.
     *
     * transient: the credential must not be serialized into the JSON request body; it is carried
     * only in the {@code Authorization: Bearer} header (consistent with all other Milvus SDKs).
     */
    private transient String apiKey;

    private Map<String, Object> options;

    // transient: RLS context is injected into the request options (rls_principal/skip_rls) by
    // BulkImportUtils, mirroring pymilvus' gRPC ImportRequest.options; it must not be serialized
    // as top-level request keys.
    private transient String rlsPrincipal;

    private transient Boolean skipRls;

    /**
     * Creates a new BaseImportRequest.
     */


    public BaseImportRequest() {
    }
    /**
     * Creates a new BaseImportRequest.
     *
     * @param apiKey the apiKey
     * @param options the options
     */


    public BaseImportRequest(String apiKey, Map<String, Object> options) {
        this.apiKey = apiKey;
        this.options = options;
    }

    protected BaseImportRequest(BaseImportRequestBuilder<?> builder) {
        this.apiKey = builder.apiKey;
        this.options = builder.options;
        this.rlsPrincipal = builder.rlsPrincipal;
        this.skipRls = builder.skipRls;
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

    /**
     * Returns the row-level security (RLS) principal for the import.
     *
     * @return the RLS principal
     */


    public String getRlsPrincipal() {
        return rlsPrincipal;
    }

    /**
     * Sets the row-level security (RLS) principal for the import.
     *
     * @param rlsPrincipal the RLS principal
     */


    public void setRlsPrincipal(String rlsPrincipal) {
        this.rlsPrincipal = rlsPrincipal;
    }

    /**
     * Returns whether the import should skip row-level security (RLS) enforcement.
     *
     * @return {@code true} to skip RLS, {@code null} for the server default
     */


    public Boolean getSkipRls() {
        return skipRls;
    }

    /**
     * Sets whether the import should skip row-level security (RLS) enforcement.
     *
     * @param skipRls {@code true} to skip RLS, {@code null} for the server default
     */


    public void setSkipRls(Boolean skipRls) {
        this.skipRls = skipRls;
    }

    @Override
    public String toString() {
        return "BaseImportRequest{" +
                "apiKey='" + RedactCredential.redactCredential(apiKey) + '\'' +
                "options=" + options +
                ", rlsPrincipal='" + rlsPrincipal + '\'' +
                ", skipRls=" + skipRls +
                '}';
    }
    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static BaseImportRequestBuilder<?> builder() {
        return new BaseImportRequestBuilder<>();
    }

    /**
     * Builder for {@link BaseImportRequest} class.
     */


    public static class BaseImportRequestBuilder<T extends BaseImportRequestBuilder<T>> {
        private String apiKey = "";
        private Map<String, Object> options;
        private String rlsPrincipal;
        private Boolean skipRls;

        protected BaseImportRequestBuilder() {
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
         * Sets the row-level security (RLS) principal for the import.
         *
         * @param rlsPrincipal the RLS principal
         * @return this builder
         */


        public T rlsPrincipal(String rlsPrincipal) {
            this.rlsPrincipal = rlsPrincipal;
            return (T) this;
        }
        /**
         * Sets whether the import should skip row-level security (RLS) enforcement.
         *
         * @param skipRls {@code true} to skip RLS, {@code null} for the server default
         * @return this builder
         */


        public T skipRls(Boolean skipRls) {
            this.skipRls = skipRls;
            return (T) this;
        }
        /**
         * Builds the BaseImportRequest.
         *
         * @return the built BaseImportRequest
         */


        public BaseImportRequest build() {
            return new BaseImportRequest(this);
        }
    }
}

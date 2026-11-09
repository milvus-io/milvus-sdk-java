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

package io.milvus.v2.service.rls.response;

import java.util.HashMap;
import java.util.Map;

/**
 * Response returned by the {@code getRLSPrincipalTags} API.
 */

public class GetRLSPrincipalTagsResp {
    private Map<String, Object> tags;

    private GetRLSPrincipalTagsResp(GetRLSPrincipalTagsRespBuilder builder) {
        this.tags = builder.tags;
    }

    /**
     * Returns the principal tags, preserving integer, float and array values.
     *
     * @return the principal tags
     */

    public Map<String, Object> getTags() {
        return tags;
    }

    /**
     * Sets the principal tags.
     *
     * @param tags the principal tags
     */

    public void setTags(Map<String, Object> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "GetRLSPrincipalTagsResp{" +
                "tags=" + tags +
                '}';
    }

    /**
     * Creates a new builder for {@link GetRLSPrincipalTagsResp}.
     *
     * @return a new {@link GetRLSPrincipalTagsRespBuilder}
     */

    public static GetRLSPrincipalTagsRespBuilder builder() {
        return new GetRLSPrincipalTagsRespBuilder();
    }

    /**
     * Builder for {@link GetRLSPrincipalTagsResp} class.
     */

    public static class GetRLSPrincipalTagsRespBuilder {
        private Map<String, Object> tags = new HashMap<>();

        private GetRLSPrincipalTagsRespBuilder() {
        }

        /**
         * Sets the principal tags.
         *
         * @param tags the principal tags
         * @return this builder
         */

        public GetRLSPrincipalTagsRespBuilder tags(Map<String, Object> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Builds the {@link GetRLSPrincipalTagsResp}.
         *
         * @return the built response
         */

        public GetRLSPrincipalTagsResp build() {
            return new GetRLSPrincipalTagsResp(this);
        }
    }
}

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

import java.util.ArrayList;
import java.util.List;

/**
 * Response returned by the {@code listRLSPrincipals} API.
 */

public class ListRLSPrincipalsResp {
    private List<String> principalNames;

    private ListRLSPrincipalsResp(ListRLSPrincipalsRespBuilder builder) {
        this.principalNames = builder.principalNames;
    }

    /**
     * Returns the principal names with stored tags in the collection.
     *
     * @return the list of principal names
     */

    public List<String> getPrincipalNames() {
        return principalNames;
    }

    /**
     * Sets the principal names.
     *
     * @param principalNames the list of principal names
     */

    public void setPrincipalNames(List<String> principalNames) {
        this.principalNames = principalNames;
    }

    @Override
    public String toString() {
        return "ListRLSPrincipalsResp{" +
                "principalNames=" + principalNames +
                '}';
    }

    /**
     * Creates a new builder for {@link ListRLSPrincipalsResp}.
     *
     * @return a new {@link ListRLSPrincipalsRespBuilder}
     */

    public static ListRLSPrincipalsRespBuilder builder() {
        return new ListRLSPrincipalsRespBuilder();
    }

    /**
     * Builder for {@link ListRLSPrincipalsResp} class.
     */

    public static class ListRLSPrincipalsRespBuilder {
        private List<String> principalNames = new ArrayList<>();

        private ListRLSPrincipalsRespBuilder() {
        }

        /**
         * Sets the principal names.
         *
         * @param principalNames the list of principal names
         * @return this builder
         */

        public ListRLSPrincipalsRespBuilder principalNames(List<String> principalNames) {
            this.principalNames = principalNames;
            return this;
        }

        /**
         * Builds the {@link ListRLSPrincipalsResp}.
         *
         * @return the built response
         */

        public ListRLSPrincipalsResp build() {
            return new ListRLSPrincipalsResp(this);
        }
    }
}

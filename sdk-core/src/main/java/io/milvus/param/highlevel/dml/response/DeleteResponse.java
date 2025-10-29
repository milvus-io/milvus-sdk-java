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

package io.milvus.param.highlevel.dml.response;

import java.util.List;

/**
 * Parameters for <code>delete</code> interface.
 */
public class DeleteResponse {
    /**
     * In the new version(greater or equal than 2.3.2), this method only returns an empty list and does not return specific values
     * Mark is as deprecated, keep it to compatible with the legacy code
     */
    @Deprecated
    private List<?> deleteIds;

    private DeleteResponse(Builder builder) {
        this.deleteIds = builder.deleteIds;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @deprecated In the new version(greater or equal than 2.3.2), this method only returns an empty list and does not return specific values
     */
    @Deprecated
    public List<?> getDeleteIds() {
        return deleteIds;
    }

    @Override
    public String toString() {
        return "DeleteResponse{" +
                "deleteIds=" + deleteIds +
                '}';
    }

    public static class Builder {
        private List<?> deleteIds;

        /**
         * @deprecated In the new version(greater or equal than 2.3.2), this method only returns an empty list and does not return specific values
         */
        @Deprecated
        public Builder deleteIds(List<?> deleteIds) {
            this.deleteIds = deleteIds;
            return this;
        }

        public DeleteResponse build() {
            return new DeleteResponse(this);
        }
    }
}

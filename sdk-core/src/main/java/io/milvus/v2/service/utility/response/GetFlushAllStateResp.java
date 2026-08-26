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

package io.milvus.v2.service.utility.response;

/**
 * Response returned by the {@code getFlushAllState} API.
 */
public class GetFlushAllStateResp {
    private Boolean flushed;

    private GetFlushAllStateResp(GetFlushAllStateRespBuilder builder) {
        this.flushed = builder.flushed;
    }

    public static GetFlushAllStateRespBuilder builder() {
        return new GetFlushAllStateRespBuilder();
    }

    /**
     * Returns whether all data up to the flush-all timestamp has been flushed.
     *
     * @return {@code true} if the flush is complete, {@code false} otherwise
     */
    public Boolean getFlushed() {
        return flushed;
    }

    /**
     * Sets whether all data up to the flush-all timestamp has been flushed.
     *
     * @param flushed {@code true} if the flush is complete, {@code false} otherwise
     */
    public void setFlushed(Boolean flushed) {
        this.flushed = flushed;
    }

    @Override
    public String toString() {
        return "GetFlushAllStateResp{" +
                "flushed=" + flushed +
                '}';
    }

    public static class GetFlushAllStateRespBuilder {
        private Boolean flushed = Boolean.FALSE;

        /**
         * Sets whether all data up to the flush-all timestamp has been flushed.
         *
         * @param flushed {@code true} if the flush is complete, {@code false} otherwise
         * @return this builder
         */
        public GetFlushAllStateRespBuilder flushed(Boolean flushed) {
            this.flushed = flushed;
            return this;
        }

        /**
         * Builds the {@code GetFlushAllStateResp}.
         *
         * @return the constructed {@code GetFlushAllStateResp}
         */
        public GetFlushAllStateResp build() {
            return new GetFlushAllStateResp(this);
        }
    }
}

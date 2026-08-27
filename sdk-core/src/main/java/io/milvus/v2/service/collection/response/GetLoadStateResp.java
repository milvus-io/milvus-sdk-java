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

package io.milvus.v2.service.collection.response;

import io.milvus.grpc.LoadState;

/**
 * Response of the {@code getLoadState} API, holding the load state of a collection
 * or partition together with the load progress.
 */
public class GetLoadStateResp {
    private LoadState state;
    private Long progress;

    private GetLoadStateResp(GetLoadStateRespBuilder builder) {
        this.state = builder.state;
        this.progress = builder.progress;
    }

    /**
     * Returns the load state of the collection or partition.
     *
     * @return the load state
     */
    public LoadState getState() {
        return state;
    }

    /**
     * Sets the load state of the collection or partition.
     *
     * @param state the load state
     */
    public void setState(LoadState state) {
        this.state = state;
    }

    /**
     * Returns the name of the load state, or {@code null} if no state is set.
     *
     * @return the load state name
     */
    public String getStateName() {
        return state == null ? null : state.name();
    }

    /**
     * Returns the load progress of the collection or partition.
     *
     * @return the load progress
     */
    public Long getProgress() {
        return progress;
    }

    /**
     * Sets the load progress of the collection or partition.
     *
     * @param progress the load progress
     */
    public void setProgress(Long progress) {
        this.progress = progress;
    }

    @Override
    public String toString() {
        return "GetLoadStateResp{" +
                "state=" + state +
                ", stateName='" + getStateName() + '\'' +
                ", progress=" + progress +
                '}';
    }

    /**
     * Creates a new builder for {@link GetLoadStateResp}.
     *
     * @return the builder
     */
    public static GetLoadStateRespBuilder builder() {
        return new GetLoadStateRespBuilder();
    }

    public static class GetLoadStateRespBuilder {
        private LoadState state;
        private Long progress;

        private GetLoadStateRespBuilder() {
        }

        /**
         * Sets the load state of the collection or partition.
         *
         * @param state the load state
         * @return this builder
         */
        public GetLoadStateRespBuilder state(LoadState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the load progress of the collection or partition.
         *
         * @param progress the load progress
         * @return this builder
         */
        public GetLoadStateRespBuilder progress(Long progress) {
            this.progress = progress;
            return this;
        }

        /**
         * Builds a {@link GetLoadStateResp} with the configured parameters.
         *
         * @return the response
         */
        public GetLoadStateResp build() {
            return new GetLoadStateResp(this);
        }
    }
}

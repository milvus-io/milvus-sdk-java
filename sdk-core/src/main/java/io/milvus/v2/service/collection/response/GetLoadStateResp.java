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

public class GetLoadStateResp {
    private LoadState state;
    private Long progress;
    private Long refreshProgress;

    private GetLoadStateResp(GetLoadStateRespBuilder builder) {
        this.state = builder.state;
        this.progress = builder.progress;
        this.refreshProgress = builder.refreshProgress;
    }

    public LoadState getState() {
        return state;
    }

    public void setState(LoadState state) {
        this.state = state;
    }

    public String getStateName() {
        return state == null ? null : state.name();
    }

    public Long getProgress() {
        return progress;
    }

    public void setProgress(Long progress) {
        this.progress = progress;
    }

    public Long getRefreshProgress() {
        return refreshProgress;
    }

    public void setRefreshProgress(Long refreshProgress) {
        this.refreshProgress = refreshProgress;
    }

    @Override
    public String toString() {
        return "GetLoadStateResp{" +
                "state=" + state +
                ", stateName='" + getStateName() + '\'' +
                ", progress=" + progress +
                ", refreshProgress=" + refreshProgress +
                '}';
    }

    public static GetLoadStateRespBuilder builder() {
        return new GetLoadStateRespBuilder();
    }

    public static class GetLoadStateRespBuilder {
        private LoadState state;
        private Long progress;
        private Long refreshProgress;

        private GetLoadStateRespBuilder() {
        }

        public GetLoadStateRespBuilder state(LoadState state) {
            this.state = state;
            return this;
        }

        public GetLoadStateRespBuilder progress(Long progress) {
            this.progress = progress;
            return this;
        }

        public GetLoadStateRespBuilder refreshProgress(Long refreshProgress) {
            this.refreshProgress = refreshProgress;
            return this;
        }

        public GetLoadStateResp build() {
            return new GetLoadStateResp(this);
        }
    }
}

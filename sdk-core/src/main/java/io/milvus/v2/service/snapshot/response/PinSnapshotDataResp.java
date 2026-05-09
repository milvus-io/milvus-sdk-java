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

package io.milvus.v2.service.snapshot.response;

public class PinSnapshotDataResp {
    private Long pinId;

    private PinSnapshotDataResp(PinSnapshotDataRespBuilder builder) {
        this.pinId = builder.pinId;
    }

    public static PinSnapshotDataRespBuilder builder() {
        return new PinSnapshotDataRespBuilder();
    }

    public Long getPinId() {
        return pinId;
    }

    public void setPinId(Long pinId) {
        this.pinId = pinId;
    }

    @Override
    public String toString() {
        return "PinSnapshotDataResp{" +
                "pinId=" + pinId +
                '}';
    }

    public static class PinSnapshotDataRespBuilder {
        private Long pinId;

        public PinSnapshotDataRespBuilder pinId(Long pinId) {
            this.pinId = pinId;
            return this;
        }

        public PinSnapshotDataResp build() {
            return new PinSnapshotDataResp(this);
        }
    }
}

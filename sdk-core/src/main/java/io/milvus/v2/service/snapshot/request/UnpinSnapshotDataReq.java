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

package io.milvus.v2.service.snapshot.request;

public class UnpinSnapshotDataReq {
    private Long pinId;

    private UnpinSnapshotDataReq(UnpinSnapshotDataReqBuilder builder) {
        this.pinId = builder.pinId;
    }

    public static UnpinSnapshotDataReqBuilder builder() {
        return new UnpinSnapshotDataReqBuilder();
    }

    public Long getPinId() {
        return pinId;
    }

    public void setPinId(Long pinId) {
        this.pinId = pinId;
    }

    @Override
    public String toString() {
        return "UnpinSnapshotDataReq{" +
                "pinId=" + pinId +
                '}';
    }

    public static class UnpinSnapshotDataReqBuilder {
        private Long pinId;

        public UnpinSnapshotDataReqBuilder pinId(Long pinId) {
            this.pinId = pinId;
            return this;
        }

        public UnpinSnapshotDataReq build() {
            return new UnpinSnapshotDataReq(this);
        }
    }
}

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

package io.milvus.v2.service.utility.request;

public class FlushAllReq {
    private String databaseName;
    private Long waitFlushedTimeoutMs;

    private FlushAllReq(FlushAllReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.waitFlushedTimeoutMs = builder.waitFlushedTimeoutMs;
    }

    public static FlushAllReqBuilder builder() {
        return new FlushAllReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getWaitFlushedTimeoutMs() {
        return waitFlushedTimeoutMs;
    }

    public void setWaitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
        this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
    }

    @Override
    public String toString() {
        return "FlushAllReq{" +
                "databaseName='" + databaseName + '\'' +
                ", waitFlushedTimeoutMs=" + waitFlushedTimeoutMs +
                '}';
    }

    public static class FlushAllReqBuilder {
        private String databaseName;
        private Long waitFlushedTimeoutMs = 0L;

        public FlushAllReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public FlushAllReqBuilder waitFlushedTimeoutMs(Long waitFlushedTimeoutMs) {
            this.waitFlushedTimeoutMs = waitFlushedTimeoutMs;
            return this;
        }

        public FlushAllReq build() {
            return new FlushAllReq(this);
        }
    }
}

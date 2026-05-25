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

public class GetFlushAllStateReq {
    private String databaseName;
    private Long flushAllTs;

    private GetFlushAllStateReq(GetFlushAllStateReqBuilder builder) {
        this.databaseName = builder.databaseName;
        this.flushAllTs = builder.flushAllTs;
    }

    public static GetFlushAllStateReqBuilder builder() {
        return new GetFlushAllStateReqBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getFlushAllTs() {
        return flushAllTs;
    }

    public void setFlushAllTs(Long flushAllTs) {
        this.flushAllTs = flushAllTs;
    }

    @Override
    public String toString() {
        return "GetFlushAllStateReq{" +
                "databaseName='" + databaseName + '\'' +
                ", flushAllTs=" + flushAllTs +
                '}';
    }

    public static class GetFlushAllStateReqBuilder {
        private String databaseName;
        private Long flushAllTs = 0L;

        public GetFlushAllStateReqBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public GetFlushAllStateReqBuilder flushAllTs(Long flushAllTs) {
            this.flushAllTs = flushAllTs;
            return this;
        }

        public GetFlushAllStateReq build() {
            return new GetFlushAllStateReq(this);
        }
    }
}

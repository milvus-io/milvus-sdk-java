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

package io.milvus.v2.service.collection.request;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class RefreshLoadReq {
    private String databaseName;
    private String collectionName;
    @Builder.Default
    private Boolean async = Boolean.TRUE;
    @Builder.Default
    private Boolean sync = Boolean.TRUE; // wait the collection to be fully loaded. "async" is deprecated, use "sync" instead
    @Builder.Default
    private Long timeout = 60000L; // timeout value for waiting the collection to be fully loaded

    public static abstract class RefreshLoadReqBuilder<C extends RefreshLoadReq, B extends RefreshLoadReq.RefreshLoadReqBuilder<C, B>> {
        public B async(Boolean async) {
            this.async$value = async;
            this.async$set = true;
            this.sync$value = !async;
            this.sync$set = true;
            return self();
        }

        public B sync(Boolean sync) {
            this.sync$value = sync;
            this.sync$set = true;
            this.async$value = !sync;
            this.async$set = true;
            return self();
        }
    }
}

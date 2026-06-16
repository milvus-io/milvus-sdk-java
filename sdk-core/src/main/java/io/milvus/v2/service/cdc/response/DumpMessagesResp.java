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

package io.milvus.v2.service.cdc.response;

import java.util.Collections;
import java.util.Iterator;

public class DumpMessagesResp implements Iterable<DumpMessageInfo> {
    private final Iterable<DumpMessageInfo> messages;

    private DumpMessagesResp(DumpMessagesRespBuilder builder) {
        this.messages = builder.messages != null ? builder.messages : Collections.emptyList();
    }

    public static DumpMessagesRespBuilder builder() {
        return new DumpMessagesRespBuilder();
    }

    public Iterable<DumpMessageInfo> getMessages() {
        return messages;
    }

    @Override
    public Iterator<DumpMessageInfo> iterator() {
        return messages.iterator();
    }

    @Override
    public String toString() {
        return "DumpMessagesResp{messages=<stream>}";
    }

    public static class DumpMessagesRespBuilder {
        private Iterable<DumpMessageInfo> messages;

        public DumpMessagesRespBuilder messages(Iterable<DumpMessageInfo> messages) {
            this.messages = messages;
            return this;
        }

        public DumpMessagesResp build() {
            return new DumpMessagesResp(this);
        }
    }
}

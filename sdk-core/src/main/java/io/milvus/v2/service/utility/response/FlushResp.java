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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlushResp {
    private String databaseName;
    private Map<String, List<Long>> collectionSegmentIDs;
    private Map<String, Long> collectionFlushTs;

    private FlushResp(FlushRespBuilder builder) {
        this.databaseName = builder.databaseName;
        this.collectionSegmentIDs = builder.collectionSegmentIDs;
        this.collectionFlushTs = builder.collectionFlushTs;
    }

    public static FlushRespBuilder builder() {
        return new FlushRespBuilder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, List<Long>> getCollectionSegmentIDs() {
        return collectionSegmentIDs;
    }

    public void setCollectionSegmentIDs(Map<String, List<Long>> collectionSegmentIDs) {
        this.collectionSegmentIDs = collectionSegmentIDs;
    }

    public Map<String, Long> getCollectionFlushTs() {
        return collectionFlushTs;
    }

    public void setCollectionFlushTs(Map<String, Long> collectionFlushTs) {
        this.collectionFlushTs = collectionFlushTs;
    }

    @Override
    public String toString() {
        return "FlushResp{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionSegmentIDs=" + collectionSegmentIDs +
                ", collectionFlushTs=" + collectionFlushTs +
                '}';
    }

    public static class FlushRespBuilder {
        private String databaseName = "";
        private Map<String, List<Long>> collectionSegmentIDs = new HashMap<>();
        private Map<String, Long> collectionFlushTs = new HashMap<>();

        public FlushRespBuilder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public FlushRespBuilder collectionSegmentIDs(Map<String, List<Long>> collectionSegmentIDs) {
            this.collectionSegmentIDs = collectionSegmentIDs;
            return this;
        }

        public FlushRespBuilder collectionFlushTs(Map<String, Long> collectionFlushTs) {
            this.collectionFlushTs = collectionFlushTs;
            return this;
        }

        public FlushResp build() {
            return new FlushResp(this);
        }
    }
}

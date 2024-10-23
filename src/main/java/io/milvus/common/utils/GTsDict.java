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

package io.milvus.common.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GTsDict {
    // GTsDict stores the last write timestamp for ConsistencyLevel.Session
    // It is a Map<String, Long>, key is the name of a collection, value is the last write timestamp of the collection.
    // It only takes effect when consistency level is Session.
    // For each dml action, the GTsDict is updated, the last write timestamp is returned from server-side.
    // When search/query/hybridSearch is called, and the consistency level is Session, the ts of the collection will
    // be passed to construct a guarantee_ts to the server.
    private final static GTsDict TS_DICT = new GTsDict();

    private GTsDict(){}

    public static GTsDict getInstance() {
        return TS_DICT;
    }

    private ConcurrentMap<String, Long> tsDict = new ConcurrentHashMap<>();

    public void updateCollectionTs(String collectionName, long ts) {
        // If the collection name exists, use its value to compare to the input ts,
        // only when the input ts is larger than the existing value, replace it with the input ts.
        // If the collection name doesn't exist, directly set the input value.
        tsDict.compute(collectionName, (key, value) -> (value == null) ? ts : ((ts > value) ? ts : value));
    }

    public Long getCollectionTs(String collectionName) {
        return tsDict.get(collectionName);
    }
}

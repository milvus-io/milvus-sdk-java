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

package io.milvus.param;

/**
 * Constant/static values for internal usage.
 */
public class Constant {
    // default value for search key
    public static final String VECTOR_TAG = "$0";
    public static final String VECTOR_FIELD = "anns_field";
    public static final String VECTOR_DIM = "dim";
    public static final String VARCHAR_MAX_LENGTH = "max_length";
    public static final String TOP_K = "topk";
    public static final String INDEX_TYPE = "index_type";
    public static final String METRIC_TYPE = "metric_type";
    public static final String ROUND_DECIMAL = "round_decimal";
    public static final String PARAMS = "params";
    public static final String ROW_COUNT = "row_count";
    public static final String BUCKET = "bucket";
    public static final String FAILED_REASON = "failed_reason";
    public static final String IMPORT_FILES = "files";
    public static final String IMPORT_COLLECTION = "collection";
    public static final String IMPORT_PARTITION = "partition";
    public static final String IMPORT_PROGRESS = "progress_percent";
    public static final String DEFAULT_INDEX_NAME = "";
    public final static String OFFSET = "offset";

    // constant values for general
    public static final String TTL_SECONDS = "collection.ttl.seconds";

    // max value for waiting loading collection/partition interval, unit: millisecond
    public static final Long MAX_WAITING_LOADING_INTERVAL = 2000L;

    // max value for waiting loading collection/partition timeout,  unit: second
    public static final Long MAX_WAITING_LOADING_TIMEOUT = 300L;

    // max value for waiting flushing collection/partition interval, unit: millisecond
    public static final Long MAX_WAITING_FLUSHING_INTERVAL = 2000L;

    // max value for waiting flushing collection/partition timeout,  unit: second
    public static final Long MAX_WAITING_FLUSHING_TIMEOUT = 300L;

    // max value for waiting create index interval, unit: millisecond
    public static final Long MAX_WAITING_INDEX_INTERVAL = 2000L;


    // set this value for "withGuaranteeTimestamp" of QueryParam/SearchParam
    // to instruct server execute query/search immediately.
    public static final Long GUARANTEE_EVENTUALLY_TS = 1L;

    // set this value for "withGuaranteeTimestamp" of QueryParam/SearchParam
    // to instruct server execute query/search after all DML operations finished.
    public static final Long GUARANTEE_STRONG_TS = 0L;
}

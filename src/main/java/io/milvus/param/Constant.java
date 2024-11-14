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
    public static final String ARRAY_MAX_CAPACITY = "max_capacity";
    public static final String TOP_K = "topk";
    public static final String IGNORE_GROWING = "ignore_growing";
    public static final String REDUCE_STOP_FOR_BEST = "reduce_stop_for_best";
    public static final String ITERATOR_FIELD = "iterator";
    public static final String GROUP_BY_FIELD = "group_by_field";
    public static final String GROUP_SIZE = "group_size";
    public static final String STRICT_GROUP_SIZE = "strict_group_size";

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
    public final static String LIMIT = "limit";
    public final static String DYNAMIC_FIELD_NAME = "$meta";
    // constant values for general
    public static final String TTL_SECONDS = "collection.ttl.seconds";
    public static final String MMAP_ENABLED = "mmap.enabled";
    public static final String DATABASE_REPLICA_NUMBER = "database.replica.number";
    public static final String DATABASE_RESOURCE_GROUPS = "database.resource_groups";

    public static final String COLLECTION_REPLICA_NUMBER = "collection.replica.number";
    public static final String COLLECTION_RESOURCE_GROUPS = "collection.resource_groups";

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

    // high level api
    public static final String VECTOR_FIELD_NAME_DEFAULT  = "vector";
    public static final String PRIMARY_FIELD_NAME_DEFAULT = "id";
    public static final String VECTOR_INDEX_NAME_DEFAULT  = "vector_idx";
    public static final Long LIMIT_DEFAULT = 100L;
    public static final Long OFFSET_DEFAULT = 0L;
    public static final String ALL_OUTPUT_FIELDS = "*";

    public static final int MAX_BATCH_SIZE = 16384;
    public static final int NO_CACHE_ID = -1;
    public static final int UNLIMITED = -1;
    public static final int DEFAULT_SEARCH_EXTENSION_RATE = 10;
    public static final int MAX_FILTERED_IDS_COUNT_ITERATION = 100000;
    public static final int MAX_TRY_TIME = 20;

    public static final String RADIUS = "radius";
    public static final String EF = "ef";
    public static final String RANGE_FILTER = "range_filter";

}

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

package io.milvus.param.index;

import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;

import lombok.Getter;
import lombok.NonNull;
import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for <code>createIndex</code> interface.
 */
@Getter
public class CreateIndexParam {
    private final String collectionName;
    private final String fieldName;
    private final Map<String, String> extraParam = new HashMap<>();
    private final boolean syncMode;
    private final long syncWaitingInterval;
    private final long syncWaitingTimeout;

    private CreateIndexParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.extraParam.put(Constant.INDEX_TYPE, builder.indexType.name());
        this.extraParam.put(Constant.METRIC_TYPE, builder.metricType.name());
        this.extraParam.put(Constant.PARAMS, builder.extraParam);
        this.syncMode = builder.syncMode;
        this.syncWaitingInterval = builder.syncWaitingInterval;
        this.syncWaitingTimeout = builder.syncWaitingTimeout;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for <code>CreateIndexParam</code> class.
     */
    public static final class Builder {
        private String collectionName;
        private String fieldName;
        private IndexType indexType;
        private MetricType metricType;
        private String extraParam;

        // syncMode:
        //   Default behavior is sync mode, createIndex() return after the index successfully created.
        private Boolean syncMode = Boolean.TRUE;

        // syncWaitingDuration:
        //   When syncMode is ture, createIndex() return after the index successfully created.
        //   this value control the waiting interval. Unit: millisecond. Default value: 500 milliseconds.
        private Long syncWaitingInterval = 500L;

        // syncWaitingTimeout:
        //   When syncMode is ture, createIndex() return after the index successfully created.
        //   this value control the waiting timeout. Unit: second. Default value: 600 seconds.
        private Long syncWaitingTimeout = 600L;

        private Builder() {
        }

        /**
         * Set collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Set target field name. Field name cannot be empty or null.
         *
         * @param fieldName field name
         * @return <code>Builder</code>
         */
        public Builder withFieldName(@NonNull String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Set index type of the index.
         *
         * @param indexType index type
         * @return <code>Builder</code>
         */
        public Builder withIndexType(@NonNull IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        /**
         * Set metric type of the index.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Set extra index parameters according to index type.
         *
         * For example: IVF index, the extra parameters can be "{\"nlist\":1024}"
         * For more information: @see <a href="https://milvus.io/docs/v2.0.0/index_selection.md">Index Selection</a>
         *
         * @param extraParam extra parameters in json format
         * @return <code>Builder</code>
         */
        public Builder withExtraParam(@NonNull String extraParam) {
            this.extraParam = extraParam;
            return this;
        }

        /**
         * Set to sync mode.
         * With sync mode, the client side will keep waiting until all segments of the collection successfully indexed.
         *
         * If not sync mode, client will return at once after the createIndex() is called.
         *
         * @param syncMode <code>Boolean.TRUE</code> is sync mode, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withSyncMode(@NonNull Boolean syncMode) {
            this.syncMode = syncMode;
            return this;
        }

        /**
         * Set waiting interval in sync mode. In sync mode, the client will constantly check index state by interval.
         * Interval must be larger than zero, and cannot be larger than Constant.MAX_WAITING_INDEX_INTERVAL.
         * @see Constant
         *
         * @param milliseconds interval
         * @return <code>Builder</code>
         */
        public Builder withSyncWaitingInterval(@NonNull Long milliseconds) {
            this.syncWaitingInterval = milliseconds;
            return this;
        }

        /**
         * Set time out value for sync mode.
         * Time out value must be larger than zero. No upper limit. Default value is 600 seconds.
         * @see Constant
         *
         * @param seconds time out value for sync mode
         * @return <code>Builder</code>
         */
        public Builder withSyncWaitingTimeout(@NonNull Long seconds) {
            this.syncWaitingTimeout = seconds;
            return this;
        }

        /**
         * Verify parameters and create a new <code>CreateIndexParam</code> instance.
         *
         * @return <code>CreateIndexParam</code>
         */
        public CreateIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(fieldName, "Field name");

            if (indexType == IndexType.INVALID) {
                throw new ParamException("Index type is required");
            }

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is required");
            }

            if (syncMode == Boolean.TRUE) {
                if (syncWaitingInterval <= 0) {
                    throw new ParamException("Sync index waiting interval must be larger than zero");
                } else if (syncWaitingInterval > Constant.MAX_WAITING_LOADING_INTERVAL) {
                    throw new ParamException("Sync index waiting interval cannot be larger than "
                            + Constant.MAX_WAITING_LOADING_INTERVAL.toString() + " milliseconds");
                }

                if (syncWaitingTimeout <= 0) {
                    throw new ParamException("Sync index waiting timeout must be larger than zero");
                }
            }

            ParamUtils.CheckNullEmptyString(extraParam, "Index extra param");

            return new CreateIndexParam(this);
        }
    }

    /**
     * Construct a <code>String</code> by <code>CreateIndexParam</code> instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "CreateIndexParam{" +
                "collectionName='" + collectionName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", params='" + extraParam.toString() + '\'' +
                '}';
    }
}

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

package io.milvus.response;

import io.milvus.grpc.GetPartitionStatisticsResponse;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.Constant;

import java.util.List;

/**
 * Utility class to wrap response of <code>getPartitionStatistics</code> interface.
 */
public class GetPartStatResponseWrapper {
    private final GetPartitionStatisticsResponse stat;

    public GetPartStatResponseWrapper(GetPartitionStatisticsResponse stat) {
        if (stat == null) {
            throw new IllegalArgumentException("GetPartitionStatisticsResponse cannot be null");
        }
        this.stat = stat;
    }

    /**
     * Gets the row count of a field.
     * Throw {@link NumberFormatException} if the row count is not a number.
     *
     * @return <code>int</code> dimension of the vector field
     */
    public long getRowCount() throws NumberFormatException {
        List<KeyValuePair> stats = stat.getStatsList();
        for (KeyValuePair kv : stats) {
            if (kv.getKey().compareTo(Constant.ROW_COUNT) == 0) {
                return Long.parseLong(kv.getValue());
            }
        }

        return 0;
    }

    /**
     * Constructs a <code>String</code> by {@link GetPartStatResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Partition Statistics{" + "row_count:" + getRowCount() + '}';
    }
}

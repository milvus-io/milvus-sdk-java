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

import io.milvus.exception.IllegalResponseException;
import io.milvus.grpc.ShowPartitionsResponse;

import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class to wrap response of <code>showPartitions</code> interface.
 */
public class ShowPartResponseWrapper {
    private final ShowPartitionsResponse response;

    public ShowPartResponseWrapper(@NonNull ShowPartitionsResponse response) {
        this.response = response;
    }

    /**
     * Get information of the partitions.
     *
     * @return List of PartitionInfo, information array of the partitions
     */
    public List<PartitionInfo> getPartitionsInfo() throws IllegalResponseException {
        if (response.getPartitionNamesCount() != response.getPartitionIDsCount()
                || response.getPartitionNamesCount() != response.getCreatedUtcTimestampsCount()) {
            throw new IllegalResponseException("Partition information count doesn't match");
        }

        List<PartitionInfo> results = new ArrayList<>();
        for (int i = 0; i < response.getPartitionNamesCount(); ++i) {
            PartitionInfo info = new PartitionInfo(response.getPartitionNames(i), response.getPartitionIDs(i),
                    response.getCreatedUtcTimestamps(i));
            if (response.getInMemoryPercentagesCount() > i) {
                info.setInMemoryPercentage(response.getInMemoryPercentages(i));
            }
            results.add(info);
        }

        return results;
    }

    /**
     * Get information of one partition by name.
     *
     * @param partitionName partition name to get information
     * @return {@link PartitionInfo} information of the partition
     */
    public PartitionInfo getPartitionInfoByName(@NonNull String partitionName) {
        for (int i = 0; i < response.getPartitionNamesCount(); ++i) {
            if ( partitionName.compareTo(response.getPartitionNames(i)) == 0) {
                PartitionInfo info = new PartitionInfo(response.getPartitionNames(i), response.getPartitionIDs(i),
                        response.getCreatedUtcTimestamps(i));
                if (response.getInMemoryPercentagesCount() > i) {
                    info.setInMemoryPercentage(response.getInMemoryPercentages(i));
                }
                return info;
            }
        }

        return null;
    }

    /**
     * Internal-use class to wrap response of <code>showPartitions</code> interface.
     */
    @Getter
    public static final class PartitionInfo {
        private final String name;
        private final long id;
        private final long utcTimestamp;
        private long inMemoryPercentage = 0;

        public PartitionInfo(String name, long id, long utcTimestamp) {
            this.name = name;
            this.id = id;
            this.utcTimestamp = utcTimestamp;
        }

        public void setInMemoryPercentage(long inMemoryPercentage) {
            this.inMemoryPercentage = inMemoryPercentage;
        }

        @Override
        public String toString() {
            return "(name: " + getName() + " id: " + getId() + " utcTimestamp: " + getUtcTimestamp() +
                    " inMemoryPercentage: " + getInMemoryPercentage() + ")";
        }
    }

    /**
     * Construct a <code>String</code> by {@link ShowPartResponseWrapper} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "Partitions{" +
                getPartitionsInfo().toString() +
                '}';
    }
}


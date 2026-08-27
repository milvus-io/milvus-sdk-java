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

package io.milvus.v2.service.vector.request.data;

import io.milvus.grpc.PlaceholderType;

import java.util.SortedMap;

/**
 * A sparse float vector used in search and insert requests, stored as a sorted map of
 * dimension index to value so that only the non-zero dimensions need to be provided.
 */
public class SparseFloatVec implements BaseVector {
    private final SortedMap<Long, Float> data;

    /**
     * Constructs a sparse float vector from a sorted map of dimension index to value.
     *
     * @param data the sparse vector data
     */
    public SparseFloatVec(SortedMap<Long, Float> data) {
        this.data = data;
    }

    /**
     * Returns the placeholder type of a sparse float vector.
     *
     * @return the {@link PlaceholderType#SparseFloatVector} placeholder type
     */
    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.SparseFloatVector;
    }

    /**
     * Returns the sparse float vector data.
     *
     * @return the vector data as a map of dimension index to value
     */
    @Override
    public Object getData() {
        return this.data;
    }
}

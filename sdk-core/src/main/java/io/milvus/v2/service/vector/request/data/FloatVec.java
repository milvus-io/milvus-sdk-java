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

import java.util.ArrayList;
import java.util.List;

/**
 * A float vector used in search and insert requests, where each element represents one
 * dimension of the vector.
 */
public class FloatVec implements BaseVector {
    private final List<Float> data;

    /**
     * Constructs a float vector from a list of float values.
     *
     * @param data the vector values
     */
    public FloatVec(List<Float> data) {
        this.data = data;
    }

    /**
     * Constructs a float vector from a float array.
     *
     * @param data the vector values
     */
    public FloatVec(float[] data) {
        this.data = new ArrayList<>();
        for (float f : data) {
            this.data.add(f);
        }
    }

    /**
     * Returns the placeholder type of a float vector.
     *
     * @return the {@link PlaceholderType#FloatVector} placeholder type
     */
    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.FloatVector;
    }

    /**
     * Returns the float vector data.
     *
     * @return the vector data as a list of float values
     */
    @Override
    public Object getData() {
        return this.data;
    }
}

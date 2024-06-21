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

import io.milvus.common.utils.Float16Utils;
import io.milvus.grpc.PlaceholderType;

import java.nio.ByteBuffer;
import java.util.List;

public class BFloat16Vec implements BaseVector {
    private final ByteBuffer data;

    public BFloat16Vec(ByteBuffer data) {
        this.data = data;
    }
    public BFloat16Vec(byte[] data) {
        this.data = ByteBuffer.wrap(data);
    }

    /**
     * Construct a bfloat16 vector by a float32 array.
     * Note that all the float32 values will be cast to bfloat16 values and store into ByteBuffer.
     */
    public BFloat16Vec(List<Float> data) {
        this.data = Float16Utils.f32VectorToBf16Buffer(data);
    }

    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.BFloat16Vector;
    }

    @Override
    public Object getData() {
        return this.data;
    }
}

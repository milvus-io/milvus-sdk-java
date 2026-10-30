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

import java.nio.ByteBuffer;

/**
 * An int8 vector used in search and insert requests, where each byte represents one
 * dimension of the vector.
 */
public class Int8Vec implements BaseVector {
    private final ByteBuffer data;

    /**
     * Constructs an int8 vector from a {@link ByteBuffer}.
     *
     * @param data the int8 vector data
     */
    public Int8Vec(ByteBuffer data) {
        this.data = data;
    }

    /**
     * Constructs an int8 vector from a byte array.
     *
     * @param data the int8 vector data
     */
    public Int8Vec(byte[] data) {
        this.data = ByteBuffer.wrap(data);
    }

    /**
     * Returns the placeholder type of an int8 vector.
     *
     * @return the {@link PlaceholderType#Int8Vector} placeholder type
     */
    @Override
    public PlaceholderType getPlaceholderType() {
        return PlaceholderType.Int8Vector;
    }

    /**
     * Returns the int8 vector data.
     *
     * @return the vector data as a {@link ByteBuffer}
     */
    @Override
    public Object getData() {
        return this.data;
    }
}

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

/**
 * The base interface for all vector data types that can be used in search and insert
 * requests, such as float, binary, sparse float, int8, float16, and bfloat16 vectors.
 */
public interface BaseVector {
    /**
     * Returns the placeholder type of the vector, which describes how the vector data is
     * packed in the request.
     *
     * @return the placeholder type
     */
    PlaceholderType getPlaceholderType();

    /**
     * Returns the vector data.
     *
     * @return the vector data
     */
    Object getData();
}

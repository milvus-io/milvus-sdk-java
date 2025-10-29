package io.milvus.v2.service.vector.request.data;
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


import io.milvus.grpc.PlaceholderType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.util.ArrayList;
import java.util.List;

// EmbeddingList is mainly for searching vectors in struct field
public class EmbeddingList implements BaseVector {
    private List<BaseVector> data = new ArrayList<>();

    public void add(BaseVector vector) {
        if (!data.isEmpty() && data.get(0).getPlaceholderType() != vector.getPlaceholderType()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Not allow to add different types of vector");
        }
        data.add(vector);
    }

    @Override
    public PlaceholderType getPlaceholderType() {
        if (data.isEmpty()) {
            return PlaceholderType.None;
        }
        PlaceholderType pt = data.get(0).getPlaceholderType();
        switch (pt) {
            case FloatVector:
                return PlaceholderType.EmbListFloatVector;
            case BinaryVector:
                return PlaceholderType.EmbListBinaryVector;
            case Float16Vector:
                return PlaceholderType.EmbListFloat16Vector;
            case BFloat16Vector:
                return PlaceholderType.EmbListBFloat16Vector;
            case SparseFloatVector:
                return PlaceholderType.EmbListSparseFloatVector;
            case Int8Vector:
                return PlaceholderType.EmbListInt8Vector;
            default:
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Unsupported vector type: " + pt.name());
        }
    }

    @Override
    public Object getData() {
        if (data.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "EmbeddingList is empty");
        }

        // return the vectors as flatten
        PlaceholderType pt = data.get(0).getPlaceholderType();
        switch (pt) {
            case FloatVector:
                List<Object> floats = new ArrayList<>();
                for (BaseVector vec : data) {
                    floats.addAll((List<Object>) vec.getData());
                }
                return floats;
            default:
                // so far,
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Unsupported vector type: " + pt.name());
        }
    }
}

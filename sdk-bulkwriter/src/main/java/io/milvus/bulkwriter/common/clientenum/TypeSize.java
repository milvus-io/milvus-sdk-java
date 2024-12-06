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

package io.milvus.bulkwriter.common.clientenum;

import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;

public enum TypeSize {
    BOOL(DataType.Bool, 1),
    INT8(DataType.Int8, 1),
    INT16(DataType.Int16, 2),
    INT32(DataType.Int32, 4),
    INT64(DataType.Int64, 8),
    FLOAT(DataType.Float, 4),
    DOUBLE(DataType.Double, 8),

    ;
    private DataType dataType;
    private Integer size;

    TypeSize(DataType dataType, Integer size) {
        this.dataType = dataType;
        this.size = size;
    }

    public static boolean contains(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return true;
            }
        }
        return false;
    }

    public static Integer getSize(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return typeSize.size;
            }
        }
        throw new ParamException("TypeSize not contains this dataType: " + dataType);
    }

}

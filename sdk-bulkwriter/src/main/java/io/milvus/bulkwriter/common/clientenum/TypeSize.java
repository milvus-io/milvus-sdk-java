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
import io.milvus.v2.common.DataType;

/**
 * Maps Milvus scalar data types to the size in bytes that each value occupies in bulk data files.
 */
public enum TypeSize {
    /** Boolean values, 1 byte. */
    BOOL(DataType.Bool, 1),
    /** Int8 values, 1 byte. */
    INT8(DataType.Int8, 1),
    /** Int16 values, 2 bytes. */
    INT16(DataType.Int16, 2),
    /** Int32 values, 4 bytes. */
    INT32(DataType.Int32, 4),
    /** Int64 values, 8 bytes. */
    INT64(DataType.Int64, 8),
    /** Float values, 4 bytes. */
    FLOAT(DataType.Float, 4),
    /** Double values, 8 bytes. */
    DOUBLE(DataType.Double, 8),

    ;
    private DataType dataType;
    private Integer size;

    TypeSize(DataType dataType, Integer size) {
        this.dataType = dataType;
        this.size = size;
    }

    /**
     * Checks whether the given data type has a defined size in this enum.
     *
     * @param dataType the Milvus data type to check
     * @return {@code true} if the data type is mapped to a size
     */
    public static boolean contains(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the size in bytes of a value of the given data type.
     *
     * @param dataType the Milvus data type
     * @return the size in bytes
     * @throws io.milvus.exception.ParamException if the data type has no mapped size
     */
    public static Integer getSize(DataType dataType) {
        for (TypeSize typeSize : values()) {
            if (typeSize.dataType == dataType) {
                return typeSize.size;
            }
        }
        throw new ParamException("TypeSize not contains this dataType: " + dataType);
    }

}

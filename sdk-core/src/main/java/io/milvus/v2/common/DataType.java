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

package io.milvus.v2.common;

import java.util.Arrays;

/**
 * Data type of a field in a collection schema.
 */
public enum DataType {
    /**
     * No data type specified.
     */
    None(0),
    /**
     * A boolean data type.
     */
    Bool(1),
    /**
     * An 8-bit signed integer.
     */
    Int8(2),
    /**
     * A 16-bit signed integer.
     */
    Int16(3),
    /**
     * A 32-bit signed integer.
     */
    Int32(4),
    /**
     * A 64-bit signed integer.
     */
    Int64(5),

    /**
     * A 32-bit floating-point number.
     */
    Float(10),
    /**
     * A 64-bit floating-point number.
     */
    Double(11),

    /**
     * A variable-length string data type, alias of {@code VarChar}.
     */
    String(20),
    VarChar(21), // variable-length strings with a specified maximum length
    /**
     * An array whose elements share a scalar data type.
     */
    Array(22),
    /**
     * A JSON data type.
     */
    JSON(23),
    /**
     * A geometry data type for spatial data, expressed in GeoJSON.
     */
    Geometry(24),
    /**
     * A text data type supporting full-text search.
     */
    Text(25),
    /**
     * A timezone-aware timestamp data type.
     */
    Timestamptz(26),

    /**
     * A binary vector whose elements are single bits.
     */
    BinaryVector(100),
    /**
     * A float vector whose elements are 32-bit floats.
     */
    FloatVector(101),
    /**
     * A float vector whose elements are 16-bit floats.
     */
    Float16Vector(102),
    /**
     * A float vector whose elements are bfloat16 values.
     */
    BFloat16Vector(103),
    /**
     * A sparse float vector that stores only non-zero elements.
     */
    SparseFloatVector(104),
    /**
     * A vector whose elements are 8-bit integers.
     */
    Int8Vector(105),

    /**
     * A structured data type that groups multiple fields.
     */
    Struct(201);

    private final int code;

    DataType(int code) {
        this.code = code;
    }

    /**
     * Returns the numeric code of the data type.
     *
     * @return the numeric code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the data type that matches the given numeric code.
     *
     * @param code the numeric code of the data type
     * @return the matching data type, or {@code null} if none matches
     */
    public static DataType forNumber(int code) {
        return Arrays.stream(DataType.values())
                .filter(dataType -> dataType.code == code)
                .findFirst()
                .orElse(null);
    }
}

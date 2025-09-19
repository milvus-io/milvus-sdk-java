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

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum DataType {
    None(0),
    Bool(1),
    Int8(2),
    Int16(3),
    Int32(4),
    Int64(5),

    Float(10),
    Double(11),

    String(20),
    VarChar(21), // variable-length strings with a specified maximum length
    Array(22),
    JSON(23),

    BinaryVector(100),
    FloatVector(101),
    Float16Vector(102),
    BFloat16Vector(103),
    SparseFloatVector(104),
    Int8Vector(105),

    Struct(201);

    private final int code;
    DataType(int code) {
        this.code = code;
    }
    ;

    public static DataType forNumber(int code) {
        return Arrays.stream(DataType.values())
                .filter(dataType -> dataType.code == code)
                .findFirst()
                .orElse(null);
    }
}

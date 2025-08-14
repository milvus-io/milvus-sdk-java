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

package io.milvus.common.clientenum;

import lombok.Getter;

public enum FunctionType {
    UNKNOWN("Unknown", 0), // in milvus-proto, the name is "Unknown"
    BM25(1),
    ;

    private final String name;
    @Getter
    private final int code;

    FunctionType(){
        this.name = this.name();
        this.code = this.ordinal();
    }

    FunctionType(int code){
        this.name = this.name();
        this.code = code;
    }

    FunctionType(String name, int code){
        this.name = name;
        this.code = code;
    }

    public static FunctionType fromName(String name) {
        for (FunctionType type : FunctionType.values()) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        return null;
    }
}

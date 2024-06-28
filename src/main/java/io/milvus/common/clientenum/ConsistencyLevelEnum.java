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

public enum ConsistencyLevelEnum {

    STRONG("Strong", 0),
    // Session level is not allowed here because no ORM is implemented
//    SESSION("Session", 1),
    BOUNDED("Bounded", 2),
    EVENTUALLY("Eventually",3),
    ;

    @Getter
    private final String name;

    @Getter
    private final int code;

    ConsistencyLevelEnum(String name, int code){
        this.name = name;
        this.code = code;
    }

    private static final ConsistencyLevelEnum[] CONSISTENCY_LEVELS = new ConsistencyLevelEnum[values().length];

    public static ConsistencyLevelEnum getNameByCode(int code) {
        if (code >= 0 && code < CONSISTENCY_LEVELS.length) {
            return CONSISTENCY_LEVELS[code];
        }
        return null;
    }
}

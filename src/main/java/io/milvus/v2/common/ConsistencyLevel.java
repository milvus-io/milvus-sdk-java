package io.milvus.v2.common;

import lombok.Getter;
@Getter
public enum ConsistencyLevel{
    STRONG("Strong", 0),
    BOUNDED("Bounded", 2),
    EVENTUALLY("Eventually",3),
    ;
    private final String name;
    private final int code;
    ConsistencyLevel(String name, int code) {
        this.name = name;
        this.code = code;
    }
}

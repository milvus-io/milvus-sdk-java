package io.milvus.bulkwriter.common.clientenum;

public enum BulkFileType {
    PARQUET(1),
    ;

    private Integer code;

    BulkFileType(Integer code) {
        this.code = code;
    }
}

package com.zilliz.milvustestv2.params;

import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FieldParam {
    String fieldName;
    DataType dataType;
    int dim;
    int maxLength;
    int maxCapacity;
    DataType elementType;
    IndexParam.IndexType indextype;
}

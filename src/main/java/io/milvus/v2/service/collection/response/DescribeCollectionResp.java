package io.milvus.v2.service.collection.response;

import io.milvus.v2.service.collection.request.CreateCollectionReq;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
public class DescribeCollectionResp {
    private String collectionName;
    private String description;
    private Long numOfPartitions;

    private List<String> fieldNames;
    private List<String> vectorFieldName;
    private String primaryFieldName;
    private Boolean enableDynamicField;
    private Boolean autoID;

    private CreateCollectionReq.CollectionSchema collectionSchema;
    private Long createTime;
}

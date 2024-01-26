package io.milvus.v2.service.index;

import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(IndexTest.class);

    @Test
    void testCreateIndex() {
        IndexParam indexParam = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("vector")
                .build();

        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParam(indexParam)
                .build();
        client_v2.createIndex(createIndexReq);
    }
    @Test
    void testDescribeIndex() {
        DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        R<DescribeIndexResp> responseR = client_v2.describeIndex(describeIndexReq);
        logger.info(responseR.toString());
    }
    @Test
    void testDropIndex() {
        DropIndexReq dropIndexReq = DropIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        R<RpcStatus> resp = client_v2.dropIndex(dropIndexReq);
        Assertions.assertEquals(R.Status.Success.getCode(), resp.getStatus());
    }
}
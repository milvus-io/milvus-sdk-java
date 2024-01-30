package io.milvus.v2.service.index;

import io.milvus.v2.BaseTest;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class IndexTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(IndexTest.class);

    @Test
    void testCreateIndex() {
        // vector index
        IndexParam indexParam = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("vector")
                .build();
        // scalar index
        IndexParam scalarIndexParam = IndexParam.builder()
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("age")
                .build();
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParam);
        indexParams.add(scalarIndexParam);
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(indexParams)
                .build();
        client_v2.createIndex(createIndexReq);
    }
    @Test
    void testDescribeIndex() {
        DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        DescribeIndexResp responseR = client_v2.describeIndex(describeIndexReq);
        logger.info(responseR.toString());
    }
    @Test
    void testDropIndex() {
        DropIndexReq dropIndexReq = DropIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        client_v2.dropIndex(dropIndexReq);
    }
}
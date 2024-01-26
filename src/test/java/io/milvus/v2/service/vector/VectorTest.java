package io.milvus.v2.service.vector;

import com.alibaba.fastjson.JSONObject;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class VectorTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorTest.class);

    @Test
    void testInsert() {
        JSONObject vector = new JSONObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);
        vector.put("vector", vectorList);
        vector.put("id", 0L);

        InsertReq request = InsertReq.builder()
                .collectionName("test2")
                .insertData(Collections.singletonList(vector))
                .build();
        R<RpcStatus> statusR = client_v2.insert(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testUpsert() {

        JSONObject jsonObject = new JSONObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.put("vector", vectorList);
        //jsonObject.put("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .upsertData(Collections.singletonList(jsonObject))
                .build();

        R<RpcStatus> statusR = client_v2.upsert(request);
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testQuery() {
        QueryReq req = QueryReq.builder()
                .collectionName("test2")
                .expr("")
                .limit(10)
                //.outputFields(Collections.singletonList("count(*)"))
                .build();
        R<QueryResp> resultsR = client_v2.query(req);

        logger.info(resultsR.toString());
        Assertions.assertEquals(resultsR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testSearch() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .vectors(Collections.singletonList(vectorList))
                .topK(10)
                .offset(0L)
                .build();
        R<SearchResp> statusR = client_v2.search(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDelete() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .expr("id > 0")
                .build();
        R<RpcStatus> statusR = client_v2.delete(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testDeleteById(){
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList("0"))
                .build();
        R<RpcStatus> statusR = client_v2.delete(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }

    @Test
    void testGet() {
        GetReq request = GetReq.builder()
                .collectionName("test2")
                .ids(Collections.singletonList("447198483337881033"))
                .build();
        R<GetResp> statusR = client_v2.get(request);
        logger.info(statusR.toString());
        Assertions.assertEquals(statusR.getStatus(), R.Status.Success.getCode());
    }
}
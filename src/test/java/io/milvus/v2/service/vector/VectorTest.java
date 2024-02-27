package io.milvus.v2.service.vector;

import com.alibaba.fastjson.JSONObject;
import io.milvus.v2.BaseTest;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class VectorTest extends BaseTest {

    Logger logger = LoggerFactory.getLogger(VectorTest.class);

    @Test
    void testInsert() {

        List<JSONObject> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JSONObject vector = new JSONObject();
            List<Float> vectorList = new ArrayList<>();
            vectorList.add(1.0f);
            vectorList.add(2.0f);
            vector.put("vector", vectorList);
            vector.put("id", (long) i);
            data.add(vector);
        }

        InsertReq request = InsertReq.builder()
                .collectionName("test")
                .data(data)
                .build();
        InsertResp statusR = client_v2.insert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testUpsert() {

        JSONObject jsonObject = new JSONObject();
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(2.0f);
        vectorList.add(3.0f);
        jsonObject.put("vector", vectorList);
        jsonObject.put("id", 0L);
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(jsonObject))
                .build();

        UpsertResp statusR = client_v2.upsert(request);
        logger.info(statusR.toString());
    }

    @Test
    void testQuery() {
        QueryReq req = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(0))
                .limit(10)
                //.outputFields(Collections.singletonList("count(*)"))
                .build();
        QueryResp resultsR = client_v2.query(req);

        logger.info(resultsR.toString());
    }

    @Test
    void testSearch() {
        List<Float> vectorList = new ArrayList<>();
        vectorList.add(1.0f);
        vectorList.add(2.0f);
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(vectorList))
                .topK(10)
                .offset(0L)
                .build();
        SearchResp statusR = client_v2.search(request);
        logger.info(statusR.toString());
    }

    @Test
    void testDelete() {
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .filter("id > 0")
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testDeleteById(){
        DeleteReq request = DeleteReq.builder()
                .collectionName("test")
                .ids(Collections.singletonList("0"))
                .build();
        DeleteResp statusR = client_v2.delete(request);
        logger.info(statusR.toString());
    }

    @Test
    void testGet() {
        GetReq request = GetReq.builder()
                .collectionName("test2")
                .ids(Collections.singletonList("447198483337881033"))
                .build();
        GetResp statusR = client_v2.get(request);
        logger.info(statusR.toString());
    }
}
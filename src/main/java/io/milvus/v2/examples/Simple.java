package io.milvus.v2.examples;

import com.alibaba.fastjson.JSONObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Simple {
    Integer dim = 2;
    String collectionName = "book";
    static Logger logger = LoggerFactory.getLogger(Simple.class);
    public static void main(String[] args) {
        try {
            new Simple().run();
        } catch (MilvusClientException | InterruptedException e) {
            logger.info(e.toString());
        }
    }

    public void run() throws InterruptedException {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("https://in01-******.aws-us-west-2.vectordb.zillizcloud.com:19531")
                .token("*****")
                .build();
        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        // check collection exists
        if (client.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build())) {
            logger.info("collection exists");
            client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
            TimeUnit.SECONDS.sleep(1);
        }
        // create collection
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(dim)
                .build();
        client.createCollection(createCollectionReq);

        logger.info(String.valueOf(client.listCollections()));
        logger.info(String.valueOf(client.describeCollection(DescribeCollectionReq.builder().collectionName(collectionName).build())));
        //insert data
        List<JSONObject> insertData = new ArrayList<>();
        for(int i = 0; i < 6; i++){
            JSONObject jsonObject = new JSONObject();
            List<Float> vectorList = new ArrayList<>();
            for(int j = 0; j < dim; j++){
                // generate random float vector
                vectorList.add(new Random().nextFloat());
            }
            jsonObject.put("id", (long) i);
            jsonObject.put("vector", vectorList);
            insertData.add(jsonObject);
        }
        InsertReq insertReq = InsertReq.builder()
                .collectionName(collectionName)
                .data(insertData)
                .build();
        client.insert(insertReq);
        //query data
        QueryReq queryReq = QueryReq.builder()
                .collectionName(collectionName)
                .filter("id in [0]")
                .build();
        QueryResp queryResp = client.query(queryReq);
        System.out.println(queryResp);
        //search data
        SearchReq searchReq = SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(insertData.get(0).get("vector")))
                .topK(10)
                .build();
        SearchResp searchResp = client.search(searchReq);
        System.out.println(searchResp);
    }
}

package io.milvus.v2.examples;

import com.alibaba.fastjson.JSONObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
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

public class Simple_Schema {
    Integer dim = 2;
    String collectionName = "book";
    static Logger logger = LoggerFactory.getLogger(Simple_Schema.class);
    public void run() throws InterruptedException {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("https://in01-****.aws-us-west-2.vectordb.zillizcloud.com:19531")
                .token("******")
                .build();
        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        // check collection exists
        if (client.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build())) {
            logger.info("collection exists");
            client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
            TimeUnit.SECONDS.sleep(1);
        }
        // create collection
        CreateCollectionReq.CollectionSchema collectionSchema = client.createSchema(Boolean.TRUE, "");
        collectionSchema.addPrimaryField("id", DataType.Int64, Boolean.FALSE);
        collectionSchema.addVectorField("vector", DataType.FloatVector, dim);
        collectionSchema.addScalarField("num", DataType.Int32);

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(createCollectionReq);
        //create index
        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.COSINE)
                .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build();
        client.createIndex(createIndexReq);
        client.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
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
            jsonObject.put("num", i);
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
    public static void main(String[] args) {
        try {
            new Simple_Schema().run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

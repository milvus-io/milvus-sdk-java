package io.milvus.v2.examples;

import com.google.gson.*;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
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
                .uri("https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19531")
                .token("***")
                .build();
        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        // check collection exists
        if (client.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build())) {
            logger.info("collection exists");
            client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
        // create collection
        CreateCollectionReq.CollectionSchema collectionSchema = client.createSchema();
        collectionSchema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(Boolean.TRUE).autoID(Boolean.FALSE).description("id").build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(dim).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("num").dataType(DataType.Int64).isPartitionKey(Boolean.TRUE).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("array").dataType(DataType.Array).elementType(DataType.Int32).maxCapacity(10).description("array").build());

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .description("simple collection")
                .collectionSchema(collectionSchema)
                .enableDynamicField(Boolean.FALSE)
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
        TimeUnit.SECONDS.sleep(1);
        client.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
        //insert data
        List<JsonObject> insertData = new ArrayList<>();
        Gson gson = new Gson();
        for(int i = 0; i < 6; i++){
            JsonObject jsonObject = new JsonObject();
            List<Float> vectorList = new ArrayList<>();
            for(int j = 0; j < dim; j++){
                // generate random float vector
                vectorList.add(new Random().nextFloat());
            }
            List<Integer> array = new ArrayList<>();
            array.add(i);
            jsonObject.addProperty("id", (long) i);
            jsonObject.add("vector", gson.toJsonTree(vectorList).getAsJsonArray());
            jsonObject.addProperty("num", (long) i);
            jsonObject.add("array", gson.toJsonTree(array).getAsJsonArray());
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
        queryResp.getQueryResults().get(0).getEntity().get("vector");
        System.out.println(queryResp);
        //search data
        SearchReq searchReq = SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(insertData.get(0).get("vector")))
                .outputFields(Collections.singletonList("vector"))
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

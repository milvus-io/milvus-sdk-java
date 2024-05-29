/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2.examples;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
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
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
                .uri("https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19531")
                .token("***")
                .build();
        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        // check collection exists
        if (client.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build())) {
            logger.info("collection exists");
            client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
            logger.info("collection dropped");
        }
        // create collection
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .description("simple collection")
                .dimension(dim)
                .build();
        client.createCollection(createCollectionReq);

        logger.info(String.valueOf(client.listCollections()));
        logger.info(String.valueOf(client.describeCollection(DescribeCollectionReq.builder().collectionName(collectionName).build())));
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
            jsonObject.addProperty("id", (long) i);
            jsonObject.add("vector", gson.toJsonTree(vectorList).getAsJsonArray());
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
        List<Float> vector = new Gson().fromJson(insertData.get(0).get("vector"), new TypeToken<List<Float>>() {}.getType());
        SearchReq searchReq = SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(vector)))
                .topK(10)
                .build();
        SearchResp searchResp = client.search(searchReq);
        System.out.println(searchResp);
    }
}

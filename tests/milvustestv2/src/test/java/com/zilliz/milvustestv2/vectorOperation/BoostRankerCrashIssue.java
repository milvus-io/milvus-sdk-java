package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.BoostRanker;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 最小复现：hybridSearch + FunctionScore(BoostRanker) 导致 Milvus 2.6-20261022-2775715f9f 服务端宕机
 * 复现步骤：mvn test-compile exec:java -Dexec.mainClass="com.zilliz.milvustestv2.vectorOperation.BoostRankerCrashIssue" -Dexec.classpathScope=test
 * 现象：请求发出后服务端进程退出（19530/9091 端口失联），客户端阻塞至 rpcDeadline
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class BoostRankerCrashIssue {
    private static final int dim = 128;
    private static String uri = "http://localhost:19530";
    private static final String collectionName = "BoostRankerCrashRepro";
    private static final String pkName = "fieldInt64";
    private static final String numberName = "fieldNumber";
    private static final String vectorName1 = "floatVector1";
    private static final String vectorName2 = "floatVector2";

    public static void main(String[] args) {
        if (args.length > 0) {
            uri = args[0];
        }
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri(uri)
                .token("root:Milvus")
                .secure(false)
                .connectTimeoutMs(5000L)
                .rpcDeadlineMs(60000L)
                .build());

        // 1. create collection: Int64 pk + Int64 scalar + 2 FloatVector
        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .autoID(false).dataType(DataType.Int64).isPrimaryKey(true).name(pkName).build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.Int64).isPrimaryKey(false).name(numberName).build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector).dimension(dim).name(vectorName1).build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector).dimension(dim).name(vectorName2).build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(fieldSchemaList).build())
                .numShards(1)
                .build());
        System.out.println("collection created");

        // 2. insert 1000 rows
        Random ran = new Random();
        Gson gson = new Gson();
        List<JsonObject> data = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(pkName, i);
            row.addProperty(numberName, i);
            row.add(vectorName1, gson.toJsonTree(randomVector(ran)));
            row.add(vectorName2, gson.toJsonTree(randomVector(ran)));
            data.add(row);
        }
        client.insert(InsertReq.builder().collectionName(collectionName).data(data).build());
        System.out.println("inserted 1000 rows");

        // 3. index + load
        List<IndexParam> indexParams = new ArrayList<>();
        for (String vectorName : Arrays.asList(vectorName1, vectorName2)) {
            indexParams.add(IndexParam.builder()
                    .fieldName(vectorName)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .extraParams(new java.util.HashMap<String, Object>() {{
                        put("M", 16);
                        put("efConstruction", 64);
                    }})
                    .build());
        }
        client.createIndex(CreateIndexReq.builder().collectionName(collectionName).indexParams(indexParams).build());
        client.loadCollection(LoadCollectionReq.builder().collectionName(collectionName).build());
        System.out.println("loaded");

        // 4. hybridSearch/search with BoostRanker via functionScore —— 触发服务端宕机的请求
        // args[1]: "hybrid"(默认) 或 "search"
        String mode = args.length > 1 ? args[1] : "hybrid";
        BoostRanker boostRanker = BoostRanker.builder()
                .name("br")
                .filter(numberName + " > 500")
                .weight(2.0f)
                .build();
        List<BaseVector> vectors = Collections.singletonList(new FloatVec(randomVector(ran)));
        if ("hybrid-ranker".equalsIgnoreCase(mode)) {
            // hybridSearch + 旧版 .ranker(BoostRanker)（单 ranker 路径，非 FunctionScore）
            List<AnnSearchReq> searchRequests = new ArrayList<>();
            for (String vectorName : Arrays.asList(vectorName1, vectorName2)) {
                searchRequests.add(AnnSearchReq.builder()
                        .vectorFieldName(vectorName)
                        .vectors(vectors)
                        .limit(10)
                        .build());
            }
            System.out.println("sending hybridSearch with .ranker(BoostRanker)...");
            SearchResp searchResp = client.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(searchRequests)
                    .ranker(boostRanker)
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println("no crash, result size: " + searchResp.getSearchResults().size());
        } else if ("search".equalsIgnoreCase(mode)) {
            System.out.println("sending single search with BoostRanker...");
            SearchResp searchResp = client.search(io.milvus.v2.service.vector.request.SearchReq.builder()
                    .collectionName(collectionName)
                    .annsField(vectorName1)
                    .data(vectors)
                    .functionScore(FunctionScore.builder().addFunction(boostRanker).build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println("no crash, result size: " + searchResp.getSearchResults().size());
        } else {
            List<AnnSearchReq> searchRequests = new ArrayList<>();
            for (String vectorName : Arrays.asList(vectorName1, vectorName2)) {
                searchRequests.add(AnnSearchReq.builder()
                        .vectorFieldName(vectorName)
                        .vectors(vectors)
                        .limit(10)
                        .build());
            }
            System.out.println("sending hybridSearch with BoostRanker...");
            SearchResp searchResp = client.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(searchRequests)
                    .functionScore(FunctionScore.builder().addFunction(boostRanker).build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println("no crash, result size: " + searchResp.getSearchResults().size());
        }

        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }

    private static List<Float> randomVector(Random ran) {
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
            vector.add(ran.nextFloat());
        }
        return vector;
    }
}

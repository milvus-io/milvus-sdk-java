package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.*;


public class Float16VectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_float16";
    private static final String ID_FIELD = "id";
    private static final String FP16_VECTOR_FIELD = "fp16_vector";
    private static final String BF16_VECTOR_FIELD = "bf16_vector";
    private static final Integer VECTOR_DIM = 128;

    private static final MilvusClientV2 milvusClient;
    static {
        milvusClient = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection() {

        // drop the collection if you don't need the collection anymore
        Boolean has = milvusClient.hasCollection(HasCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        if (has) {
            dropCollection();
        }

        // build a collection with two vector fields
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(FP16_VECTOR_FIELD)
                .dataType(io.milvus.v2.common.DataType.Float16Vector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(BF16_VECTOR_FIELD)
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .dimension(VECTOR_DIM)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist",64);
        indexes.add(IndexParam.builder()
                .fieldName(FP16_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(BF16_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        milvusClient.createCollection(requestCreate);
    }

    private static void prepareData(int count) {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            ByteBuffer buf1 = CommonUtils.generateFloat16Vector(VECTOR_DIM, false);
            row.add(FP16_VECTOR_FIELD, gson.toJsonTree(buf1.array()));
            ByteBuffer buf2 = CommonUtils.generateFloat16Vector(VECTOR_DIM, true);
            row.add(BF16_VECTOR_FIELD, gson.toJsonTree(buf1.array()));
            rows.add(row);
        }

        InsertResp insertResp = milvusClient.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.println(insertResp.getInsertCnt() + " rows inserted");
    }

    private static void searchVectors(List<Long> taargetIDs, List<BaseVector> targetVectors, String vectorFieldName) {
        int topK = 5;
        SearchResp searchResp = milvusClient.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(targetVectors)
                .annsField(vectorFieldName)
                .topK(topK)
                .outputFields(Collections.singletonList(vectorFieldName))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        if (searchResults.isEmpty()) {
            throw new RuntimeException("The search result is empty");
        }

        for (int i = 0; i < searchResults.size(); i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            if (results.size() != topK) {
                throw new RuntimeException(String.format("The search result should contains top%d items", topK));
            }

            SearchResp.SearchResult topResult = results.get(0);
            long id = (long) topResult.getId();
            if (id != taargetIDs.get(i)) {
                throw new RuntimeException("The top1 id is incorrect");
            }
            Map<String, Object> entity = topResult.getEntity();
            ByteBuffer vectorBuf = (ByteBuffer) entity.get(vectorFieldName);
            if (!vectorBuf.equals(targetVectors.get(i).getData())) {
                throw new RuntimeException("The top1 output vector is incorrect");
            }
        }
        System.out.println("Search result of float16 vector is correct");
    }

    private static void search() {
        // retrieve some rows for search
        List<Long> targetIDs = Arrays.asList(999L, 2024L);
        QueryResp queryResp = milvusClient.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " in " + targetIDs)
                .outputFields(Arrays.asList(FP16_VECTOR_FIELD, BF16_VECTOR_FIELD))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        if (queryResults.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        }

        List<BaseVector> targetFP16Vectors = new ArrayList<>();
        List<BaseVector> targetBF16Vectors = new ArrayList<>();
        for (QueryResp.QueryResult queryResult : queryResults) {
            Map<String, Object> entity = queryResult.getEntity();
            ByteBuffer f16VectorBuf = (ByteBuffer) entity.get(FP16_VECTOR_FIELD);
            targetFP16Vectors.add(new Float16Vec(f16VectorBuf));
            ByteBuffer bf16VectorBuf = (ByteBuffer) entity.get(BF16_VECTOR_FIELD);
            targetBF16Vectors.add(new BFloat16Vec(bf16VectorBuf));
        }

        // search float16 vector
        searchVectors(targetIDs, targetFP16Vectors, FP16_VECTOR_FIELD);

        // search bfloat16 vector
        searchVectors(targetIDs, targetBF16Vectors, BF16_VECTOR_FIELD);
    }

    private static void dropCollection() {
        milvusClient.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");
    }


    public static void main(String[] args) {
        createCollection();
        prepareData(10000);
        search();
        dropCollection();
    }
}
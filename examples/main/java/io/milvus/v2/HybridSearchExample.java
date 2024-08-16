package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.common.DataType;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class HybridSearchExample {
    private static final MilvusClientV2 client;

    static {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        client = new MilvusClientV2(config);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_hybrid_search_v2";
    private static final String ID_FIELD = "ID";

    private static final String FLOAT_VECTOR_FIELD = "float_vector";
    private static final Integer FLOAT_VECTOR_DIM = 128;
    private static final IndexParam.MetricType FLOAT_VECTOR_METRIC = IndexParam.MetricType.COSINE;

    private static final String BINARY_VECTOR_FIELD = "binary_vector";
    private static final Integer BINARY_VECTOR_DIM = 256;
    private static final IndexParam.MetricType BINARY_VECTOR_METRIC = IndexParam.MetricType.JACCARD;

    private static final String FLOAT16_VECTOR_FIELD = "float16_vector";
    private static final Integer FLOAT16_VECTOR_DIM = 256;
    private static final IndexParam.MetricType FLOAT16_VECTOR_METRIC = IndexParam.MetricType.L2;

    private static final String SPARSE_VECTOR_FIELD = "sparse_vector";
    private static final IndexParam.MetricType SPARSE_VECTOR_METRIC = IndexParam.MetricType.IP;

    private void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(FLOAT_VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(FLOAT_VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(BINARY_VECTOR_FIELD)
                .dataType(DataType.BinaryVector)
                .dimension(BINARY_VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(FLOAT16_VECTOR_FIELD)
                .dataType(DataType.Float16Vector)
                .dimension(FLOAT16_VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(SPARSE_VECTOR_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> fvParams = new HashMap<>();
        fvParams.put("nlist",128);
        fvParams.put("m",16);
        fvParams.put("nbits",8);
        indexes.add(IndexParam.builder()
                .fieldName(FLOAT_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.IVF_PQ)
                .extraParams(fvParams)
                .metricType(FLOAT_VECTOR_METRIC)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(BINARY_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(BINARY_VECTOR_METRIC)
                .build());
        Map<String,Object> fv16Params = new HashMap<>();
        fv16Params.clear();
        fv16Params.put("M",16);
        fv16Params.put("efConstruction",64);
        indexes.add(IndexParam.builder()
                .fieldName(FLOAT16_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.HNSW)
                .extraParams(fv16Params)
                .metricType(FLOAT16_VECTOR_METRIC)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(SPARSE_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(SPARSE_VECTOR_METRIC)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");
    }

    private void insertData() {
        long idCount = 0;
        int rowCount = 10000;
        // Insert entities by rows
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 1L; i <= rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, idCount++);
            row.add(FLOAT_VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
            row.add(BINARY_VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
            row.add(FLOAT16_VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloat16Vector(FLOAT16_VECTOR_DIM, false).array()));
            row.add(SPARSE_VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateSparseVector()));
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        System.out.printf("%d entities inserted by rows\n", rowCount);
    }

    private void hybridSearch() {
        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Search on multiple vector fields
        int NQ = 2;
        List<BaseVector> floatVectors = new ArrayList<>();
        List<BaseVector> binaryVectors = new ArrayList<>();
        List<BaseVector> sparseVectors = new ArrayList<>();
        for (int i = 0; i < NQ; i++) {
            floatVectors.add(new FloatVec(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
            binaryVectors.add(new BinaryVec(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM)));
            sparseVectors.add(new SparseFloatVec(CommonUtils.generateSparseVector()));
        }

        List<AnnSearchReq> searchRequests = new ArrayList<>();
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName("float_vector")
                .vectors(floatVectors)
                .params("{\"nprobe\": 10}")
                .topK(10)
                .build());
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName("binary_vector")
                .vectors(binaryVectors)
                .topK(50)
                .build());
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName("sparse_vector")
                .vectors(sparseVectors)
                .topK(100)
                .build());

        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(searchRequests)
                .ranker(new RRFRanker(2))
                .topK(5)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        SearchResp searchResp = client.hybridSearch(hybridSearchReq);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < NQ; i++) {
            System.out.printf("============= Search result of No.%d vector =============\n", i);
            List<SearchResp.SearchResult> results = searchResults.get(i);
            for (SearchResp.SearchResult result : results) {
                System.out.printf("{id: %d, score: %f}%n", result.getId(), result.getScore());
            }
        }
    }

    private void dropCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");
    }

    public static void main(String[] args) {
        io.milvus.v2.HybridSearchExample example = new io.milvus.v2.HybridSearchExample();
        example.createCollection();
        example.insertData();
        example.hybridSearch();
        example.dropCollection();

        client.close();
    }
}

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
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TimestampExample {
    private static final MilvusClientV2 client;

    static {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        client = new MilvusClientV2(config);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_timestamp_v2";
    private static final String ID_FIELD = "ID";

    private static final String FLOAT_VECTOR_FIELD = "vector";
    private static final Integer FLOAT_VECTOR_DIM = 128;
    private static final IndexParam.MetricType FLOAT_VECTOR_METRIC = IndexParam.MetricType.COSINE;

    private static final String TIMESTAMP_VECTOR_FIELD = "tsz";


    private static void createCollection() {
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
                .fieldName(TIMESTAMP_VECTOR_FIELD)
                .dataType(DataType.Timestamptz)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(FLOAT_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(FLOAT_VECTOR_METRIC)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(TIMESTAMP_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.STL_SORT)
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

    private static void insertData() {
        int rowCount = 10;
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        System.out.printf("\n================= Insert with timezone: %s =================\n", zone);

        // Insert entities by rows
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(FLOAT_VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));

            LocalDateTime tt = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
            tt = tt.plusDays(i);
            ZonedDateTime zt = tt.atZone(zone);
            String tzFormat = zt.format(formatter);
            System.out.println(tzFormat);
            row.addProperty(TIMESTAMP_VECTOR_FIELD, tzFormat);
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        printRowCount();
    }

    private static void printRowCount() {
        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("\n%d rows persisted", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void query(String timezone) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " <= 3")
                .timezone(timezone)
                .outputFields(Collections.singletonList(TIMESTAMP_VECTOR_FIELD))
                .build());
        System.out.println("\nQuery results:");
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
    }

    private static void search(String timezone) {
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM))))
                .limit(10)
                .filter(ID_FIELD + " <= 3")
                .timezone(timezone)
                .outputFields(Collections.singletonList(TIMESTAMP_VECTOR_FIELD))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
        System.out.println("\nSearch results:");
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long) result.getId(), result.getScore(), result.getEntity().toString());
            }
        }
    }

    private static void hybridSearch(String timezone) {
        // this is a single-route hybrid search, just demo the timezone paramter
        List<AnnSearchReq> searchRequests = new ArrayList<>();
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName(FLOAT_VECTOR_FIELD)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM))))
                .limit(10)
                .filter(ID_FIELD + " <= 3")
                .timezone(timezone)
                .build());

        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(searchRequests)
                .functionScore(FunctionScore.builder()
                        .addFunction(RRFRanker.builder().k(10).build())
                        .build())
                .limit(10)
                .outFields(Collections.singletonList(TIMESTAMP_VECTOR_FIELD))
                .build();
        SearchResp searchResp = client.hybridSearch(hybridSearchReq);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        System.out.println("\nHybridSearch result:");
        List<SearchResp.SearchResult> results = searchResults.get(0);
        for (SearchResp.SearchResult result : results) {
            System.out.println(result);
        }
    }

    public static void main(String[] args) {
        createCollection();
        insertData();

        List<String> timezones = Arrays.asList("Asia/Shanghai", "America/Havana", "Africa/Bangui", "Australia/Sydney");

        for (String timezone : timezones) {
            System.out.printf("\n================= Query with timezone: %s =================", timezone);
            query(timezone);
            search(timezone);
            hybridSearch(timezone);
        }

        client.close();
    }
}

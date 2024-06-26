package io.milvus.v2;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class IteratorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_iterator_v2";
    private static final String ID_FIELD = "userID";
    private static final String AGE_FIELD = "userAge";
    private static final String VECTOR_FIELD = "userFace";
    private static final Integer VECTOR_DIM = 128;

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(AGE_FIELD)
                .dataType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // insert rows
        long count = 10000;
        List<JsonObject> rowsData = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0L; i < count; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.addProperty(AGE_FIELD, ran.nextInt(99));
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            rowsData.add(row);
        }
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rowsData)
                .build());

        // check row count
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        System.out.printf("Inserted row count: %d\n", queryResults.size());

        // search iterator
        SearchIterator searchIterator = client.searchIterator(SearchIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Lists.newArrayList(AGE_FIELD))
                .batchSize(50L)
                .vectorFieldName(VECTOR_FIELD)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .expr(String.format("%s > 50 && %s < 100", AGE_FIELD, AGE_FIELD))
                .params("{\"range_filter\": 15.0, \"radius\": 20.0}")
                .topK(300)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("Search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.println(String.format("%d search results returned\n", counter));

        // query iterator
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(String.format("%s < 300", ID_FIELD))
                .outputFields(Lists.newArrayList(ID_FIELD, AGE_FIELD))
                .batchSize(50L)
                .offset(5)
                .limit(400)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.println(String.format("%d query results returned", counter));

        client.dropCollection(DropCollectionReq.builder().collectionName(COLLECTION_NAME).build());
    }
}

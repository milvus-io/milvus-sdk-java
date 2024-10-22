package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ConsistencyLevelExample {
    private static final MilvusClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    private static final String COLLECTION_NAME_PREFIX = "java_sdk_example_clevel_v1_";
    private static final Integer VECTOR_DIM = 512;

    private static String createCollection(ConsistencyLevelEnum level) {
        String collectionName = COLLECTION_NAME_PREFIX + level.getName();

        // Drop collection if exists
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build());

        // Quickly create a collection with "id" field and "vector" field
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName("id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName("vector")
                        .withDataType(DataType.FloatVector)
                        .withDimension(VECTOR_DIM)
                        .build()
        );

        // Create the collection with 3 fields
        R<RpcStatus> response = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldTypes(fieldsSchema)
                .withConsistencyLevel(level)
                .build());
        CommonUtils.handleResponseStatus(response);
        System.out.printf("Collection '%s' created\n", collectionName);

        response = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName("vector")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build());
        CommonUtils.handleResponseStatus(response);

        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build());

        return collectionName;
    }

    private static void showCollectionLevel(String collectionName) {
        R<DescribeCollectionResponse> response = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build());
        CommonUtils.handleResponseStatus(response);
        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(response.getData());
        System.out.printf("Default consistency level: %s\n", wrapper.getConsistencyLevel().getName());
    }

    private static int insertData(String collectionName) {
        Gson gson = new Gson();
        int rowCount = 1000;
        for (int i = 0; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            R<MutationResult> response = milvusClient.insert(InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withRows(Collections.singletonList(row))
                    .build());
            CommonUtils.handleResponseStatus(response);
        }

        System.out.printf("%d rows inserted\n", rowCount);
        return rowCount;
    }

    private static List<SearchResultsWrapper.IDScore> search(String collectionName, int topK) {
        R<SearchResults> searchR = milvusClient.search(SearchParam.newBuilder()
                .withCollectionName(collectionName)
                .withVectorFieldName("vector")
                .withFloatVectors(Collections.singletonList(CommonUtils.generateFloatVector(VECTOR_DIM)))
                .withTopK(topK)
                .withMetricType(MetricType.L2)
                .build());
        CommonUtils.handleResponseStatus(searchR);

        SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchR.getData().getResults());
        return resultsWrapper.getIDScore(0);
    }

    private static void testStrongLevel() {
        String collectionName = createCollection(ConsistencyLevelEnum.STRONG);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Strong level, all the entities are visible
        List<SearchResultsWrapper.IDScore> scores = search(collectionName, rowCount);
        if (scores.size() != rowCount) {
            throw new RuntimeException(String.format("All inserted entities should be visible with Strong" +
                    " consistency level, but only %d returned", scores.size()));
        }
        System.out.printf("Strong level is working fine, %d results returned\n", scores.size());
    }

    private static void testSessionLevel() throws ClassNotFoundException, NoSuchMethodException {
        String collectionName = createCollection(ConsistencyLevelEnum.SESSION);
        showCollectionLevel(collectionName);

        ConnectParam connectConfig = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(10) // max idle clients per key
                .maxTotalPerKey(20) // max total(idle + active) clients per key
                .maxTotal(100) // max total clients for all keys
                .maxBlockWaitDuration(Duration.ofSeconds(5L)) // getClient() will wait 5 seconds if no idle client available
                .minEvictableIdleDuration(Duration.ofSeconds(10L)) // if number of idle clients is larger than maxIdlePerKey, redundant idle clients will be evicted after 10 seconds
                .build();
        MilvusClientV1Pool pool = new MilvusClientV1Pool(poolConfig, connectConfig);

        // The same process, different MilvusClient object, insert and search with Session level.
        // The Session level ensure that the newly inserted data instantaneously become searchable.
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(vector));

            // insert by a MilvusClient
            String clientName1 = String.format("client_%d", i%10);
            MilvusClient client1 = pool.getClient(clientName1);
            client1.insert(InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withRows(Collections.singletonList(row))
                    .build());
            pool.returnClient(clientName1, client1); // don't forget to return the client to pool
            System.out.println("insert");

            // search by another MilvusClient, use the just inserted vector to search
            // the returned item is expected to be the just inserted item
            String clientName2 = String.format("client_%d", i%10+1);
            MilvusClient client2 = pool.getClient(clientName2);
            R<SearchResults> searchR = client2.search(SearchParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withVectorFieldName("vector")
                    .withFloatVectors(Collections.singletonList(vector))
                    .withTopK(1)
                    .withMetricType(MetricType.L2)
                    .build());
            pool.returnClient(clientName2, client2); // don't forget to return the client to pool
            SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchR.getData().getResults());
            List<SearchResultsWrapper.IDScore> scores = resultsWrapper.getIDScore(0);
            if (scores.size() != 1) {
                throw new RuntimeException("Search result is empty");
            }
            if (i != scores.get(0).getLongID()) {
                throw new RuntimeException("The just inserted entity is not found");
            }
            System.out.println("search");
        }

        System.out.println("Session level is working fine");
    }

    private static void testBoundedLevel() {
        String collectionName = createCollection(ConsistencyLevelEnum.BOUNDED);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Bounded level, not all the entities are visible
        List<SearchResultsWrapper.IDScore> scores = search(collectionName, rowCount);
        System.out.printf("Bounded level is working fine, %d results returned\n", scores.size());
    }

    private static void testEventuallyLevel() {
        String collectionName = createCollection(ConsistencyLevelEnum.EVENTUALLY);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Bounded level, not all the entities are visible
        List<SearchResultsWrapper.IDScore> scores = search(collectionName, rowCount);
        System.out.printf("Eventually level is working fine, %d results returned\n", scores.size());
    }

    public static void main(String[] args) throws Exception {
        testStrongLevel();
        System.out.println("==============================================================");
        testSessionLevel();
        System.out.println("==============================================================");
        testBoundedLevel();
        System.out.println("==============================================================");
        testEventuallyLevel();
    }
}

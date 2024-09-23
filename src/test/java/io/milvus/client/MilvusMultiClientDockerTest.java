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

package io.milvus.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.response.*;
import org.apache.commons.text.RandomStringGenerator;
import org.codehaus.plexus.util.FileUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class MilvusMultiClientDockerTest {
    private static MilvusClient client;
    private static RandomStringGenerator generator;
    private static final int dimension = 128;
    private static final Boolean useDockerCompose = Boolean.TRUE;

    private static void startDockerContainer() {
        if (!useDockerCompose) {
            return;
        }

        // start the test container
        Runtime runtime = Runtime.getRuntime();
        String bashCommand = "docker compose up -d";

        try {
            System.out.println(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                System.out.println("Failed to start docker compose, status " + status);
            }

            System.out.println("Milvus service started");
        } catch (Throwable t) {
            System.out.println("Failed to execute docker compose up: " + t.getMessage());
        }

        ConnectParam connectParam = connectParamBuilder().withAuthorization("root", "Milvus").build();
        MilvusServiceClient tempClient = new MilvusServiceClient(connectParam);
        long waitTime = 0;
        while (true) {
            // although milvus container is alive, it is still in initializing,
            // connection will failed and get error "proxy not health".
            // check health state for every few seconds, until the server is ready.
            long checkInterval = 3;
            try {
                TimeUnit.SECONDS.sleep(checkInterval);
            } catch (InterruptedException t) {
                System.out.println("Interrupted: " + t.getMessage());
                break;
            }

            try{
                R<CheckHealthResponse> resp = tempClient.checkHealth();
                if (resp.getData().getIsHealthy()) {
                    System.out.println(String.format("Milvus service is ready after %d seconds", waitTime));
                    break;
                }
                System.out.println("Milvus service is not ready, waiting...");
            } catch (Throwable t) {
                System.out.println("Milvus service is in initialize, not able to connect: " + t.getMessage());
            }

            waitTime += checkInterval;
            if (waitTime > 120) {
                System.out.println(String.format("Milvus service failed to start within %d seconds", waitTime));
                break;
            }
        }
    }

    private static void stopDockerContainer() {
        if (!useDockerCompose) {
            return;
        }

        // stop all test dockers
        Runtime runtime = Runtime.getRuntime();
        String bashCommand = "docker compose down";

        try {
            System.out.println("Milvus service stopping...");
            TimeUnit.SECONDS.sleep(5);
            System.out.println(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                System.out.println("Failed to stop test docker containers" + pro.getOutputStream().toString());
            }
        } catch (Throwable t) {
            System.out.println("Failed to execute docker compose down: " + t.getMessage());
        }

        // clean up log dir
        runtime = Runtime.getRuntime();
        bashCommand = "docker compose rm";

        try {
            System.out.println(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                System.out.println("Failed to clean up test docker containers" + pro.getOutputStream().toString());
            }

            System.out.println("Clean up volume directory of Docker");
            FileUtils.deleteDirectory("volumes");
        } catch (Throwable t) {
            System.out.println("Failed to remove docker compose volume:" + t.getMessage());
        }
    }

    @BeforeAll
    public static void setUp() {
        startDockerContainer();

        MultiConnectParam connectParam = multiConnectParamBuilder()
                .withAuthorization("root", "Milvus")
                .build();
        client = new MilvusMultiServiceClient(connectParam);
//        TimeUnit.SECONDS.sleep(10);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterAll
    public static void tearDown() {
        if (client != null) {
            client.close();
        }

        stopDockerContainer();
    }

    protected static ConnectParam.Builder connectParamBuilder() {
        return connectParamBuilder("localhost", 19530);
    }

    private static ConnectParam.Builder connectParamBuilder(String host, int port) {
        return ConnectParam.newBuilder().withHost(host).withPort(port);
    }

    private static MultiConnectParam.Builder multiConnectParamBuilder() {
        ServerAddress serverAddress = ServerAddress.newBuilder().withHost("localhost").withPort(19530).build();
        ServerAddress serverSlaveAddress = ServerAddress.newBuilder().withHost("localhost").withPort(19531).withHealthPort(9092).build();
        return MultiConnectParam.newBuilder().withHosts(Arrays.asList(serverAddress, serverSlaveAddress));
    }

    private List<List<Float>> generateFloatVectors(int count) {
        Random ran = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            List<Float> vector = new ArrayList<>();
            for (int i = 0; i < dimension; ++i) {
                vector.add(ran.nextFloat());
            }
            vectors.add(vector);
        }

        return vectors;
    }

    private List<List<Float>> normalizeFloatVectors(List<List<Float>> src) {
        for (List<Float> vector : src) {
            double total = 0.0;
            for (Float val : vector) {
                total = total + val * val;
            }
            float squre = (float) Math.sqrt(total);
            for (int i = 0; i < vector.size(); ++i) {
                vector.set(i, vector.get(i) / squre);
            }
        }

        return src;
    }

    private List<ByteBuffer> generateBinaryVectors(int count) {
        Random ran = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        int byteCount = dimension / 8;
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = ByteBuffer.allocate(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
            }
            vectors.add(vector);
        }
        return vectors;

    }

    @Test
    void testFloatVectors() {
        client.setLogLevel(LogLevel.Error);
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "long_field";
        String field2Name = "vec_field";
        String field3Name = "bool_field";
        String field4Name = "double_field";
        String field5Name = "int_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Bool)
                .withName(field3Name)
                .withDescription("gender")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Double)
                .withName(field4Name)
                .withDescription("weight")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Int8)
                .withName(field5Name)
                .withDescription("age")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        // insert data
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        List<Boolean> genders = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        List<Short> ages = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
            genders.add(i % 3 == 0 ? Boolean.TRUE : Boolean.FALSE);
            weights.add(((double) (i + 1) / 100));
            ages.add((short) ((i + 1) % 99));
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field5Name, ages));
        fieldsInsert.add(new InsertParam.Field(field4Name, weights));
        fieldsInsert.add(new InsertParam.Field(field3Name, genders));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertR = client.withTimeout(10, TimeUnit.SECONDS).insert(insertParam);
        assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // get partition statistics
        R<GetPartitionStatisticsResponse> statPartR = client.getPartitionStatistics(GetPartitionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withPartitionName("_default") // each collection has '_default' partition
                .withFlush(true)
                .build());
        assertEquals(R.Status.Success.getCode(), statPartR.getStatus().intValue());

        GetPartStatResponseWrapper statPart = new GetPartStatResponseWrapper(statPartR.getData());
        System.out.println("Partition row count: " + statPart.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":256}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        DescIndexResponseWrapper indexDesc = new DescIndexResponseWrapper(descIndexR.getData());
        System.out.println("Index description: " + indexDesc.toString());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // show collections
        R<ShowCollectionsResponse> showR = client.showCollections(ShowCollectionsParam.newBuilder()
                .addCollectionName(randomCollectionName)
                .build());
        assertEquals(R.Status.Success.getCode(), showR.getStatus().intValue());
        ShowCollResponseWrapper info = new ShowCollResponseWrapper(showR.getData());
        System.out.println("Collection info: " + info.toString());

        // show partitions
        R<ShowPartitionsResponse> showPartR = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addPartitionName("_default") // each collection has a '_default' partition
                .build());
        assertEquals(R.Status.Success.getCode(), showPartR.getStatus().intValue());
        ShowPartResponseWrapper infoPart = new ShowPartResponseWrapper(showPartR.getData());
        System.out.println("Partition info: " + infoPart.toString());

        // query vectors to verify
        List<Long> queryIDs = new ArrayList<>();
        List<Double> compareWeights = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            queryIDs.add(ids.get(i));
            compareWeights.add(weights.get(i));
        }
        String expr = field1Name + " in " + queryIDs.toString();
        List<String> outputFields = Arrays.asList(field1Name, field2Name, field3Name, field4Name, field4Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
            assertEquals(nq, wrapper.getFieldData().size());

            if (fieldName.compareTo(field1Name) == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
                assertEquals(nq, out.size());
                for (Object o : out) {
                    long id = (Long) o;
                    assertTrue(queryIDs.contains(id));
                }
            }
        }

        // Note: the query() return vectors are not in same sequence to the input
        // here we cannot compare vector one by one
        // the boolean also cannot be compared
        if (outputFields.contains(field2Name)) {
            assertTrue(queryResultsWrapper.getFieldWrapper(field2Name).isVectorField());
            List<?> out = queryResultsWrapper.getFieldWrapper(field2Name).getFieldData();
            assertEquals(nq, out.size());
        }

        if (outputFields.contains(field3Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field3Name).getFieldData();
            assertEquals(nq, out.size());
        }

        if (outputFields.contains(field4Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field4Name).getFieldData();
            assertEquals(nq, out.size());
            for (Object o : out) {
                double d = (Double) o;
                assertTrue(compareWeights.contains(d));
            }
        }


        // pick some vectors to search
        List<Long> targetVectorIDs = new ArrayList<>();
        List<List<Float>> targetVectors = new ArrayList<>();
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add(ids.get(i));
            targetVectors.add(vectors.get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .withParams("{\"nprobe\":8}")
                .addOutField(field4Name)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        List<?> fieldData = results.getFieldData(field4Name, 0);
        assertEquals(topK, fieldData.size());
        fieldData = results.getFieldData(field4Name, nq - 1);
        assertEquals(topK, fieldData.size());

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testBinaryVectors() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "field1";
        String field2Name = "field2";
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("hello")
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withDataType(DataType.BinaryVector)
                .withName(field2Name)
                .withDescription("world")
                .withDimension(dimension)
                .build();

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .addFieldType(field1)
                .addFieldType(field2)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
        }
        List<ByteBuffer> vectors = generateBinaryVectors(rowCount);

        List<InsertParam.Field> fields = new ArrayList<>();
        // no need to provide id here since this field is auto_id
        fields.add(new InsertParam.Field(field1Name, ids));
        fields.add(new InsertParam.Field(field2Name, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.BIN_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // pick some vectors to search
        int nq = 5;
        List<Long> targetVectorIDs = new ArrayList<>();
        List<ByteBuffer> targetVectors = new ArrayList<>();
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            targetVectorIDs.add(ids.get(i));
            targetVectors.add(vectors.get(i));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.HAMMING)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testAsyncMethods() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "long_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert async
        List<ListenableFuture<R<MutationResult>>> futureResponses = new ArrayList<>();
        int rowCount = 1000;
        for (long i = 0L; i < 10; ++i) {
            List<List<Float>> vectors = normalizeFloatVectors(generateFloatVectors(rowCount));
            List<InsertParam.Field> fieldsInsert = new ArrayList<>();
            fieldsInsert.add(new InsertParam.Field(field2Name, vectors));

            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(randomCollectionName)
                    .withFields(fieldsInsert)
                    .build();

            ListenableFuture<R<MutationResult>> insertFuture = client.insertAsync(insertParam);
            futureResponses.add(insertFuture);
        }

        // get insert result
        List<Long> queryIDs = new ArrayList<>();
        for (ListenableFuture<R<MutationResult>> response : futureResponses) {
            try {
                R<MutationResult> insertR = response.get();
                assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

                MutationResultWrapper wrapper = new MutationResultWrapper(insertR.getData());
                queryIDs.add(wrapper.getLongIDs().get(0));
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("failed to insert:" + e.getMessage());
                return;
            }
        }

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.IP)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // search async
        List<List<Float>> targetVectors = normalizeFloatVectors(generateFloatVectors(2));
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .build();

        ListenableFuture<R<SearchResults>> searchFuture = client.searchAsync(searchParam);

        // query async
        String expr = field1Name + " in " + queryIDs.toString();
        List<String> outputFields = Arrays.asList(field1Name, field2Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        ListenableFuture<R<QueryResults>> queryFuture = client.queryAsync(queryParam);

        try {
            // get search results
            R<SearchResults> searchR = searchFuture.get();
            assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

            // verify search result
            SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
            System.out.println("Search results:");
            for (int i = 0; i < targetVectors.size(); ++i) {
                List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
                assertEquals(topK, scores.size());
                System.out.println(scores.toString());
            }

            // get query results
            R<QueryResults> queryR = queryFuture.get();
            assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

            // verify query result
            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
            for (String fieldName : outputFields) {
                FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
                System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
                System.out.println(wrapper.getFieldData());
                assertEquals(queryIDs.size(), wrapper.getFieldData().size());
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }
}

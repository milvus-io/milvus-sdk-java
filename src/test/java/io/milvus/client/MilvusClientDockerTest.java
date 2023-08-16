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

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.dml.UpsertParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.GetPartitionStatisticsParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.response.*;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.codehaus.plexus.util.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class MilvusClientDockerTest {
    private static final Logger logger = LogManager.getLogger("MilvusClientTest");
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
        String bashCommand = "docker-compose up -d";
        try {

            logger.info(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to start docker compose, status " + status);
            }

            logger.info("Docker compose is up");
        } catch (Throwable t) {
            logger.error("Failed to execute docker compose up", t);
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
                logger.error("Interrupted", t);
                break;
            }

            try{
                R<CheckHealthResponse> resp = tempClient.checkHealth();
                if (resp.getData().getIsHealthy()) {
                    logger.info(String.format("Milvus service is ready after %d seconds", waitTime));
                    break;
                }
                logger.info("Milvus service is not ready, waiting...");
            } catch (Throwable t) {
                logger.error("Milvus service is in initialize, not able to connect", t);
            }

            waitTime += checkInterval;
            if (waitTime > 120) {
                logger.error(String.format("Milvus service failed to start within %d seconds", waitTime));
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
        String bashCommand = "docker-compose down";

        try {
            logger.info("Milvus service stopping...");
            TimeUnit.SECONDS.sleep(5);
            logger.info(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to stop test docker containers" + pro.getOutputStream().toString());
            }
        } catch (Throwable t) {
            logger.error("Failed to execute docker compose down", t);
        }

        // clean up log dir
        runtime = Runtime.getRuntime();
        bashCommand = "docker-compose rm";

        try {
            logger.info(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to clean up test docker containers" + pro.getOutputStream().toString());
            }

            logger.error("Clean up volume directory of Docker");
            FileUtils.cleanDirectory("volumes");
        } catch (Throwable t) {
            logger.error("Failed to remove docker compose volume", t);
        }
    }

    @BeforeAll
    public static void setUp() {
        startDockerContainer();

        ConnectParam connectParam = connectParamBuilder().withAuthorization("root", "Milvus").build();
        client = new MilvusServiceClient(connectParam);
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
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        R<ShowPartitionsResponse> spResp = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        System.out.println(spResp);

        ShowPartResponseWrapper wra = new ShowPartResponseWrapper(spResp.getData());
        List<ShowPartResponseWrapper.PartitionInfo> parts = wra.getPartitionsInfo();
        System.out.println("Partition num: "+parts.size());


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
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // get partition statistics
        R<GetPartitionStatisticsResponse> statPartR = client.getPartitionStatistics(GetPartitionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withPartitionName("_default") // each collection has '_default' partition
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statPartR.getStatus().intValue());

        GetPartStatResponseWrapper statPart = new GetPartStatResponseWrapper(statPartR.getData());
        System.out.println("Partition row count: " + statPart.getRowCount());

        // create index on scalar field
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field5Name)
                .withIndexType(IndexType.STL_SORT)
                .withSyncMode(Boolean.TRUE)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // create index on vector field
        indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexName("abv")
                .withIndexType(IndexType.HNSW)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"M\":16,\"efConstruction\":64}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        DescIndexResponseWrapper indexDesc = new DescIndexResponseWrapper(descIndexR.getData());
        System.out.println("Index description: " + indexDesc.toString());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // show collections
        R<ShowCollectionsResponse> showR = client.showCollections(ShowCollectionsParam.newBuilder()
                .addCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), showR.getStatus().intValue());
        ShowCollResponseWrapper info = new ShowCollResponseWrapper(showR.getData());
        System.out.println("Collection info: " + info.toString());

        // show partitions
        R<ShowPartitionsResponse> showPartR = client.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .addPartitionName("_default") // each collection has a '_default' partition
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), showPartR.getStatus().intValue());
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
        List<String> outputFields = Arrays.asList(field1Name, field2Name, field3Name, field4Name, field5Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
            Assertions.assertEquals(nq, wrapper.getFieldData().size());

            if (fieldName.compareTo(field1Name) == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
                Assertions.assertEquals(nq, out.size());
                for (Object o : out) {
                    long id = (Long) o;
                    Assertions.assertTrue(queryIDs.contains(id));
                }
            }
        }

        // Note: the query() return vectors are not in same sequence to the input
        // here we cannot compare vector one by one
        // the boolean also cannot be compared
        if (outputFields.contains(field2Name)) {
            Assertions.assertTrue(queryResultsWrapper.getFieldWrapper(field2Name).isVectorField());
            List<?> out = queryResultsWrapper.getFieldWrapper(field2Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(field3Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field3Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
        }

        if (outputFields.contains(field4Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field4Name).getFieldData();
            Assertions.assertEquals(nq, out.size());
            for (Object o : out) {
                double d = (Double) o;
                Assertions.assertTrue(compareWeights.contains(d));
            }
        }

        // query with offset and limit
        int queryLimit = 5;
        expr = field5Name + " > 1"; // "age > 1"
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOffset(100L)
                .withLimit((long) queryLimit)
                .build();
        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        // we didn't set the output fields, only primary key field is returned
        List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
        Assertions.assertEquals(queryLimit, out.size());

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
                .withParams("{\"ef\":64}")
                .addOutField(field4Name)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            Assertions.assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        List<?> fieldData = results.getFieldData(field4Name, 0);
        Assertions.assertEquals(topK, fieldData.size());
        fieldData = results.getFieldData(field4Name, nq - 1);
        Assertions.assertEquals(topK, fieldData.size());

        // release collection
        ReleaseCollectionParam releaseCollectionParam = ReleaseCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> releaseCollectionR = client.releaseCollection(releaseCollectionParam);
        Assertions.assertEquals(R.Status.Success.getCode(), releaseCollectionR.getStatus().intValue());

        // drop index
        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testBinaryVectors() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "field1";
        String field2Name = "field2";
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
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
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data
        int rowCount = 10000;
        List<ByteBuffer> vectors = generateBinaryVectors(rowCount);

        List<InsertParam.Field> fields = new ArrayList<>();
        // no need to provide id here since this field is auto_id
        fields.add(new InsertParam.Field(field2Name, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());
//        System.out.println(insertR.getData());
        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids = insertResultWrapper.getLongIDs();
//        System.out.println("Auto-generated ids: " + ids);

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.BIN_FLAT)
                .withMetricType(MetricType.JACCARD)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR2 = client.createIndex(indexParam2);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR2.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // search with BIN_FLAT index
        List<ByteBuffer> oneVector = new ArrayList<>();
        oneVector.add(vectors.get(0));

        SearchParam searchOneParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.JACCARD)
                .withTopK(5)
                .withVectors(oneVector)
                .withVectorFieldName(field2Name)
                .build();

        R<SearchResults> searchOne = client.search(searchOneParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchOne.getStatus().intValue());

        SearchResultsWrapper oneResult = new SearchResultsWrapper(searchOne.getData().getResults());
        List<SearchResultsWrapper.IDScore> oneScores = oneResult.getIDScore(0);
        System.out.println("The result of " + ids.get(0) + " with SUPERSTRUCTURE metric:");
        System.out.println(oneScores);

        // release collection
        ReleaseCollectionParam releaseCollectionParam = ReleaseCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> releaseCollectionR = client.releaseCollection(releaseCollectionParam);
        Assertions.assertEquals(R.Status.Success.getCode(), releaseCollectionR.getStatus().intValue());

        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName).build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexName("abv")
                .withIndexType(IndexType.BIN_IVF_FLAT)
                .withMetricType(MetricType.HAMMING)
                .withExtraParam("{\"nlist\":64}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR2 = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR2.getStatus().intValue());

        // pick some vectors to search with index
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
                .withParams("{\"nprobe\":8}")
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            Assertions.assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
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
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

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
                Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

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
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

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
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

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
            Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

            // verify search result
            SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
            System.out.println("Search results:");
            for (int i = 0; i < targetVectors.size(); ++i) {
                List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
                Assertions.assertEquals(topK, scores.size());
                System.out.println(scores.toString());
            }

            // get query results
            R<QueryResults> queryR = queryFuture.get();
            Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

            // verify query result
            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
            for (String fieldName : outputFields) {
                FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
                System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
                System.out.println(wrapper.getFieldData());
                Assertions.assertEquals(queryIDs.size(), wrapper.getFieldData().size());
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    // this case can be executed when the milvus image of version 2.1 is published.
    @Test
    void testCredential() {
        String randomCollectionName = generator.generate(10);
        // collection schema
        String field1Name = "long_field";
        String field2Name = "vec_field";
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

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.IVF_FLAT)
                .withIndexName("xxx")
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":256}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        client.getIndexState(GetIndexStateParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());

        R<RpcStatus> dropIndexR = client.dropIndex(DropIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testStringField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "str_id";
        String field2Name = "vec_field";
        String field3Name = "str_field";
        String field4Name = "int_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.VarChar)
                .withName(field1Name)
                .withMaxLength(32)
                .withDescription("string identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(dimension)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName(field3Name)
                .withMaxLength(32)
                .withDescription("comment")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.Int64)
                .withName(field4Name)
                .withDescription("sequence")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        // insert data
        int rowCount = 10000;
        List<String> ids = new ArrayList<>();
        List<String> comments = new ArrayList<>();
        List<Long> sequences = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(generator.generate(8));
            comments.add(generator.generate(8));
            sequences.add(i);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field3Name, comments));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));
        fieldsInsert.add(new InsertParam.Field(field4Name, sequences));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertR = client.withTimeout(10, TimeUnit.SECONDS).insert(insertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

        MutationResultWrapper insertResultWrapper = new MutationResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field3Name)
                .withIndexName("stridx")
                .withIndexType(IndexType.TRIE)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // get index description
        DescribeIndexParam descIndexParam = DescribeIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<DescribeIndexResponse> descIndexR = client.describeIndex(descIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), descIndexR.getStatus().intValue());

        // create index
        CreateIndexParam indexParam2 = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.IP)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR2 = client.createIndex(indexParam2);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR2.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // query vectors to verify
        List<Long> queryItems = new ArrayList<>();
        List<String> queryIds = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        int randomIndex = ran.nextInt(rowCount - nq);
        for (int i = randomIndex; i < randomIndex + nq; ++i) {
            queryIds.add(ids.get(i));
            queryItems.add(sequences.get(i));
        }
        String expr = field4Name + " in " + queryItems.toString();
        List<String> outputFields = Arrays.asList(field1Name, field3Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
            Assertions.assertEquals(nq, wrapper.getFieldData().size());

            if (fieldName.compareTo(field1Name) == 0) {
                List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
                Assertions.assertEquals(nq, out.size());
                for (Object o : out) {
                    String id = (String) o;
                    Assertions.assertTrue(queryIds.contains(id));
                }
            }
        }

        // search
        int topK = 5;
        List<List<Float>> targetVectors = new ArrayList<>();
        for (Long seq : queryItems) {
            targetVectors.add(vectors.get(seq.intValue()));
        }
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .addOutField(field4Name)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + queryIds.get(i) + "):");
            System.out.println(scores);
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    private static void testIndex(String collectionName, String fieldName,
                                  IndexType type, MetricType metric,
                                  String params, Boolean syncMode) {
        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(fieldName)
                .withIndexName("index")
                .withIndexType(type)
                .withMetricType(metric)
                .withExtraParam(params)
                .withSyncMode(syncMode)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // drop index
        DropIndexParam dropIndexParam = DropIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withIndexName(indexParam.getIndexName())
                .build();
        R<RpcStatus> dropIndexR = client.dropIndex(dropIndexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), dropIndexR.getStatus().intValue());
    }

    @Test
    void testFloatVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "idg_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDimension(dimension)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        Map<IndexType, String> indexTypes = new HashMap<>();
        indexTypes.put(IndexType.FLAT, "{}");
        indexTypes.put(IndexType.IVF_FLAT, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_SQ8, "{\"nlist\":128}");
        indexTypes.put(IndexType.IVF_PQ, "{\"nlist\":128, \"m\":16, \"nbits\":8}");
        indexTypes.put(IndexType.HNSW, "{\"M\":16,\"efConstruction\":64}");

        List<MetricType> metricTypes = new ArrayList<>();
        metricTypes.add(MetricType.L2);
        metricTypes.add(MetricType.IP);

        for (IndexType type : indexTypes.keySet()) {
            for (MetricType metric : metricTypes) {
                testIndex(randomCollectionName, field2Name, type, metric, indexTypes.get(type), Boolean.TRUE);
                testIndex(randomCollectionName, field2Name, type, metric, indexTypes.get(type), Boolean.FALSE);
            }
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testBinaryVectorIndex() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "id_field";
        String field2Name = "vector_field";
        FieldType field1 = FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withDataType(DataType.BinaryVector)
                .withName(field2Name)
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
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // test all supported indexes
        List<MetricType> flatMetricTypes = new ArrayList<>();
        flatMetricTypes.add(MetricType.HAMMING);
        flatMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : flatMetricTypes) {
            testIndex(randomCollectionName, field2Name, IndexType.BIN_FLAT, metric, "{}", Boolean.TRUE);
            testIndex(randomCollectionName, field2Name, IndexType.BIN_FLAT, metric, "{}", Boolean.FALSE);
        }

        List<MetricType> ivfMetricTypes = new ArrayList<>();
        ivfMetricTypes.add(MetricType.HAMMING);
        ivfMetricTypes.add(MetricType.JACCARD);

        for (MetricType metric : ivfMetricTypes) {
            testIndex(randomCollectionName, field2Name, IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.TRUE);
            testIndex(randomCollectionName, field2Name, IndexType.BIN_IVF_FLAT, metric, "{\"nlist\":128}", Boolean.FALSE);
        }

        client.dropCollection(DropCollectionParam.newBuilder().withCollectionName(randomCollectionName).build());
    }

    @Test
    void testDynamicField() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "id_field";
        String field2Name = "vec_field";
        String field3Name = "json_field";
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
                .withDataType(DataType.JSON)
                .withName(field3Name)
                .withDescription("info")
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .withEnableDynamicField(true)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        R<DescribeCollectionResponse> response = client.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());

        DescCollResponseWrapper desc = new DescCollResponseWrapper(response.getData());
        System.out.println(desc.toString());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexName("abv")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        int rowCount = 10;
        // insert data by row-based
        List<JSONObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JSONObject row = new JSONObject();
            row.put(field1Name, i);
            row.put(field2Name, generateFloatVectors(1).get(0));

            // JSON field
            JSONObject info = new JSONObject();
            info.put("row-based-info", i);
            row.put(field3Name, info);

            // extra meta is automatically stored in dynamic field
            row.put("extra_meta", i % 3 == 0);
            row.put(generator.generate(5), 100);

            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // insert data by column-based
        List<Long> ids = new ArrayList<>();
        List<JSONObject> infos = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(rowCount + i);
            JSONObject obj = new JSONObject();
            obj.put("column-based-info", i);
            obj.put(generator.generate(5), i);
            infos.add(obj);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, ids));
        fieldsInsert.add(new InsertParam.Field(field2Name, vectors));
        fieldsInsert.add(new InsertParam.Field(field3Name, infos));

        InsertParam insertColumnsParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertColumnResp = client.insert(insertColumnsParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertColumnResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // retrieve rows
        String expr = "extra_meta == true";
        List<String> outputFields = Arrays.asList(field3Name, "extra_meta");
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object extraMeta = record.get("extra_meta");
            System.out.println("'extra_meta' is from dynamic field, value: " + extraMeta);
        }

        // search
        List<List<Float>> targetVectors = generateFloatVectors(2);
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .withParams("{}")
                .withOutFields(outputFields)
                .build();

        R<SearchResults> searchR = client.search(searchParam);
        Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
            System.out.println("The result of No." + i + " target vector:");
            for (SearchResultsWrapper.IDScore score:scores) {
                System.out.println(score);
                Object extraMeta = score.get("extra_meta");
                if (extraMeta != null) {
                    System.out.println("'extra_meta' is from dynamic field, value: " + extraMeta);
                }
            }
        }

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testUpsert() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "id_field";
        String field2Name = "vec_field";
        String field3Name = "varchar_field";
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
                .withDataType(DataType.VarChar)
                .withName(field3Name)
                .withDescription("name")
                .withMaxLength(100)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data by row-based with id from 0 ~ 9
        int rowCount = 10;
        List<JSONObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JSONObject row = new JSONObject();
            row.put(field1Name, i);
            row.put(field2Name, generateFloatVectors(1).get(0));
            row.put(field3Name, String.format("name_%d", i));
            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // get collection statistics with flush, the 10 rows are flushed to a sealed segment
        // wait 2 seconds, ensure the data node consumes the data
        TimeUnit.SECONDS.sleep(2);
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexName("abv")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // retrieve one row from the sealed segment
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("%s == 5", field1Name))
                .addOutField(field3Name)
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in sealed segment:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object name = record.get(field3Name);
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_5", name);
        }

        // insert 10 rows into growing segment with id from 10 ~ 19
        // since the ids are not exist, the upsert call is equal to an insert call
        rows.clear();
        for (long i = 0L; i < rowCount; ++i) {
            JSONObject row = new JSONObject();
            row.put(field1Name, rowCount + i);
            row.put(field2Name, generateFloatVectors(1).get(0));
            row.put(field3Name, String.format("name_%d", rowCount + i));
            rows.add(row);
        }

        UpsertParam upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // retrieve one row from the growing segment
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("%s == 18", field1Name))
                .addOutField(field3Name)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in growing segment:");
        for (QueryResultsWrapper.RowRecord record:records) {
            System.out.println(record);
            Object name = record.get(field3Name);
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_18", name);
        }

        // upsert to change the no.5 and no.18 items
        rows.clear();
        JSONObject row = new JSONObject();
        row.put(field1Name, 5L);
        row.put(field2Name, generateFloatVectors(1).get(0));
        row.put(field3Name, "updated_5");
        rows.add(row);
        row = new JSONObject();
        row.put(field1Name, 18L);
        row.put(field2Name, generateFloatVectors(1).get(0));
        row.put(field3Name, "updated_18");
        rows.add(row);

        upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());

        // verify the two items
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(String.format("%s == 5 || %s == 18", field1Name, field1Name))
                .addOutField(field3Name)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("updated_5", records.get(0).get(field3Name));
        Assertions.assertEquals("updated_18", records.get(1).get(field3Name));

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }
}

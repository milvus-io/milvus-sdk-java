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

import io.milvus.Response.*;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;

import io.milvus.param.index.CreateIndexParam;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MilvusClientDockerTest {
    private static final Logger logger = LogManager.getLogger("MilvusClientTest");
    private static MilvusClient client;
    private static RandomStringGenerator generator;
    private static final int dimension = 128;
    private static final Boolean useDockerCompose = Boolean.FALSE;

    private static void startDockerContainer() {
        if (!useDockerCompose) {
            return;
        }

        // start the test container
        Runtime runtime = Runtime.getRuntime();
        String bashCommand = "docker-compose up -d";

        try {
            logger.debug(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to start docker compose, status " + status);
            }
        } catch (Throwable t) {
            logger.error("Failed to execute docker compose up", t);
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
            logger.debug(bashCommand);
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
            logger.debug(bashCommand);
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to clean up test docker containers" + pro.getOutputStream().toString());
            }

            logger.error("Clean up volume directory of Docker");
            FileUtils.deleteDirectory("volumes");
        } catch (Throwable t) {
            logger.error("Failed to remove docker compose volume", t);
        }
    }

    @BeforeClass
    public static void setUp() {
        startDockerContainer();

        ConnectParam connectParam = connectParamBuilder().build();
        client = new MilvusServiceClient(connectParam);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterClass
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

    private List<ByteBuffer> generateBinaryVectors(int count) {
        Random ran = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        int byteCount = dimension/8;
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = ByteBuffer.allocate(byteCount);
            for (int i = 0; i < byteCount; ++i) {
                vector.put((byte)ran.nextInt(Byte.MAX_VALUE));
            }
            vectors.add(vector);
        }
        return vectors;

    }

    @Test
    public void testFloatVectors() {
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
                .withDataType(DataType.Int32)
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
        assertEquals(createR.getStatus().intValue(), R.Status.Success.getCode());

        // insert data
        int rowCount = 10000;
        List<Long> ids = new ArrayList<>();
        List<Boolean> genders = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        List<Integer> ages = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
            genders.add(i%3 == 0 ? Boolean.TRUE : Boolean.FALSE);
            weights.add((double) (i / 100));
            ages.add((int)i%99);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(field1Name, DataType.Int64, ids));
        fieldsInsert.add(new InsertParam.Field(field2Name, DataType.FloatVector, vectors));
        fieldsInsert.add(new InsertParam.Field(field3Name, DataType.Bool, genders));
        fieldsInsert.add(new InsertParam.Field(field4Name, DataType.Double, weights));
        fieldsInsert.add(new InsertParam.Field(field5Name, DataType.Int8, ages));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fieldsInsert)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        assertEquals(insertR.getStatus().intValue(), R.Status.Success.getCode());
//        System.out.println(insertR.getData());

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(statR.getStatus().intValue(), R.Status.Success.getCode());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.GetRowCount());

        // create index
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.IVF_FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"nlist\":64}")
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(param);
        assertEquals(createIndexR.getStatus().intValue(), R.Status.Success.getCode());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(loadR.getStatus().intValue(), R.Status.Success.getCode());

        // query vectors to verify
        List<Long> queryIDs = new ArrayList<>();
        List<Boolean> compareGenders = new ArrayList<>();
        List<Double> compareWeights = new ArrayList<>();
        int nq = 5;
        Random ran = new Random();
        for (int i = 0; i < nq; ++i) {
            int randomIndex = ran.nextInt(rowCount);
            queryIDs.add(ids.get(randomIndex));
            compareGenders.add(genders.get(randomIndex));
            compareWeights.add(weights.get(randomIndex));
        }
        String expr = field1Name + " in " + queryIDs.toString();
        List<String> outputFields = Arrays.asList(field1Name, field2Name, field3Name, field4Name, field4Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        R<QueryResults> queryR= client.query(queryParam);
        assertEquals(queryR.getStatus().intValue(), R.Status.Success.getCode());

        // verify query result
        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        for (String fieldName : outputFields) {
            FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
            System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
            System.out.println(wrapper.getFieldData());
        }

        if (outputFields.contains(field1Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field1Name).getFieldData();
            assertEquals(out.size(), nq);
            for (Object o : out) {
                long id = (Long) o;
                assertTrue(queryIDs.contains(id));
            }
        }

        // Note: the query() return vectors are not in same sequence to the input
        // here we cannot compare vector one by one
        if (outputFields.contains(field2Name)) {
            assertTrue(queryResultsWrapper.getFieldWrapper(field2Name).isVectorField());
            List<?> out = queryResultsWrapper.getFieldWrapper(field2Name).getFieldData();
            assertEquals(out.size(), nq);
        }

        if (outputFields.contains(field3Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field3Name).getFieldData();
            assertEquals(out.size(), nq);
            for (Object o : out) {
                boolean b = (Boolean)o;
                assertTrue(compareGenders.contains(b));
            }
        }

        if (outputFields.contains(field4Name)) {
            List<?> out = queryResultsWrapper.getFieldWrapper(field4Name).getFieldData();
            assertEquals(out.size(), nq);
            for (Object o : out) {
                double d = (Double)o;
                assertTrue(compareWeights.contains(d));
            }
        }


        // pick some vectors to search
        List<Long> targetVectorIDs = new ArrayList<>();
        List<List<Float>> targetVectors = new ArrayList<>();
        for (int i = 0; i < nq; ++i) {
            int randomIndex = ran.nextInt(rowCount);
            targetVectorIDs.add(ids.get(randomIndex));
            targetVectors.add(vectors.get(randomIndex));
        }

        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .withParams("{\"nprobe\":8}")
                .build();

        R<SearchResults> searchR = client.search(searchParam);
//        System.out.println(searchR);
        assertEquals(searchR.getStatus().intValue(), R.Status.Success.getCode());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.GetIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(dropR.getStatus().intValue(), R.Status.Success.getCode());
    }

    @Test
    public void testBinaryVectors() {
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
        assertEquals(createR.getStatus().intValue(), R.Status.Success.getCode());

        // insert data
        int rowCount = 10000;
        List<ByteBuffer> vectors = generateBinaryVectors(rowCount);

        List<InsertParam.Field> fields = new ArrayList<>();
        // no need to provide id here since this field is auto_id
        fields.add(new InsertParam.Field(field2Name, DataType.BinaryVector, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        assertEquals(insertR.getStatus().intValue(), R.Status.Success.getCode());
//        System.out.println(insertR.getData());
        InsertResultWrapper insertResultWrapper = new InsertResultWrapper(insertR.getData());
        System.out.println(insertResultWrapper.getInsertCount() + " rows inserted");
        List<Long> ids = insertResultWrapper.getLongIDs();
//        System.out.println("Auto-generated ids: " + ids);

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(statR.getStatus().intValue(), R.Status.Success.getCode());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.GetRowCount());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(loadR.getStatus().intValue(), R.Status.Success.getCode());

        // pick some vectors to search
        int nq = 5;
        List<Long> targetVectorIDs = new ArrayList<>();
        List<ByteBuffer> targetVectors = new ArrayList<>();
        Random ran = new Random();
        for (int i = 0; i < nq; ++i) {
            int randomIndex = ran.nextInt(rowCount);
            targetVectorIDs.add(ids.get(randomIndex));
            targetVectors.add(vectors.get(randomIndex));
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
        assertEquals(searchR.getStatus().intValue(), R.Status.Success.getCode());

        // verify the search result
        SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
        for (int i = 0; i < targetVectors.size(); ++i) {
            List<SearchResultsWrapper.IDScore> scores = results.GetIDScore(i);
            System.out.println("The result of No." + i + " target vector(ID = " + targetVectorIDs.get(i) + "):");
            System.out.println(scores);
            assertEquals(targetVectorIDs.get(i).longValue(), scores.get(0).getLongID());
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(dropR.getStatus().intValue(), R.Status.Success.getCode());
    }
}

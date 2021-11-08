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

import io.milvus.Response.GetCollStatResponseWrapper;
import io.milvus.Response.SearchResultsWrapper;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

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

    private List<List<Float>> generateFloatVectors(int nq) {
        Random ran = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < nq; ++n) {
            List<Float> vector = new ArrayList<>();
            for (int i = 0; i < dimension; ++i) {
                vector.add(ran.nextFloat());
            }
            vectors.add(vector);
        }

        return vectors;
    }

    @Test
    public void testMainWorkflow() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "field1";
        String field2Name = "field2";
        FieldType field1 = FieldType.newBuilder()
                .withFieldID(0L)
                .withPrimaryKey(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("hello")
                .build();

        FieldType field2 = FieldType.newBuilder()
                .withFieldID(1L)
                .withDataType(DataType.FloatVector)
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
        List<Long> ids = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            ids.add(i);
        }
        List<List<Float>> vectors = generateFloatVectors(rowCount);

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(field1Name, DataType.Int64, ids));
        fields.add(new InsertParam.Field(field2Name, DataType.FloatVector, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
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

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(loadR.getStatus().intValue(), R.Status.Success.getCode());

        // pick some vectors to search
        int nq = 5;
        List<Long> targetVectorIDs = new ArrayList<>();
        List<List<Float>> targetVectors = new ArrayList<>();
        Random ran = new Random();
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

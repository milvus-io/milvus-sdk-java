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

import io.milvus.grpc.DataType;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class MilvusClientDockerTest {
    private static final Logger logger = LogManager.getLogger("MilvusClientTest");
    private static MilvusClient client;
    private static RandomStringGenerator generator;
    private int dimension = 128;
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
            logger.error("Failed to remove docker com", t);
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
        return ConnectParam.Builder.newBuilder().withHost(host).withPort(port);
    }

    private static void checkR(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            logger.error("Error code: {}" + r.getMessage(), r.getStatus());
        }
    }

    @Test
    public void testMainWorkflow() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "field1";
        String field2Name = "field2";
        FieldType[] fieldTypes = new FieldType[2];
        fieldTypes[0] = FieldType.Builder.newBuilder().withFieldID(0l)
                .withPrimaryKey(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("hello")
                .build();

        fieldTypes[1] = FieldType.Builder.newBuilder().withFieldID(1l)
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("world")
                .withDimension(dimension)
                .build();

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.Builder.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldTypes)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        assertEquals(createR.getStatus().intValue(), R.Status.Success.getCode());

        // insert data
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        Random ran=new Random();
        for (Long i = 0L; i < 10000; ++i) {
            ids.add(i + 100L);
            List<Float> vector = new ArrayList<>();
            for (int d = 0; d < dimension; ++d) {
                vector.add(ran.nextFloat());
            }
            vectors.add(vector);
        }

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(field1Name, DataType.Int64, ids));
        fields.add(new InsertParam.Field(field2Name, DataType.FloatVector, vectors));

        InsertParam insertParam = InsertParam.Builder
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFields(fields)
                .build();

        R<MutationResult> insertR = client.insert(insertParam);
        assertEquals(insertR.getStatus().intValue(), R.Status.Success.getCode());

        // flush
        R<FlushResponse> flushR = client.flush(randomCollectionName);
        assertEquals(flushR.getStatus().intValue(), R.Status.Success.getCode());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.Builder
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(loadR.getStatus().intValue(), R.Status.Success.getCode());

        // search
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < 2; ++i) {
            vector.add(ran.nextFloat());
        }

        List<String> outFields = Collections.singletonList(field1Name);
        SearchParam searchParam = SearchParam.Builder.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.L2)
                .withOutFields(outFields)
                .withTopK(5)
                .withVectors(Collections.singletonList(vector))
                .withVectorFieldName(field2Name)
                .build();


        R<SearchResults> searchR = client.search(searchParam);
        assertEquals(searchR.getStatus().intValue(), R.Status.Success.getCode());

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.Builder.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> deleteR = client.dropCollection(dropParam);
        assertEquals(deleteR.getStatus().intValue(), R.Status.Success.getCode());
    }
}

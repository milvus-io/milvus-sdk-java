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
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import org.codehaus.plexus.util.FileUtils;
import org.junit.*;

import static org.junit.Assert.assertEquals;

public class MilvusClientDockerTest {
    private static final Logger logger = LogManager.getLogger("MilvusClientTest");
    private static MilvusClient client;
    private static RandomStringGenerator generator;
    private int dimension = 128;

    @BeforeClass
    public static void setUp()  {
        // start the test container
        Runtime runtime = Runtime.getRuntime();
        String bashCommand = "docker-compose up -d";
        try {
            Process pro = runtime.exec(bashCommand);
            int status = pro.waitFor();
            if (status != 0) {
                logger.error("Failed to start docker compose, status " + status);
            }

        } catch (Throwable t) {
            logger.error("Failed to execute docker compose up", t);
        }

        ConnectParam connectParam = connectParamBuilder().build();
        client = new MilvusServiceClient(connectParam);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterClass
    public static  void tearDown() {
        if (client != null) {
            client.close();
        }

        // stop all test dockers
        Runtime runtime = Runtime.getRuntime();
        String bashCommand = "docker-compose down";
        try {
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

    protected static ConnectParam.Builder connectParamBuilder() {;
        return connectParamBuilder("localhost",19530);
    }


    private static ConnectParam.Builder connectParamBuilder(String host, int port) {
        return ConnectParam.Builder.newBuilder().withHost(host).withPort(port);
    }

    @Test
    public void testCreateCollection() {
        String randomCollectionName = generator.generate(10);;
        FieldType[] fieldTypes = new FieldType[2];
        fieldTypes[0] = FieldType.Builder.newBuilder().withFieldID(0l)
                .withPrimaryKey(true)
                .withDataType(DataType.Int64)
                .withName("test2")
                .withDescription("hello")
                .build();

        Map<String, String> typeParams = new HashMap<>();
        typeParams.put("dim", String.valueOf(dimension));
        fieldTypes[1] = FieldType.Builder.newBuilder().withFieldID(1l)
                .withDataType(DataType.BinaryVector)
                .withName("test1")
                .withDescription("world")
                .withTypeParams(typeParams)
                .build();

        CreateCollectionParam param = CreateCollectionParam.Builder.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldTypes)
                .build();
        R<RpcStatus> createR =  client.createCollection(param);
        assertEquals(createR.getStatus().intValue(), R.Status.Success.getCode());

        DropCollectionParam deleteparam = DropCollectionParam.Builder.newBuilder().withCollectionName(randomCollectionName).build();
        R<RpcStatus> deleteR = client.dropCollection(deleteparam);
        assertEquals(deleteR.getStatus().intValue(), R.Status.Success.getCode());
    }

}

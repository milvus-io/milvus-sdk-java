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

package io.milvus.support.v1;


import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.support.TestUtils;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusMultiServiceClient;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public abstract class MilvusMultiDockerTestBase {
    protected static MilvusClient client;
    protected static RandomStringGenerator generator;
    protected static final int DIMENSION = 128;
    protected static final TestUtils utils = new TestUtils(DIMENSION);
    protected static final File DockerComposeFile = TestUtils.dockerComposeFile("docker-compose-multi.yml");
    protected static final File DockerComposeVolumeDirectory = new File("target/milvus-compose-multi");
    protected static final List<String> DockerComposeContainerNames = Arrays.asList("milvus-javasdk-standalone-1", "milvus-javasdk-standalone-2");
    // All split system-test classes in one JVM share a single Milvus standalone.
    // TestUtils.startMilvusStandalone is idempotent per compose file and the
    // stack is torn down once at JVM exit by a shutdown hook, so the server is
    // not restarted for every test class.
    @BeforeAll
    public static void setUp() {
        TestUtils.startMilvusStandalone(DockerComposeFile, DockerComposeVolumeDirectory, DockerComposeContainerNames);

        MultiConnectParam connectParam = multiConnectParamBuilder()
                .withAuthorization("root", "Milvus")
                .build();
        client = new MilvusMultiServiceClient(connectParam);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterAll
    public static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    protected static MultiConnectParam.Builder multiConnectParamBuilder() {
        ServerAddress serverAddress = ServerAddress.newBuilder().withHost("localhost").withPort(29530).build();
        ServerAddress serverSlaveAddress = ServerAddress.newBuilder().withHost("localhost").withPort(29531).withHealthPort(19092).build();
        return MultiConnectParam.newBuilder().withHosts(Arrays.asList(serverAddress, serverSlaveAddress));
    }

}

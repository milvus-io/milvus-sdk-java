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

package io.milvus.unit.v1.response;

import io.milvus.grpc.DescribeDatabaseResponse;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.Constant;
import io.milvus.response.DescDBResponseWrapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescDBResponseWrapperTest {

    @Test
    void constructorRejectsNullResponse() {
        assertThrows(IllegalArgumentException.class, () -> new DescDBResponseWrapper(null));
    }

    @Test
    void testWrapDescribeDatabaseResponse() {
        DescribeDatabaseResponse response = DescribeDatabaseResponse.newBuilder()
                .setDbName("db1")
                .addProperties(KeyValuePair.newBuilder()
                        .setKey(Constant.DATABASE_RESOURCE_GROUPS).setValue("rg1,rg2").build())
                .addProperties(KeyValuePair.newBuilder()
                        .setKey(Constant.DATABASE_REPLICA_NUMBER).setValue("3").build())
                .build();

        DescDBResponseWrapper wrapper = new DescDBResponseWrapper(response);
        assertEquals("db1", wrapper.getDatabaseName());

        Map<String, String> properties = wrapper.getProperties();
        assertEquals(2, properties.size());
        assertEquals("rg1,rg2", properties.get(Constant.DATABASE_RESOURCE_GROUPS));
        assertEquals("3", properties.get(Constant.DATABASE_REPLICA_NUMBER));

        assertEquals(2, wrapper.getResourceGroups().size());
        assertEquals("rg1", wrapper.getResourceGroups().get(0));
        assertEquals("rg2", wrapper.getResourceGroups().get(1));
        assertEquals(3, wrapper.getReplicaNumber());

        assertFalse(wrapper.toString().isEmpty());
    }

    @Test
    void testDefaultsWhenPropertiesMissing() {
        DescribeDatabaseResponse response = DescribeDatabaseResponse.newBuilder().build();
        DescDBResponseWrapper wrapper = new DescDBResponseWrapper(response);
        assertEquals("", wrapper.getDatabaseName());
        assertTrue(wrapper.getResourceGroups().isEmpty());
        assertEquals(0, wrapper.getReplicaNumber());
    }

    @Test
    void testInvalidReplicaNumberThrows() {
        DescribeDatabaseResponse response = DescribeDatabaseResponse.newBuilder()
                .addProperties(KeyValuePair.newBuilder()
                        .setKey(Constant.DATABASE_REPLICA_NUMBER).setValue("not-a-number").build())
                .build();
        DescDBResponseWrapper wrapper = new DescDBResponseWrapper(response);
        assertThrows(NumberFormatException.class, wrapper::getReplicaNumber);
    }
}

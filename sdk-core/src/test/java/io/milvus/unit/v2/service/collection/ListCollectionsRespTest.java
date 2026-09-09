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

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.service.collection.CollectionInfo;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListCollectionsRespTest {

    private CollectionInfo buildCollectionInfo(String name) {
        return CollectionInfo.builder()
                .collectionName(name)
                .build();
    }

    @Test
    void builderBuildsWithAllFields() {
        List<String> names = Arrays.asList("coll1", "coll2");
        List<CollectionInfo> infos = Arrays.asList(
                buildCollectionInfo("coll1"),
                buildCollectionInfo("coll2"));
        ListCollectionsResp response = ListCollectionsResp.builder()
                .collectionNames(names)
                .collectionInfos(infos)
                .build();

        assertEquals(names, response.getCollectionNames());
        assertEquals(infos, response.getCollectionInfos());
    }

    @Test
    void settersUpdateGetters() {
        ListCollectionsResp response = ListCollectionsResp.builder().build();

        List<String> names = Arrays.asList("a", "b");
        response.setCollectionNames(names);
        assertEquals(names, response.getCollectionNames());

        List<CollectionInfo> infos = Collections.singletonList(buildCollectionInfo("a"));
        response.setCollectionInfos(infos);
        assertEquals(infos, response.getCollectionInfos());
    }

    @Test
    void defaultsToEmptyLists() {
        ListCollectionsResp response = ListCollectionsResp.builder().build();
        assertNotNull(response.getCollectionNames());
        assertNotNull(response.getCollectionInfos());
        assertTrue(response.getCollectionNames().isEmpty());
        assertTrue(response.getCollectionInfos().isEmpty());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(ListCollectionsResp.builder());
    }

    @Test
    void toStringContainsFields() {
        ListCollectionsResp response = ListCollectionsResp.builder()
                .collectionNames(Arrays.asList("coll1", "coll2"))
                .build();
        assertTrue(response.toString().contains("coll1"));
    }
}

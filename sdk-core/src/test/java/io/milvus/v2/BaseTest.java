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

package io.milvus.v2;

import io.milvus.grpc.*;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BaseTest {
    @InjectMocks
    public MilvusClientV2 client_v2 = new MilvusClientV2(null);;
    @Mock
    protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;

    @BeforeEach
    public void setUp() {
        client_v2.setBlockingStub(blockingStub);

        Status successStatus = Status.newBuilder().setCode(0).build();
        BoolResponse trueResponse = BoolResponse.newBuilder().setStatus(successStatus).setValue(Boolean.TRUE).build();
        BoolResponse falseResponse = BoolResponse.newBuilder().setStatus(successStatus).setValue(Boolean.FALSE).build();

        CollectionSchema collectionSchema = CollectionSchema.newBuilder()
                .setDescription("test")
                .addFields(FieldSchema.newBuilder()
                        .setName("id")
                        .setDataType(DataType.Int64)
                        .setIsPrimaryKey(Boolean.TRUE)
                        .setAutoID(Boolean.FALSE)
                        .build())
                .addFields(FieldSchema.newBuilder()
                        .setName("vector")
                        .setDataType(DataType.FloatVector)
                        .addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue("2").build())
                        .setIsPrimaryKey(Boolean.FALSE)
                        .setAutoID(Boolean.FALSE)
                        .build())
                .setEnableDynamicField(Boolean.FALSE)
                .build();
        DescribeCollectionResponse describeCollectionResponse = DescribeCollectionResponse.newBuilder()
                .setStatus(successStatus)
                .setCollectionName("test")
                .setSchema(collectionSchema)
                .setNumPartitions(1)
                .setCreatedUtcTimestamp(0)
                .build();

        IndexDescription index = IndexDescription.newBuilder()
                .setIndexName("test")
                .setFieldName("vector")
                .addParams(KeyValuePair.newBuilder()
                        .setKey("index_type")
                        .setValue("IVF_FLAT")
                        .build())
                .addParams(KeyValuePair.newBuilder()
                        .setKey("metric_type")
                        .setValue("L2")
                        .build())
                .build();
        DescribeIndexResponse describeIndexResponse = DescribeIndexResponse.newBuilder()
                .setStatus(successStatus)
                .addIndexDescriptions(index)
                .build();
        when(blockingStub.listDatabases(any())).thenReturn(ListDatabasesResponse.newBuilder().setStatus(successStatus).addDbNames("default").build());
        // collection api
        when(blockingStub.showCollections(any(ShowCollectionsRequest.class))).thenReturn(ShowCollectionsResponse.newBuilder().setStatus(successStatus).addAllCollectionNames(Collections.singletonList("test")).build());
        when(blockingStub.createCollection(any(CreateCollectionRequest.class))).thenReturn(successStatus);
        when(blockingStub.loadCollection(any())).thenReturn(successStatus);
        when(blockingStub.releaseCollection(any())).thenReturn(successStatus);
        when(blockingStub.getLoadState(any())).thenReturn(GetLoadStateResponse.newBuilder().setState(LoadState.LoadStateLoaded).setStatus(successStatus).build());
        when(blockingStub.dropCollection(any())).thenReturn(successStatus);
        when(blockingStub.hasCollection(any())).thenReturn(trueResponse);
        when(blockingStub.describeCollection(any())).thenReturn(describeCollectionResponse);
        when(blockingStub.renameCollection(any())).thenReturn(successStatus);
        when(blockingStub.getCollectionStatistics(any())).thenReturn(GetCollectionStatisticsResponse.newBuilder().addStats(KeyValuePair.newBuilder().setKey("row_count").setValue("10").build()).setStatus(successStatus).build());

        // index api
        when(blockingStub.createIndex(any())).thenReturn(successStatus);
        when(blockingStub.describeIndex(any())).thenReturn(describeIndexResponse);
        when(blockingStub.dropIndex(any())).thenReturn(successStatus);

        //vector api
        when(blockingStub.insert(any())).thenReturn(MutationResult.newBuilder().setInsertCnt(2L).build());
        when(blockingStub.upsert(any())).thenReturn(MutationResult.newBuilder().setUpsertCnt(2L).build());
        when(blockingStub.query(any())).thenReturn(QueryResults.newBuilder().build());
        when(blockingStub.delete(any())).thenReturn(MutationResult.newBuilder().setDeleteCnt(2L).build());
        SearchResults searchResults = SearchResults.newBuilder()
                .setResults(SearchResultData.newBuilder().addScores(1L).addTopks(0L).build())
                .build();
        when(blockingStub.search(any())).thenReturn(searchResults);

        // partition api
        when(blockingStub.createPartition(any())).thenReturn(successStatus);
        when(blockingStub.dropPartition(any())).thenReturn(successStatus);
        when(blockingStub.hasPartition(any())).thenReturn(trueResponse);
        when(blockingStub.showPartitions(any())).thenReturn(ShowPartitionsResponse.newBuilder().setStatus(successStatus).addPartitionNames("test").build());
        when(blockingStub.loadPartitions(any())).thenReturn(successStatus);
        when(blockingStub.releasePartitions(any())).thenReturn(successStatus);

        // role api
        when(blockingStub.createRole(any())).thenReturn(successStatus);
        when(blockingStub.dropRole(any())).thenReturn(successStatus);
        when(blockingStub.selectRole(any())).thenReturn(SelectRoleResponse.newBuilder().setStatus(successStatus).addResults(RoleResult.newBuilder().setRole(RoleEntity.newBuilder().setName("role_test").build()).build()).build());
        when(blockingStub.selectGrant(any())).thenReturn(SelectGrantResponse.newBuilder().setStatus(successStatus).addEntities(GrantEntity.newBuilder().setDbName("test").setObjectName("test").setObject(ObjectEntity.newBuilder().setName("test").build()).build()).build());

        when(blockingStub.operatePrivilege(any())).thenReturn(successStatus);
        when(blockingStub.operateUserRole(any())).thenReturn(successStatus);

        // user api
        when(blockingStub.listCredUsers(any())).thenReturn(ListCredUsersResponse.newBuilder().addUsernames("user_test").build());
        when(blockingStub.createCredential(any())).thenReturn(successStatus);
        when(blockingStub.updateCredential(any())).thenReturn(successStatus);
        when(blockingStub.deleteCredential(any())).thenReturn(successStatus);
        when(blockingStub.selectUser(any())).thenReturn(SelectUserResponse.newBuilder().setStatus(successStatus).addResults(UserResult.newBuilder().setUser(UserEntity.newBuilder().setName("user_test").build()).build()).build());

        // utility api
        when(blockingStub.flush(any())).thenReturn(FlushResponse.newBuilder().setStatus(successStatus).build());
        when(blockingStub.createAlias(any())).thenReturn(successStatus);
        when(blockingStub.dropAlias(any())).thenReturn(successStatus);
        when(blockingStub.alterAlias(any())).thenReturn(successStatus);
        when(blockingStub.describeAlias(any())).thenReturn(DescribeAliasResponse.newBuilder().setStatus(successStatus).build());
        when(blockingStub.listAliases(any())).thenReturn(ListAliasesResponse.newBuilder().setStatus(successStatus).addAliases("test").build());
    }
    @AfterEach
    public void tearDown() throws InterruptedException {
        client_v2.close(3);
    }
}

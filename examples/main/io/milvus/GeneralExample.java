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

package io.milvus;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.param.partition.*;

import java.util.*;

public class GeneralExample {
    private static MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    private static final String COLLECTION_NAME = "TEST";
    private static final String ID_FIELD = "userID";
    private static final String VECTOR_FIELD = "userFace";
    private static final Integer VECTOR_DIM = 64;

    private static final IndexType INDEX_TYPE = IndexType.IVF_FLAT;
    private static final String INDEX_PARAM = "{\"nlist\":128}";
    private static final MetricType METRIC_TYPE = MetricType.IP;

    private static final Integer SEARCH_K = 5;
    private static final String SEARCH_PARAM = "{\"nprobe\":10}";

    private R<RpcStatus> createCollection() {
        System.out.println("========== createCollection() ==========");
        FieldType fieldType1 = FieldType.newBuilder()
                .withName(ID_FIELD)
                .withDescription("user identification")
                .withDataType(DataType.Int64)
                .withAutoID(false)
                .withPrimaryKey(true)
                .build();

        FieldType fieldType2 = FieldType.newBuilder()
                .withName(VECTOR_FIELD)
                .withDescription("face embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(VECTOR_DIM)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withDescription("customer info")
                .withShardsNum(2)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .build();
        R<RpcStatus> response = milvusClient.createCollection(createCollectionReq);

        System.out.println(response);
        return response;
    }

    private R<RpcStatus> dropCollection() {
        System.out.println("========== dropCollection() ==========");
        R<RpcStatus> response = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    private R<Boolean> hasCollection() {
        System.out.println("========== hasCollection() ==========");
        R<Boolean> response = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println(response);
        return response;
    }

    private R<RpcStatus> loadCollection() {
        System.out.println("========== loadCollection() ==========");
        R<RpcStatus> response = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    private R<RpcStatus> releaseCollection() {
        System.out.println("========== releaseCollection() ==========");
        R<RpcStatus> response = milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    private R<DescribeCollectionResponse> describeCollection() {
        System.out.println("========== describeCollection() ==========");
        R<DescribeCollectionResponse> response = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    private R<GetCollectionStatisticsResponse> getCollectionStatistics() {
        System.out.println("========== getCollectionStatistics() ==========");
        R<GetCollectionStatisticsResponse> response = milvusClient.getCollectionStatistics(
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName(COLLECTION_NAME)
                        .build());
        System.out.println(response);
        return response;
    }

    private R<ShowCollectionsResponse> showCollections() {
        System.out.println("========== showCollections() ==========");
        R<ShowCollectionsResponse> response = milvusClient.showCollections(ShowCollectionsParam.newBuilder()
                .build());
        System.out.println(response);
        return response;
    }

    private R<RpcStatus> createPartition(String partitionName) {
        System.out.println("========== createPartition() ==========");
        R<RpcStatus> response = milvusClient.createPartition(CreatePartitionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    private R<RpcStatus> dropPartition(String partitionName) {
        System.out.println("========== dropPartition() ==========");
        R<RpcStatus> response = milvusClient.dropPartition(DropPartitionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    private R<Boolean> hasPartition(String partitionName) {
        System.out.println("========== hasPartition() ==========");
        R<Boolean> response = milvusClient.hasPartition(HasPartitionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    private R<RpcStatus> loadPartition(String partitionName) {
        System.out.println("========== loadPartition() ==========");
        R<RpcStatus> response = milvusClient.loadPartitions(LoadPartitionsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .addPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    private R<RpcStatus> releasePartition(String partitionName) {
        System.out.println("========== releasePartition() ==========");
        R<RpcStatus> response = milvusClient.releasePartitions(ReleasePartitionsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .addPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    private R<GetPartitionStatisticsResponse> getPartitionStatistics(String partitionName) {
        System.out.println("========== getPartitionStatistics() ==========");
        R<GetPartitionStatisticsResponse> response = milvusClient.getPartitionStatistics(
                GetPartitionStatisticsParam.newBuilder()
                        .withCollectionName(COLLECTION_NAME)
                        .withPartitionName(partitionName)
                        .build());

        System.out.println(response);
        return response;
    }

    private R<ShowPartitionsResponse> showPartitions() {
        System.out.println("========== showPartitions() ==========");
        R<ShowPartitionsResponse> response = milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    private R<RpcStatus> createIndex() {
        System.out.println("========== createIndex() ==========");
        R<RpcStatus> response = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(INDEX_TYPE)
                .withMetricType(METRIC_TYPE)
                .withExtraParam(INDEX_PARAM)
                .build());
        System.out.println(response);
        return response;
    }

    private R<RpcStatus> dropIndex() {
        System.out.println("========== dropIndex() ==========");
        R<RpcStatus> response = milvusClient.dropIndex(DropIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    private R<DescribeIndexResponse> describeIndex() {
        System.out.println("========== describeIndex() ==========");
        R<DescribeIndexResponse> response = milvusClient.describeIndex(DescribeIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    private R<GetIndexStateResponse> getIndexState() {
        System.out.println("========== getIndexState() ==========");
        R<GetIndexStateResponse> response = milvusClient.getIndexState(GetIndexStateParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    private R<GetIndexBuildProgressResponse> getIndexBuildProgress() {
        System.out.println("========== getIndexBuildProgress() ==========");
        R<GetIndexBuildProgressResponse> response = milvusClient.getIndexBuildProgress(
                GetIndexBuildProgressParam.newBuilder()
                        .withCollectionName(COLLECTION_NAME)
                        .build());
        System.out.println(response);
        return response;
    }

    private R<MutationResult> delete(String partitionName, String expr) {
        System.out.println("========== delete() ==========");
        DeleteParam build = DeleteParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .withExpr(expr)
                .build();
        R<MutationResult> response = milvusClient.delete(build);
        System.out.println(response.getData());
        return response;
    }

    private R<SearchResults> search(String expr) {
        System.out.println("========== search() ==========");
        List<String> outFields = Collections.singletonList(ID_FIELD);

        Random ran=new Random();
        List<Float> vector = new ArrayList<>();
        for (int d = 0; d < VECTOR_DIM; ++d) {
            vector.add(ran.nextFloat());
        }

        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withMetricType(MetricType.L2)
                .withOutFields(outFields)
                .withTopK(SEARCH_K)
                .withVectors(Collections.singletonList(vector))
                .withVectorFieldName(VECTOR_FIELD)
                .withExpr(expr)
                .withParams(SEARCH_PARAM)
                .build();


        R<SearchResults> response = milvusClient.search(searchParam);
        System.out.println(response);
        return response;
    }

    private R<CalcDistanceResults> calDistance() {
        System.out.println("========== calDistance() ==========");
        Random ran=new Random();
        List<Float> vector1 = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int d = 0; d < VECTOR_DIM; ++d) {
            vector1.add(ran.nextFloat());
            vector2.add(ran.nextFloat());
        }

        CalcDistanceParam calcDistanceParam = CalcDistanceParam.newBuilder()
                .withVectorsLeft(Collections.singletonList(vector1))
                .withVectorsRight(Collections.singletonList(vector2))
                .withMetricType(MetricType.L2)
                .build();
        R<CalcDistanceResults> response = milvusClient.calcDistance(calcDistanceParam);
        System.out.println(response);
        return response;
    }

    private R<QueryResults> query(String expr) {
        System.out.println("========== query() ==========");
        List<String> fields = Arrays.asList(ID_FIELD, VECTOR_FIELD);
        QueryParam test = QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .withOutFields(fields)
                .build();
        R<QueryResults> response = milvusClient.query(test);
        System.out.println(response);
        return response;
    }

    private R<MutationResult> insert(String partitionName, Long count) {
        System.out.println("========== insert() ==========");
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        Random ran=new Random();
        for (long i = 0L; i < count; ++i) {
            ids.add(i + 100L);
            List<Float> vector = new ArrayList<>();
            for (int d = 0; d < VECTOR_DIM; ++d) {
                vector.add(ran.nextFloat());
            }
            vectors.add(vector);
        }

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(ID_FIELD, DataType.Int64, ids));
        fields.add(new InsertParam.Field(VECTOR_FIELD, DataType.FloatVector, vectors));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .withFields(fields)
                .build();

        R<MutationResult> response = milvusClient.insert(insertParam);
        System.out.println(response);

        return response;
    }

    public static void main(String[] args) {
        GeneralExample example = new GeneralExample();

        example.dropCollection();
        example.createCollection();
        example.hasCollection();
        example.describeCollection();
        example.showCollections();
        example.loadCollection();
//        example.getCollectionStatistics();

        final String partitionName = "p1";
        example.createPartition(partitionName);
        example.hasPartition(partitionName);
        example.showPartitions();
        example.loadPartition(partitionName);
        example.getPartitionStatistics(partitionName);

        final Long row_count = 10000L;
        example.insert(partitionName, row_count);
        example.createIndex();
        example.describeIndex();
        example.getIndexBuildProgress();
        example.getIndexState();

        example.delete(partitionName, ID_FIELD + " in [105, 106, 107]");
        example.query(ID_FIELD + " in [101, 102]");
        example.search("");
        example.calDistance();

        example.releasePartition(partitionName);
        example.releaseCollection();
        example.dropPartition(partitionName);
        example.dropIndex();
        example.dropCollection();
    }
}

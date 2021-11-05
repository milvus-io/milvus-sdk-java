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
import java.util.Random;

public class GeneralExample {

    public static MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.Builder.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    public static final String COLLECTION_NAME = "TEST";
    public static final String ID_FIELD = "userID";
    public static final String VECTOR_FIELD = "userFace";
    public static final Integer VECTOR_DIM = 64;

    public static final IndexType INDEX_TYPE = IndexType.IVF_FLAT;
    public static final String INDEX_PARAM = "{\"nlist\":128}";
    public static final MetricType METRIC_TYPE = MetricType.IP;

    public static final Integer SEARCH_K = 5;
    public static final String SEARCH_PARAM = "{\"nprobe\":10}";

    public R<RpcStatus> createCollection() {
        System.out.println("========== createCollection() ==========");
        FieldType[] fieldTypes = new FieldType[2];
        FieldType fieldType1 = FieldType.Builder.newBuilder()
                .withName(ID_FIELD)
                .withDescription("user identification")
                .withDataType(DataType.Int64)
                .withAutoID(false)
                .withPrimaryKey(true)
                .build();

        FieldType fieldType2 = FieldType.Builder.newBuilder()
                .withName(VECTOR_FIELD)
                .withDescription("face embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(VECTOR_DIM)
                .build();
        fieldTypes[0] = fieldType1;
        fieldTypes[1] = fieldType2;

        CreateCollectionParam createCollectionReq = CreateCollectionParam.Builder.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withDescription("customer info")
                .withShardsNum(2)
                .withFieldTypes(fieldTypes)
                .build();
        R<RpcStatus> response = milvusClient.createCollection(createCollectionReq);

        System.out.println(response);
        return response;
    }

    public R<RpcStatus> dropCollection() {
        System.out.println("========== dropCollection() ==========");
        R<RpcStatus> response = milvusClient.dropCollection(DropCollectionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<Boolean> hasCollection() {
        System.out.println("========== hasCollection() ==========");
        R<Boolean> response = milvusClient.hasCollection(HasCollectionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println(response);
        return response;
    }

    public R<RpcStatus> loadCollection() {
        System.out.println("========== loadCollection() ==========");
        R<RpcStatus> response = milvusClient.loadCollection(LoadCollectionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<RpcStatus> releaseCollection() {
        System.out.println("========== releaseCollection() ==========");
        R<RpcStatus> response = milvusClient.releaseCollection(ReleaseCollectionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<DescribeCollectionResponse> describeCollection() {
        System.out.println("========== describeCollection() ==========");
        R<DescribeCollectionResponse> response = milvusClient.describeCollection(DescribeCollectionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<GetCollectionStatisticsResponse> getCollectionStatistics() {
        System.out.println("========== getCollectionStatistics() ==========");
        R<GetCollectionStatisticsResponse> response = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<ShowCollectionsResponse> showCollections() {
        System.out.println("========== showCollections() ==========");
        String[] collectionNames = new String[]{COLLECTION_NAME};
        R<ShowCollectionsResponse> response = milvusClient.showCollections(ShowCollectionsParam.Builder
                .newBuilder()
                .withCollectionNames(collectionNames)
                .build());
        System.out.println(response);
        return response;
    }

    public R<RpcStatus> createPartition(String partitionName) {
        System.out.println("========== createPartition() ==========");
        R<RpcStatus> response = milvusClient.createPartition(CreatePartitionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    public R<RpcStatus> dropPartition(String partitionName) {
        System.out.println("========== dropPartition() ==========");
        R<RpcStatus> response = milvusClient.dropPartition(DropPartitionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    public R<Boolean> hasPartition(String partitionName) {
        System.out.println("========== hasPartition() ==========");
        R<Boolean> response = milvusClient.hasPartition(HasPartitionParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    public R<RpcStatus> loadPartition(String partitionName) {
        System.out.println("========== loadPartition() ==========");
        String[] partitionNames = new String[]{partitionName};
        R<RpcStatus> response = milvusClient.loadPartitions(LoadPartitionsParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionNames(partitionNames)
                .build());

        System.out.println(response);
        return response;
    }

    public R<RpcStatus> releasePartition(String partitionName) {
        System.out.println("========== releasePartition() ==========");
        String[] releaseNames = new String[]{partitionName};
        R<RpcStatus> response = milvusClient.releasePartitions(ReleasePartitionsParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionNames(releaseNames)
                .build());

        System.out.println(response);
        return response;
    }

    public R<GetPartitionStatisticsResponse> getPartitionStatistics(String partitionName) {
        System.out.println("========== getPartitionStatistics() ==========");
        R<GetPartitionStatisticsResponse> response = milvusClient.getPartitionStatistics(GetPartitionStatisticsParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .build());

        System.out.println(response);
        return response;
    }

    public R<ShowPartitionsResponse> showPartitions() {
        System.out.println("========== showPartitions() ==========");
        R<ShowPartitionsResponse> response = milvusClient.showPartitions(ShowPartitionsParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<RpcStatus> createIndex() {
        System.out.println("========== createIndex() ==========");
        R<RpcStatus> response = milvusClient.createIndex(CreateIndexParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(INDEX_TYPE)
                .withMetricType(METRIC_TYPE)
                .withExtraParam(INDEX_PARAM)
                .build());
        System.out.println(response);
        return response;
    }

    public R<RpcStatus> dropIndex() {
        System.out.println("========== dropIndex() ==========");
        R<RpcStatus> response = milvusClient.dropIndex(DropIndexParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    public R<DescribeIndexResponse> describeIndex() {
        System.out.println("========== describeIndex() ==========");
        R<DescribeIndexResponse> response = milvusClient.describeIndex(DescribeIndexParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    public R<GetIndexStateResponse> getIndexState() {
        System.out.println("========== getIndexState() ==========");
        R<GetIndexStateResponse> response = milvusClient.getIndexState(GetIndexStateParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .build());
        System.out.println(response);
        return response;
    }

    public R<GetIndexBuildProgressResponse> getIndexBuildProgress() {
        System.out.println("========== getIndexBuildProgress() ==========");
        R<GetIndexBuildProgressResponse> response = milvusClient.getIndexBuildProgress(GetIndexBuildProgressParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println(response);
        return response;
    }

    public R<MutationResult> insert(String partitionName, Long count) {
        System.out.println("========== insert() ==========");
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        Random ran=new Random();
        for (Long i = 0L; i < count; ++i) {
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

        InsertParam insertParam = InsertParam.Builder
                .newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .withFields(fields)
                .build();

        R<MutationResult> response = milvusClient.insert(insertParam);
        System.out.println(response);

        R<FlushResponse> ret = milvusClient.flush(COLLECTION_NAME);
        System.out.println(ret.getData());

        return response;
    }

    public R<MutationResult> delete(String partitionName, String expr) {
        System.out.println("========== delete() ==========");
        DeleteParam build = DeleteParam.Builder.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPartitionName(partitionName)
                .withExpr(expr)
                .build();
        R<MutationResult> response = milvusClient.delete(build);
        System.out.println(response.getData());
        return response;
    }

    public R<SearchResults> search(String expr) {
        System.out.println("========== search() ==========");
        List<String> outFields = Collections.singletonList(ID_FIELD);

        Random ran=new Random();
        List<Float> vector = new ArrayList<>();
        for (int d = 0; d < VECTOR_DIM; ++d) {
            vector.add(ran.nextFloat());
        }

        SearchParam searchParam = SearchParam.Builder.newBuilder()
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

    public R<CalcDistanceResults> calDistance() {
        System.out.println("========== calDistance() ==========");
        Random ran=new Random();
        List<Float> vector1 = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int d = 0; d < VECTOR_DIM; ++d) {
            vector1.add(ran.nextFloat());
            vector2.add(ran.nextFloat());
        }

        CalcDistanceParam calcDistanceParam = CalcDistanceParam.Builder.newBuilder()
                .withVectorsLeft(Collections.singletonList(vector1))
                .withVectorsRight(Collections.singletonList(vector2))
                .withMetricType(MetricType.L2)
                .build();
        R<CalcDistanceResults> response = milvusClient.calcDistance(calcDistanceParam);
        System.out.println(response);
        return response;
    }

    public R<QueryResults> query(String expr) {
        System.out.println("========== query() ==========");
        List<String> fields = Arrays.asList(ID_FIELD, VECTOR_FIELD);
        QueryParam test = QueryParam.Builder.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .withOutFields(fields)
                .build();
        R<QueryResults> response = milvusClient.query(test);
        System.out.println(response);
        return response;
    }

    public static void main(String[] args) throws InterruptedException {
        GeneralExample example = new GeneralExample();

        example.dropCollection();
        example.createCollection();
        example.hasCollection();
        example.describeCollection();
        example.showCollections();
        example.loadCollection();
        example.getCollectionStatistics();

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

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
package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.common.collect.Lists;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.VectorUtils;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.dml.*;
import io.milvus.param.highlevel.dml.response.*;
import io.milvus.response.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class HighLevelExample {
    private static final MilvusServiceClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withAuthorization("root","Milvus")
                .build();
        milvusClient = new MilvusServiceClient(connectParam);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_highlevel_v1";
    private static final String ID_FIELD = "userID";
    private static final String VECTOR_FIELD = "userFace";
    private static final String USER_JSON_FIELD = "userJson";
    private static final Integer VECTOR_DIM = 36;
    private static final String AGE_FIELD = "userAge";

    private static final String INDEX_NAME = "userFaceIndex";
    private static final IndexType INDEX_TYPE = IndexType.IVF_FLAT;
    private static final String INDEX_PARAM = "{\"nlist\":128}";

    private static final String INT32_FIELD_NAME = "int32";
    private static final String INT64_FIELD_NAME = "int64";
    private static final String VARCHAR_FIELD_NAME = "varchar";
    private static final String BOOL_FIELD_NAME = "bool";
    private static final String FLOAT_FIELD_NAME = "float";
    private static final String DOUBLE_FIELD_NAME = "double";
    

    private R<DescribeCollectionResponse> describeCollection() {
        System.out.println("========== describeCollection() ==========");
        R<DescribeCollectionResponse> response = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(response);
        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(response.getData());
        System.out.println(wrapper);
        return response;
    }

    // >>>>>>>>>>>>> high level api
    private R<RpcStatus> createCollection() {
        System.out.println("========== high level createCollection ==========");
        CreateSimpleCollectionParam createSimpleCollectionParam = CreateSimpleCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withDimension(VECTOR_DIM)
                .withPrimaryField(ID_FIELD)
                .withVectorField(VECTOR_FIELD)
                .withAutoId(true)
                .build();

        R<RpcStatus> response = milvusClient.createCollection(createSimpleCollectionParam);
        CommonUtils.handleResponseStatus(response);
        System.out.println(response);
        return response;
    }

    private R<ListCollectionsResponse> listCollections() {
        System.out.println("========== high level listCollections ==========");
        ListCollectionsParam listCollectionsParam = ListCollectionsParam.newBuilder()
                .build();

        R<ListCollectionsResponse> response = milvusClient.listCollections(listCollectionsParam);
        CommonUtils.handleResponseStatus(response);
        System.out.println(response);
        return response;
    }

    private R<InsertResponse> insertRows(int rowCount) {
        System.out.println("========== high level insertRows ==========");
        List<JsonObject> rowsData = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(AGE_FIELD, ran.nextInt(99));
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            // $meta if collection EnableDynamicField, you can input this field not exist in schema, else deny
            row.addProperty(INT32_FIELD_NAME, ran.nextInt());
            row.addProperty(INT64_FIELD_NAME, ran.nextLong());
            row.addProperty(VARCHAR_FIELD_NAME, String.format("varchar_%d", i));
            row.addProperty(FLOAT_FIELD_NAME, ran.nextFloat());
            row.addProperty(DOUBLE_FIELD_NAME, ran.nextDouble());
            row.addProperty(BOOL_FIELD_NAME, ran.nextBoolean());

            // $json
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty(INT32_FIELD_NAME, ran.nextInt());
            jsonObject.addProperty(INT64_FIELD_NAME, ran.nextLong());
            jsonObject.addProperty(VARCHAR_FIELD_NAME, String.format("varchar_%d", i));
            jsonObject.addProperty(FLOAT_FIELD_NAME, ran.nextFloat());
            jsonObject.addProperty(DOUBLE_FIELD_NAME, ran.nextDouble());
            jsonObject.addProperty(BOOL_FIELD_NAME, ran.nextBoolean());
            row.add(USER_JSON_FIELD, jsonObject);

            rowsData.add(row);
        }

        InsertRowsParam insertRowsParam = InsertRowsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rowsData)
                .build();

        R<InsertResponse> response = milvusClient.insert(insertRowsParam);
        CommonUtils.handleResponseStatus(response);
        System.out.println("insertCount: " + response.getData().getInsertCount());
        System.out.println("insertIds: " + response.getData().getInsertIds());
        return response;
    }

    private R<DeleteResponse> delete(List<?> ids) {
        System.out.println("========== high level insertRows ==========");
        DeleteIdsParam deleteIdsParam = DeleteIdsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPrimaryIds(ids)
                .build();

        R<DeleteResponse> response = milvusClient.delete(deleteIdsParam);
        CommonUtils.handleResponseStatus(response);
        System.out.println(response);
        return response;
    }

    private R<GetResponse> get(List<?> ids) {
        System.out.println("========== high level get ==========");
        GetIdsParam getParam = GetIdsParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withPrimaryIds(ids)
                .build();

        R<GetResponse> response = milvusClient.get(getParam);
        CommonUtils.handleResponseStatus(response);
        for (QueryResultsWrapper.RowRecord rowRecord : response.getData().getRowRecords()) {
            System.out.println(rowRecord);
        }
        return response;
    }

    private R<SearchResponse> searchSimple(String filter) {
        System.out.println("========== high level search ==========");
        SearchSimpleParam searchSimpleParam = SearchSimpleParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withVectors(CommonUtils.generateFloatVector(VECTOR_DIM))
                .withFilter(filter)
                .withLimit(100L)
                .withOffset(0L)
                .withOutputFields(Lists.newArrayList("int32", "int64"))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<SearchResponse> response = milvusClient.search(searchSimpleParam);
        CommonUtils.handleResponseStatus(response);
        for (QueryResultsWrapper.RowRecord rowRecord : response.getData().getRowRecords(0)) {
            System.out.println(rowRecord);
        }
        return response;
    }

    private R<QueryResponse> querySimple(String filter) {
        milvusClient.flush(FlushParam.newBuilder().addCollectionName(COLLECTION_NAME).build());

        System.out.println("========== high level query ==========");
        QuerySimpleParam querySimpleParam = QuerySimpleParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFilter(filter)
                .withOutputFields(Lists.newArrayList("int32", "int64"))
                .withLimit(100L)
                .withOffset(0L)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        R<QueryResponse> response = milvusClient.query(querySimpleParam);
        CommonUtils.handleResponseStatus(response);
        for (QueryResultsWrapper.RowRecord rowRecord : response.getData().getRowRecords()) {
            System.out.println(rowRecord);
        }
        return response;
    }

    public static void main(String[] args) {
        HighLevelExample example = new HighLevelExample();
        example.createCollection();
        example.listCollections();

        R<DescribeCollectionResponse> describeCollectionResponseR = example.describeCollection();
        DescCollResponseWrapper descCollResponseWrapper = new DescCollResponseWrapper(describeCollectionResponseR.getData());

        int dataCount = 5;
        R<InsertResponse> insertResponse = example.insertRows(dataCount);

        List<?> insertIds = insertResponse.getData().getInsertIds();
        example.get(insertIds);

        String expr = VectorUtils.convertPksExpr(insertIds, descCollResponseWrapper);
        example.querySimple(expr);
        example.searchSimple(expr);

        // Asynchronous deletion. A successful response does not guarantee immediate data deletion. Please wait for a certain period of time for the deletion operation to take effect.
        example.delete(insertIds);
    }

}
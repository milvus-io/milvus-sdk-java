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

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class GeometryExample {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_geometry_v2";
    private static final String ID_FIELD = "id";
    private static final String GEO_FIELD = "geometry";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void createCollection() {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(GEO_FIELD)
                .dataType(DataType.Geometry)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .build();
        client.createCollection(requestCreate);

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        // geometry index no need metric type
        indexParams.add(IndexParam.builder()
                .fieldName(GEO_FIELD)
                .indexType(IndexParam.IndexType.RTREE)
                .build());
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection created: " + COLLECTION_NAME);
    }

    private static void insertGeometry(String geo) {
        JsonObject row = new JsonObject();
        row.addProperty(GEO_FIELD, geo);
        row.add(VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row))
                .build());
        System.out.println("Inserted geometry: " + geo);
    }

    private static void printRowCount() {
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void query(String filter) {
        System.out.println("===================================================");
        System.out.println("Query with filter expression: " + filter);
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Collections.singletonList(GEO_FIELD))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        System.out.println("Query results:");
        for (QueryResp.QueryResult result : queryResults) {
            System.out.println(result.getEntity());
        }
    }

    public static void main(String[] args) {
        createCollection();
        insertGeometry("POINT (1 1)");
        insertGeometry("LINESTRING (10 10, 10 30, 40 40)");
        insertGeometry("POLYGON ((0 100, 100 100, 100 50, 0 50, 0 100))");
        printRowCount();

        query("ST_EQUALS(" + GEO_FIELD + ", 'POINT (1 1)')");
        query("ST_TOUCHES(" + GEO_FIELD + ", 'LINESTRING (0 50, 0 100)')");
        query("ST_CONTAINS(" + GEO_FIELD + ", 'POINT (70 70)')");
        query("ST_CROSSES(" + GEO_FIELD + ", 'LINESTRING (20 0, 20 100)')");
        query("ST_WITHIN(" + GEO_FIELD + ", 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')");
    }
}

package com.zilliz.milvustestv2;

import com.google.common.collect.Interner;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Issue {
    public static void main2(String[] args) {
        MilvusClientV2 milvusClientV2New=new MilvusClientV2(ConnectConfig.builder()
                .uri("https://in01-f08382f0719b15b.aws-us-west-2.vectordb-uat3.zillizcloud.com:19532")
                .token("6aff239bed5702130e09ad03a3379a71034a3e7b4160de384dce58c501e5bf98e49816c670b4b6335f51276c294976ce6ba25fa4")
                .connectTimeoutMs(5000L)
                .build());
        String newCollectionName="indexpool";
        CreateCollectionReq.FieldSchema fieldInt64 = CreateCollectionReq.FieldSchema.builder()
                .autoID(false)
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(true)
                .name(CommonData.fieldInt64)
                .build();
        CreateCollectionReq.FieldSchema fieldVarchar = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.VarChar)
                .name(CommonData.fieldVarchar)
                .isPrimaryKey(false)
                .maxLength(65535)
                .build();
        CreateCollectionReq.FieldSchema fieldVector = CreateCollectionReq.FieldSchema.builder()
                .dataType(DataType.FloatVector)
                .isPrimaryKey(false)
                .name(CommonData.fieldFloatVector)
                .dimension(128)
                .build();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(fieldInt64);
        fieldSchemaList.add(fieldVarchar);
        fieldSchemaList.add(fieldVector);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionSchema(collectionSchema)
                .collectionName(newCollectionName)
                .enableDynamicField(false)
                .description("collection desc")
                .numShards(1)
                .build();
        milvusClientV2New.createCollection(createCollectionReq);

        IndexParam varcharIndexParam = IndexParam.builder()
                .fieldName(CommonData.fieldVarchar)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .build();
        IndexParam vectorIndexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .build();
        milvusClientV2New.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Lists.newArrayList(varcharIndexParam,vectorIndexParam))
                .build());
//        CommonFunction.genCommonData()

    }

    public static void main(String[] args) {
        MilvusClientV2 milvusClientV2New=new MilvusClientV2(ConnectConfig.builder()
                .uri("https://in01-8d2d543507bfe37.aws-us-west-2.vectordb-uat3.zillizcloud.com:19542")
                .token("6aff239bed5702130e09ad03a3379a71034a3e7b4160de384dce58c501e5bf98e49816c670b4b6335f51276c294976ce6ba25fa4")
                .connectTimeoutMs(5000L)
                .build());
        String newCollectionName="Collection_ypMcrBTRZC";

        QueryResp query = milvusClientV2New.query(QueryReq.builder()
                .collectionName(newCollectionName)
                .filter("VarChar_0 like \"9%\"")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                        .limit(1000L)
                .build());
        List<QueryResp.QueryResult> queryResults = query.getQueryResults();
List<String> pkList=new ArrayList<>();
        for (QueryResp.QueryResult queryResult : queryResults) {
            pkList.add(queryResult.getEntity().get("VarChar_0").toString());
        }
        System.out.println(pkList.size());

        List<BaseVector> data = CommonFunction.providerBaseVector(1, 768, DataType.FloatVector);
        List<Integer> result=new ArrayList<>();
        for (String s : pkList) {
        SearchResp search = milvusClientV2New.search(SearchReq.builder()
                .collectionName(newCollectionName)
                .outputFields(Lists.newArrayList("VarChar_0"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .filter("VarChar_0 in [\""+s+"\"]")
                .data(data)
                .topK(1)
                .build());
            System.out.println("Search："+s+"    "+search.getSearchResults().get(0));
            result.add(search.getSearchResults().get(0).size());
        }
        System.out.println(result);
    }
}

package com.zilliz.milvustestv2.collection;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddCollectionFunctionReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.AlterCollectionFunctionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionFunctionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for collection function management:
 * addCollectionFunction / alterCollectionFunction / dropCollectionFunction.
 * Note: these interfaces require Milvus server >= 2.6.8.
 *
 * Verified behavior on Milvus 2.6 (internal/proxy/function_task.go):
 * - addCollectionFunction / alterCollectionFunction / dropCollectionFunction hard-reject
 *   BM25 functions with "currently does not support adding/alter/droping BM25 function";
 *   2.6 only supports TextEmbedding via these interfaces (needs external embedding service,
 *   unavailable in test env). Defining a BM25 function in the schema at createCollection
 *   time IS supported.
 * - dropping a non-existent functionName is idempotent (no error).
 *
 * @Author yongpeng.li
 * @Date 2026
 */
public class CollectionFunctionTest extends BaseTest {

    private static final String FUNCTION_NAME_1 = "bm25_func_1";

    // ==================== Helpers ====================

    // Int64 pk + varchar(enable analyzer, BM25 input) + sparse vector field (BM25 output)
    private static CreateCollectionReq.CollectionSchema buildBaseSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(512)
                .enableAnalyzer(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldSparseVector)
                .dataType(DataType.SparseFloatVector)
                .build());
        return schema;
    }

    private static void createCollectionWithoutFunction(String name) {
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(buildBaseSchema())
                .build());
    }

    private static void createCollectionWithBm25Function(String name, String functionName) {
        CreateCollectionReq.CollectionSchema schema = buildBaseSchema();
        schema.addFunction(buildBm25Function(functionName, "bm25 function defined at create",
                CommonData.fieldSparseVector));
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
    }

    private static void dropCollectionQuietly(String name) {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(name)
                .build());
    }

    private static CreateCollectionReq.Function buildBm25Function(String name, String description, String outputField) {
        return CreateCollectionReq.Function.builder()
                .name(name)
                .description(description)
                .functionType(FunctionType.BM25)
                .inputFieldNames(Collections.singletonList(CommonData.fieldVarchar))
                .outputFieldNames(Collections.singletonList(outputField))
                .build();
    }

    private static DescribeCollectionResp describeCollection(String name) {
        return milvusClientV2.describeCollection(DescribeCollectionReq.builder()
                .collectionName(name)
                .build());
    }

    // ==================== Behavior-pinning cases (BM25 not supported via these interfaces on 2.6) ====================

    @Test(description = "Add BM25 function to an existing collection is rejected by 2.6 server", groups = {"Smoke"})
    public void addBm25FunctionNotSupportedYet() {
        // 2.6 服务端暂不支持 add BM25 function（硬编码拒绝），此用例为行为固化，服务端放开后应改写为正向
        String name = "CollFuncAdd_" + GenerateUtil.getRandomString(6);
        createCollectionWithoutFunction(name);
        try {
            milvusClientV2.addCollectionFunction(AddCollectionFunctionReq.builder()
                    .collectionName(name)
                    .function(buildBm25Function(FUNCTION_NAME_1, "bm25 function 1", CommonData.fieldSparseVector))
                    .build());
            Assert.fail("addCollectionFunction with BM25 type should throw exception on 2.6 server");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage() != null
                            && e.getMessage().contains("currently does not support adding BM25 function"),
                    "Exception message should indicate BM25 add is not supported, but was: " + e.getMessage());
        } finally {
            dropCollectionQuietly(name);
        }
    }

    @Test(description = "Alter BM25 function is rejected by 2.6 server", groups = {"Smoke"})
    public void alterBm25FunctionNotSupportedYet() {
        // 2.6 服务端暂不支持 alter BM25 function，此用例为行为固化，服务端放开后应改写为正向
        String name = "CollFuncAlter_" + GenerateUtil.getRandomString(6);
        createCollectionWithBm25Function(name, FUNCTION_NAME_1);
        try {
            milvusClientV2.alterCollectionFunction(AlterCollectionFunctionReq.builder()
                    .collectionName(name)
                    .function(buildBm25Function(FUNCTION_NAME_1, "updated description", CommonData.fieldSparseVector))
                    .build());
            Assert.fail("alterCollectionFunction with BM25 type should throw exception on 2.6 server");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage() != null
                            && e.getMessage().contains("currently does not support alter"),
                    "Exception message should indicate BM25 alter is not supported, but was: " + e.getMessage());
        } finally {
            dropCollectionQuietly(name);
        }
    }

    @Test(description = "Drop BM25 function is rejected by 2.6 server", groups = {"Smoke"})
    public void dropBm25FunctionNotSupportedYet() {
        // 2.6 服务端暂不支持 drop BM25 function，此用例为行为固化，服务端放开后应改写为正向
        String name = "CollFuncDrop_" + GenerateUtil.getRandomString(6);
        createCollectionWithBm25Function(name, FUNCTION_NAME_1);
        try {
            milvusClientV2.dropCollectionFunction(DropCollectionFunctionReq.builder()
                    .collectionName(name)
                    .functionName(FUNCTION_NAME_1)
                    .build());
            Assert.fail("dropCollectionFunction with BM25 type should throw exception on 2.6 server");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage() != null
                            && e.getMessage().contains("currently does not support"),
                    "Exception message should indicate BM25 drop is not supported, but was: " + e.getMessage());
        } finally {
            dropCollectionQuietly(name);
        }
    }

    @Test(description = "Drop a non-exist function is idempotent on 2.6 server", groups = {"Smoke"})
    public void dropNonExistFunctionIsIdempotent() {
        // 2.6 服务端对不存在的 functionName drop 幂等成功（不报错），此用例固化该实际行为
        String name = "CollFuncDropNonExist_" + GenerateUtil.getRandomString(6);
        createCollectionWithBm25Function(name, FUNCTION_NAME_1);
        try {
            milvusClientV2.dropCollectionFunction(DropCollectionFunctionReq.builder()
                    .collectionName(name)
                    .functionName("not_exist_function")
                    .build());

            List<CreateCollectionReq.Function> functionList =
                    describeCollection(name).getCollectionSchema().getFunctionList();
            Assert.assertEquals(functionList.size(), 1, "Original function list should be unchanged");
            Assert.assertEquals(functionList.get(0).getName(), FUNCTION_NAME_1);
        } finally {
            dropCollectionQuietly(name);
        }
    }

    // ==================== Positive case: BM25 function defined at create time ====================

    @Test(description = "Create collection with BM25 function in schema, insert text and run full text search", groups = {"Smoke"})
    public void insertAndSearchWithFunction() {
        // defining a BM25 function in the schema at createCollection time is supported on 2.6
        String name = "CollFuncSearch_" + GenerateUtil.getRandomString(6);
        createCollectionWithBm25Function(name, FUNCTION_NAME_1);
        try {
            // describe should return the function with correct name/input/output
            List<CreateCollectionReq.Function> functionList =
                    describeCollection(name).getCollectionSchema().getFunctionList();
            Assert.assertEquals(functionList.size(), 1, "Should have exactly 1 function");
            CreateCollectionReq.Function function = functionList.get(0);
            Assert.assertEquals(function.getName(), FUNCTION_NAME_1);
            Assert.assertEquals(function.getInputFieldNames(), Collections.singletonList(CommonData.fieldVarchar));
            Assert.assertEquals(function.getOutputFieldNames(), Collections.singletonList(CommonData.fieldSparseVector));

            // insert english text, sparse vector field is generated by the BM25 function
            List<JsonObject> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) i);
                row.addProperty(CommonData.fieldVarchar, "milvus vector database is a fast and scalable database " + i);
                data.add(row);
            }
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(name)
                    .data(data)
                    .build());

            // sparse vector field needs an index before loading
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldSparseVector)
                    .indexName("sparse_idx")
                    .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                    .metricType(IndexParam.MetricType.BM25)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(name)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());

            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(name)
                    .build());

            // full text search via EmbeddedText on the BM25 output field
            SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                    .collectionName(name)
                    .annsField(CommonData.fieldSparseVector)
                    .data(Collections.singletonList(new EmbeddedText("milvus vector database")))
                    .topK(10)
                    .metricType(IndexParam.MetricType.BM25)
                    .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.assertNotNull(searchResp);
            Assert.assertFalse(searchResp.getSearchResults().isEmpty());
            Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                    "BM25 search should hit inserted text rows");
            Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
        } finally {
            dropCollectionQuietly(name);
        }
    }

    // ==================== Negative Cases ====================

    @Test(description = "Add a function with empty name should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void addFunctionWithEmptyNameShouldFail() {
        // only assert an exception is thrown: server may reject BM25 type before validating fields
        String name = "CollFuncEmptyName_" + GenerateUtil.getRandomString(6);
        createCollectionWithoutFunction(name);
        try {
            milvusClientV2.addCollectionFunction(AddCollectionFunctionReq.builder()
                    .collectionName(name)
                    .function(buildBm25Function("", "function with empty name", CommonData.fieldSparseVector))
                    .build());
        } finally {
            dropCollectionQuietly(name);
        }
    }

    @Test(description = "Add function to a non-exist collection should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void addFunctionToNonExistCollectionShouldFail() {
        // only assert an exception is thrown: server may reject BM25 type before validating fields
        milvusClientV2.addCollectionFunction(AddCollectionFunctionReq.builder()
                .collectionName("NotExistCollection_" + GenerateUtil.getRandomString(6))
                .function(buildBm25Function("bm25_func_x", "function on non-exist collection", CommonData.fieldSparseVector))
                .build());
    }
}

package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SearchIteratorTest extends BaseTest {

    @DataProvider(name = "VectorTypeList")
    public Object[][] providerVectorType() {
        return new Object[][]{
                {CommonData.defaultFloatVectorCollection, DataType.FloatVector, CommonData.fieldFloatVector},
                {CommonData.defaultBinaryVectorCollection, DataType.BinaryVector, CommonData.fieldBinaryVector},
                {CommonData.defaultFloat16VectorCollection, DataType.Float16Vector, CommonData.fieldFloat16Vector},
                {CommonData.defaultBFloat16VectorCollection, DataType.BFloat16Vector, CommonData.fieldBF16Vector},
                {CommonData.defaultSparseFloatVectorCollection, DataType.SparseFloatVector, CommonData.fieldSparseVector},
        };
    }

    @Test(description = "search iterator test with expr", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchIteratorTestWithExpr(String collection, DataType vectorType, String vectorName) {
        SearchIterator searchIterator = milvusClientV2.searchIterator(SearchIteratorReq.builder()
                .collectionName(collection)
                .batchSize(CommonData.batchSize)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
//                .params("{\"range_filter\": 15.0, \"radius\": 20.0}")
                .expr(CommonData.fieldInt64 + "< 10 ")
                .metricType(CommonFunction.provideMetricTypeByVectorType(vectorType))
                .vectors(CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType))
                .vectorFieldName(vectorName)
                .batchSize(10)
                .build());
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(searchIterator.next().size(), 10);
        }

    }

    @Test(description = "search iterator test2", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchIteratorTest(String collection, DataType vectorType, String vectorName) {
        SearchIterator searchIterator = milvusClientV2.searchIterator(SearchIteratorReq.builder()
                .collectionName(collection)
                .batchSize(CommonData.batchSize)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
//                .params("{\"range_filter\": 15.0, \"radius\": 20.0}")
//                .expr(CommonData.fieldInt64 + "< 10 ")
                .metricType(CommonFunction.provideMetricTypeByVectorType(vectorType))
                .vectors(CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType))
                .vectorFieldName(vectorName)
                .topK(10000)
                .build());
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(searchIterator.next().size(), 1000);
        } else {
            Assert.assertTrue(searchIterator.next().size() > 1);
        }
    }

    @Test(description = "search iterator with error range filter", groups = {"L2"})
    public void searchIteratorWithErrorRangeFilter() {
        try {
            SearchIterator searchIterator = milvusClientV2.searchIterator(SearchIteratorReq.builder()
                    .collectionName(CommonData.defaultFloatVectorCollection)
                    .batchSize(CommonData.batchSize)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Lists.newArrayList("*"))
                    .params("{\"range_filter\": 25.0, \"radius\": 20.0}")
                    //                .expr(CommonData.fieldInt64 + "< 10 ")
                    .metricType(CommonFunction.provideMetricTypeByVectorType(DataType.FloatVector))
                    .vectors(CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector))
                    .vectorFieldName(CommonData.fieldFloatVector)
                    .topK(10000)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("radius must be larger than range_filter"));
        }

    }
}

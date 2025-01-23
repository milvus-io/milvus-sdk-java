package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class QueryIteratorTest extends BaseTest {
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


    @Test(description = "Query iterator", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void queryIteratorTest(String collection, DataType vectorType, String vectorField) {
        QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collection)
                .batchSize(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .limit(1000)
                .build());
        int i = 0;
        int size;
        do {
            size = queryIterator.next().size();
            i++;
        } while (size > 0);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(i, 101);
        }else {
            Assert.assertTrue(i>0);
        }

    }
}

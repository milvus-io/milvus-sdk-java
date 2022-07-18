/*
package com.zilliz.milvustest.load;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.ImportResponse;
import io.milvus.grpc.ListImportTasksResponse;
import io.milvus.param.R;
import io.milvus.param.dml.BulkloadParam;
import io.milvus.param.dml.ListBulkloadTasksParam;
import io.qameta.allure.Epic;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

public class ListBulkloadTasksTest extends BaseTest {
    @BeforeClass(description = "init bulk load task")
    public void bulkLoad() {
        R<ImportResponse> importResponseR = milvusClient.bulkload(BulkloadParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .addFile("rowJson0.json")
                .addFile("rowJson1.json")
                .build());

    }
    @Epic("L0")
    @Test(description = "List all bulk load tasks")
    public void listBulkLoadTest() {
        R<ListImportTasksResponse> listImportTasksResponseR = milvusClient.listBulkloadTasks(ListBulkloadTasksParam.newBuilder()
                .build());
        Assert.assertEquals(listImportTasksResponseR.getStatus().intValue(), 0);
        Assert.assertTrue(listImportTasksResponseR.getData().getTasksList().size()>1);
    }

}
*/

package com.zilliz.milvustestv2.bulk;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.bulkwriter.BulkImport;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class BulkImportTest extends BaseTest {

    String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = CommonFunction.createSimpleCollection(CommonData.dim, null,false);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName).build());
    }

    @Test(description = "bulk import", groups = {"L1"})
    public void bulkImport() {
        List<List<String>> batchFiles = CommonFunction.providerBatchFiles(collectionName, BulkFileType.PARQUET,10000);

        MilvusImportRequest milvusImportRequest = MilvusImportRequest.builder()
                .collectionName(collectionName)
                .files(batchFiles)
                .build();
        String bulkImportResult = BulkImport.bulkImport(
                System.getProperty("uri")== null? PropertyFilesUtil.getRunValue("uri"):System.getProperty("uri"),
                milvusImportRequest);
        JsonObject bulkImportJO= JsonParser.parseString(bulkImportResult).getAsJsonObject();
        String jobId = bulkImportJO.getAsJsonObject("data").get("jobId").getAsString();
        System.out.println(jobId);
        Assert.assertNotNull(jobId);

    }




}

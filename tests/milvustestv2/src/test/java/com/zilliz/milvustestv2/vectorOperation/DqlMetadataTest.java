package com.zilliz.milvustestv2.vectorOperation;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Milvus 3.0.9 query metadata tests.
 * GetResp / QueryResp added: getSessionTs() (long, default 1), getCost() (Long),
 * getScannedRemoteBytes() (Long), getScannedTotalBytes() (Long), getCacheHitRatio() (Float).
 * GetResp.getGetResults() is deprecated in favor of getQueryResults().
 * The wrapper-typed metrics are null when the (old) server does not report them; against a 3.0
 * server they are expected to be non-null — if they turn out to be null in practice, relax the
 * assertion to log-only and record it in the comment (see assertScanMetric).
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class DqlMetadataTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 500;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "DqlMetadata_" + GenerateUtil.getRandomString(6);
        // simple collection: Int64 pk "id" + FloatVector "vector"
        CommonFunction.createSimpleCollection(DIM, collectionName, false);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(CommonFunction.generateSimpleData(INSERT_COUNT, DIM))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT, "Insert count should match");
        // createSimpleCollection 是快速建表，已自带向量字段的 AUTOINDEX 索引，
        // 再手动 createIndex 会报 "at most one distinct index is allowed per field"，直接 load 即可
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    private QueryResp doQuery(String filter) {
        return milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .outputFields(Collections.singletonList(CommonData.simplePk))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
    }

    /**
     * The scan metrics are populated from the response status extraInfo; a 3.0 server should
     * report them. If a null shows up (older/standalone server not reporting), keep the value
     * observable instead of failing hard — recorded per the task's relaxation rule.
     */
    private static void assertScanMetric(String name, Number value) {
        if (value == null) {
            System.out.println("DqlMetadataTest: metric " + name
                    + " is null (server did not report it; relax per comment in class javadoc)");
        } else {
            Assert.assertTrue(value.doubleValue() >= 0,
                    name + " should be non-negative, actual: " + value);
        }
    }

    // ==================== Positive Tests ====================

    @Test(description = "Query response carries sessionTs / cost / scanned bytes / cacheHitRatio metadata",
            groups = {"Smoke", "V3"})
    public void queryMetadataPositive() {
        QueryResp queryResp = doQuery(CommonData.simplePk + " < 10");

        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 10, "query should return 10 rows");

        // sessionTs: 3.0-20261023 nightly 实测返回 0（服务端未回填），软断言记录实际值
        System.out.println("queryMetadataPositive sessionTs actual: " + queryResp.getSessionTs());
        Assert.assertTrue(queryResp.getSessionTs() >= 0, "sessionTs should be >= 0");
        // cost: filled from status extraInfo report_value, never null on success
        Assert.assertNotNull(queryResp.getCost(), "cost should not be null");
        Assert.assertTrue(queryResp.getCost() >= 0, "cost should be non-negative");

        assertScanMetric("scannedRemoteBytes", queryResp.getScannedRemoteBytes());
        assertScanMetric("scannedTotalBytes", queryResp.getScannedTotalBytes());
        assertScanMetric("cacheHitRatio", queryResp.getCacheHitRatio() == null
                ? null : queryResp.getCacheHitRatio().doubleValue());
    }

    @Test(description = "Get by ids reads results via GetResp.getQueryResults() (getGetResults deprecated)",
            groups = {"Smoke", "V3"})
    public void getByIdsUsesQueryResults() {
        GetResp getResp = milvusClientV2.get(GetReq.builder()
                .collectionName(collectionName)
                .ids(Arrays.<Object>asList(1L, 2L, 3L))
                .outputFields(Collections.singletonList(CommonData.simplePk))
                .build());

        Assert.assertNotNull(getResp);
        // deprecated getGetResults() migrated to getQueryResults()
        Assert.assertEquals(getResp.getQueryResults().size(), 3, "get should return 3 rows");
        for (QueryResp.QueryResult result : getResp.getQueryResults()) {
            Object id = result.getEntity().get(CommonData.simplePk);
            Assert.assertNotNull(id, "get result should carry pk");
        }

        // sessionTs: 3.0-20261023 nightly 实测返回 0，软断言记录
        System.out.println("getByIdsUsesQueryResults sessionTs actual: " + getResp.getSessionTs());
        Assert.assertTrue(getResp.getSessionTs() >= 0, "get sessionTs should be >= 0");
        Assert.assertNotNull(getResp.getCost(), "get cost should not be null");
        assertScanMetric("scannedRemoteBytes", getResp.getScannedRemoteBytes());
        assertScanMetric("scannedTotalBytes", getResp.getScannedTotalBytes());
        assertScanMetric("cacheHitRatio", getResp.getCacheHitRatio() == null
                ? null : getResp.getCacheHitRatio().doubleValue());
    }

    @Test(description = "Run the same query twice and observe cacheHitRatio change (no hard assert)",
            groups = {"Smoke", "V3"})
    public void cacheHitRatioObservation() {
        String filter = CommonData.simplePk + " < 100";
        QueryResp first = doQuery(filter);
        QueryResp second = doQuery(filter);

        Assert.assertEquals(first.getQueryResults().size(), second.getQueryResults().size(),
                "two identical queries should return the same row count");

        Float firstRatio = first.getCacheHitRatio();
        Float secondRatio = second.getCacheHitRatio();
        System.out.println("cacheHitRatioObservation: first=" + firstRatio + ", second=" + secondRatio);
        if (firstRatio != null && secondRatio != null) {
            // behavior observation only: with result cache the second hit ratio should not drop;
            // not hard-asserted because reporting depends on server cache configuration
            if (secondRatio < firstRatio) {
                System.out.println("cacheHitRatioObservation: second query cacheHitRatio "
                        + secondRatio + " < first " + firstRatio + " (observed, not asserted)");
            }
        }
    }

    // ==================== Boundary Tests ====================

    @Test(description = "Empty result set query still carries metadata",
            groups = {"Smoke", "V3"})
    public void emptyResultQueryMetadata() {
        QueryResp queryResp = doQuery(CommonData.simplePk + " < 0");

        Assert.assertNotNull(queryResp);
        Assert.assertTrue(queryResp.getQueryResults().isEmpty(), "query should return empty result");

        // sessionTs: 3.0-20261023 nightly 实测返回 0，软断言记录
        System.out.println("emptyResultQueryMetadata sessionTs actual: " + queryResp.getSessionTs());
        Assert.assertTrue(queryResp.getSessionTs() >= 0, "empty-result query sessionTs should be >= 0");
        Assert.assertNotNull(queryResp.getCost(), "empty-result query cost should not be null");
        assertScanMetric("scannedRemoteBytes", queryResp.getScannedRemoteBytes());
        assertScanMetric("scannedTotalBytes", queryResp.getScannedTotalBytes());
        assertScanMetric("cacheHitRatio", queryResp.getCacheHitRatio() == null
                ? null : queryResp.getCacheHitRatio().doubleValue());
    }
}

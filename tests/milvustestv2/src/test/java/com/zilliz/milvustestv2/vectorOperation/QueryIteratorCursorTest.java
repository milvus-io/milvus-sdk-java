package com.zilliz.milvustestv2.vectorOperation;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.QueryIteratorCursor;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.response.InsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Milvus 3.0 resumable QueryIterator cursor tests.
 * Facts (see IteratorExample.queryIteratorWithCursor):
 * - QueryIteratorCursor cursor = queryIterator.getCursor() captures sessionTs / intPk / strPk /
 *   lastElementOffset (all final fields, builder available, no serialization API);
 * - resume with QueryIteratorReq.builder()....cursor(cursor).build();
 * - pitfall: limit is counted per iterator, so the resumed iterator needs
 *   .limit(limit - rowsAlreadyConsumed).
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class QueryIteratorCursorTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;
    private static final int BATCH_SIZE = 100;
    private static final int TOTAL_LIMIT = 300;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "QueryIteratorCursor_" + GenerateUtil.getRandomString(6);
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

    private static List<Long> drainIterator(QueryIterator queryIterator, List<Long> sink, long maxCount) {
        long consumed = 0;
        while (consumed < maxCount) {
            List<QueryResultsWrapper.RowRecord> records = queryIterator.next();
            if (records.isEmpty()) {
                break;
            }
            for (QueryResultsWrapper.RowRecord record : records) {
                Object id = record.get(CommonData.simplePk);
                sink.add(((Number) id).longValue());
                consumed++;
            }
        }
        return sink;
    }

    // ==================== Positive Tests ====================

    @Test(description = "Capture cursor after 100 rows, resume in a new iterator to complete 300 rows",
            groups = {"Smoke", "V3"})
    public void queryIteratorCursorResume() {
        String expr = CommonData.simplePk + " >= 0";

        // first segment: consume one batch (100 rows) out of limit 300
        QueryIterator firstIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(expr)
                .outputFields(Collections.singletonList(CommonData.simplePk))
                .batchSize(BATCH_SIZE)
                .limit(TOTAL_LIMIT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> firstSegmentIds = new ArrayList<>();
        drainIterator(firstIterator, firstSegmentIds, BATCH_SIZE);
        Assert.assertEquals(firstSegmentIds.size(), BATCH_SIZE,
                "first segment should consume exactly one batch");

        QueryIteratorCursor cursor = firstIterator.getCursor();
        firstIterator.close();

        // cursor field assertions
        Assert.assertNotNull(cursor, "cursor should not be null");
        Assert.assertTrue(cursor.getSessionTs() > 0, "cursor sessionTs should be positive");
        Assert.assertNotNull(cursor.getIntPk(), "cursor intPk should not be null for Int64 pk");
        Assert.assertNull(cursor.getStrPk(), "cursor strPk should be null for Int64 pk");
        Assert.assertNull(cursor.getLastElementOffset(),
                "cursor lastElementOffset should be null for non-element-filter iterator");
        long lastIdOfFirstSegment = firstSegmentIds.get(firstSegmentIds.size() - 1);
        Assert.assertEquals(cursor.getIntPk().longValue(), lastIdOfFirstSegment,
                "cursor intPk should be the last pk of the consumed segment");

        // second segment: resume with cursor; limit is per-iterator, so pass the remaining count
        QueryIterator resumedIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(expr)
                .outputFields(Collections.singletonList(CommonData.simplePk))
                .batchSize(BATCH_SIZE)
                .limit(TOTAL_LIMIT - firstSegmentIds.size())
                .cursor(cursor)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> secondSegmentIds = new ArrayList<>();
        drainIterator(resumedIterator, secondSegmentIds, TOTAL_LIMIT);
        resumedIterator.close();

        Assert.assertEquals(secondSegmentIds.size(), TOTAL_LIMIT - BATCH_SIZE,
                "resumed iterator should return the remaining rows");
        Assert.assertEquals(firstSegmentIds.size() + secondSegmentIds.size(), TOTAL_LIMIT,
                "two segments should total 300 rows");

        // no duplicate pk across the two segments
        Set<Long> allIds = new HashSet<>(firstSegmentIds);
        allIds.addAll(secondSegmentIds);
        Assert.assertEquals(allIds.size(), TOTAL_LIMIT, "segments should have no duplicate pk");

        // the second segment continues right after the first segment (pk-ordered iteration)
        long maxFirstSegment = Collections.max(firstSegmentIds);
        long minSecondSegment = Collections.min(secondSegmentIds);
        Assert.assertTrue(minSecondSegment > maxFirstSegment,
                "resumed segment should start after the first segment's last pk, first segment max="
                        + maxFirstSegment + ", second segment min=" + minSecondSegment);
    }

    // ==================== Boundary / Negative Tests ====================

    @Test(description = "Iterator built with explicit null cursor works like a normal iterator",
            groups = {"Smoke", "V3"})
    public void iteratorWithNullCursorWorks() {
        QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(CommonData.simplePk + " >= 0")
                .outputFields(Collections.singletonList(CommonData.simplePk))
                .batchSize(BATCH_SIZE)
                .limit(50)
                .cursor(null)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> ids = new ArrayList<>();
        drainIterator(queryIterator, ids, 50);
        queryIterator.close();
        Assert.assertEquals(ids.size(), 50, "null-cursor iterator should return 50 rows");
    }

    @Test(description = "Use a cursor from collection A on an iterator of collection B: behavior record",
            groups = {"Smoke", "V3"})
    public void cursorUsedAcrossCollections() {
        // second collection with a disjoint pk range (10000..10099)
        String otherCollection = "QueryIteratorCursorOther_" + GenerateUtil.getRandomString(6);
        try {
            CommonFunction.createSimpleCollection(DIM, otherCollection, false);
            List<com.google.gson.JsonObject> data = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                com.google.gson.JsonObject row = new com.google.gson.JsonObject();
                row.addProperty(CommonData.simplePk, 10000L + i);
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < DIM; k++) {
                    vector.add((float) Math.random());
                }
                row.add(CommonData.simpleVector, new com.google.gson.Gson().toJsonTree(vector));
                data.add(row);
            }
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(otherCollection)
                    .data(data)
                    .build());
            // 快速建表已自带 AUTOINDEX 索引，无需（也不能再）手动建索引，直接 load
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(otherCollection)
                    .build());

            // capture cursor from the main collection after consuming 100 rows (intPk=99)
            QueryIterator firstIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                    .collectionName(collectionName)
                    .expr(CommonData.simplePk + " >= 0")
                    .outputFields(Collections.singletonList(CommonData.simplePk))
                    .batchSize(BATCH_SIZE)
                    .limit(TOTAL_LIMIT)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            List<Long> consumed = new ArrayList<>();
            drainIterator(firstIterator, consumed, BATCH_SIZE);
            QueryIteratorCursor cursor = firstIterator.getCursor();
            firstIterator.close();

            try {
                QueryIterator crossIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                        .collectionName(otherCollection)
                        .expr(CommonData.simplePk + " >= 0")
                        .outputFields(Collections.singletonList(CommonData.simplePk))
                        .batchSize(BATCH_SIZE)
                        .limit(10)
                        .cursor(cursor)
                        .consistencyLevel(ConsistencyLevel.STRONG)
                        .build());
                List<Long> crossIds = new ArrayList<>();
                drainIterator(crossIterator, crossIds, 10);
                crossIterator.close();
                // Observed behavior (3.0): no exception and no collection binding check. The cursor
                // only carries sessionTs + last pk, so the resumed iterator on another collection
                // silently continues with "pk > cursor.intPk" against THAT collection's data —
                // wrong-scope data (ids 10000+). A cursor must only be reused on the same
                // collection + same expr.
                System.out.println("cursorUsedAcrossCollections: cross-collection cursor returned "
                        + crossIds.size() + " rows, ids=" + crossIds
                        + " (no exception; cursor is not bound to a collection)");
            } catch (Exception e) {
                // alternative acceptable behavior: server rejects the foreign cursor
                System.out.println("cursorUsedAcrossCollections: cross-collection cursor threw: "
                        + e.getMessage());
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(otherCollection)
                    .build());
        }
    }
}

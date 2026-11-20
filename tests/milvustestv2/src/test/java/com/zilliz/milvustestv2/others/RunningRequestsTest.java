package com.zilliz.milvustestv2.others;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.shaded.io.grpc.CallOptions;
import io.milvus.shaded.io.grpc.Channel;
import io.milvus.shaded.io.grpc.ClientCall;
import io.milvus.shaded.io.grpc.MethodDescriptor;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.request.CancelRequestsReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.utility.request.ListRunningRequestsReq;
import io.milvus.v2.service.utility.response.CancelRequestsResp;
import io.milvus.v2.service.utility.response.ListRunningRequestsResp;
import io.milvus.v2.service.utility.response.RunningRequestInfo;
import io.milvus.v2.service.utility.response.RunningRequestNodeResult;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Single-Proxy native SDK parity with the Python and REST L0 lifecycle. */
public class RunningRequestsTest {
    private MilvusClientV2 client;
    private long nodeId;

    @BeforeMethod(alwaysRun = true)
    public void connect() {
        nodeId = 0;
        String uri = System.getProperty("uri", PropertyFilesUtil.getRunValue("uri"));
        String token = System.getProperty("token", "root:Milvus");
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(uri).token(token).rpcDeadlineMs(120000L).build());
        ListRunningRequestsResp result = list("missing_" + UUID.randomUUID().toString().replace("-", ""));
        if (result.getNodeResults().size() > 1) {
            throw new SkipException("Requires a single Proxy");
        }
        assertAnswered(result.getNodeResults());
        nodeId = result.getNodeResults().get(0).getNodeId();
    }

    @AfterMethod(alwaysRun = true)
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private void assertAnswered(List<RunningRequestNodeResult> nodes) {
        Assert.assertEquals(nodes.size(), 1);
        Assert.assertTrue(nodes.get(0).isSuccess());
        Assert.assertFalse(nodes.get(0).getUnimplemented());
        Assert.assertTrue(nodes.get(0).getNodeId() > 0);
        if (nodeId != 0) {
            Assert.assertEquals(nodes.get(0).getNodeId(), nodeId);
        }
    }

    private ListRunningRequestsResp list(String collection) {
        return client.listRunningRequests(ListRunningRequestsReq.builder()
                .collectionName(collection).build());
    }

    @Test(groups = {"Smoke"})
    public void emptyListAndUnknownId() {
        String missing = "unknown_" + UUID.randomUUID().toString().replace("-", "");
        ListRunningRequestsResp result = list(missing);
        assertAnswered(result.getNodeResults());
        Assert.assertTrue(result.getRequests().isEmpty());
        CancelRequestsResp canceled = client.cancelRequests(
                CancelRequestsReq.builder().requestIds(Collections.singletonList(missing)).build());
        assertAnswered(canceled.getNodeResults());
        Assert.assertEquals(canceled.getNotFound(), Collections.singletonList(missing));
        Assert.assertTrue(canceled.getCanceled().isEmpty());
        Assert.assertTrue(canceled.getUndetermined().isEmpty());
    }

    @Test(groups = {"Smoke"})
    public void emptyIdsDoNotSendRpc() {
        Channel noRpcChannel = new Channel() {
            @Override
            public String authority() {
                return "unused";
            }

            @Override
            public <A, B> ClientCall<A, B> newCall(MethodDescriptor<A, B> method, CallOptions options) {
                throw new AssertionError("Invalid IDs issued an RPC: " + method.getFullMethodName());
            }
        };
        client.setBlockingStub(MilvusServiceGrpc.newBlockingStub(noRpcChannel));
        for (List<String> ids : Arrays.asList(Collections.<String>emptyList(), null)) {
            try {
                client.cancelRequests(CancelRequestsReq.builder().requestIds(ids).build());
                Assert.fail("Empty/null IDs were accepted");
            } catch (MilvusClientException error) {
                Assert.assertEquals(error.getErrorCode().name(), "INVALID_PARAMS");
            }
        }
    }

    @Test(groups = {"Smoke"})
    public void cancelSearchAndImmediatelyUnregister() throws Exception {
        final int dim = 128;
        final int nq = 16384;
        final int topK = 10;
        String collection = "running_requests_" + UUID.randomUUID().toString().replace("-", "");
        String reason = "java-single-proxy-" + UUID.randomUUID();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(true).build());
        schema.addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(dim).build());
        client.createCollection(CreateCollectionReq.builder().collectionName(collection)
                .collectionSchema(schema).consistencyLevel(ConsistencyLevel.STRONG).build());
        // Register cleanup only after creation. If the test fails, preserve that error.
        try (AutoCloseable dropCollection = () -> client.dropCollection(
                DropCollectionReq.builder().collectionName(collection).build())) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<SearchResp> target = null;
            try {
                Random random = new Random(1113);
                for (int start = 0; start < 100000; start += 5000) {
                    List<JsonObject> rows = new ArrayList<>();
                    for (int offset = 0; offset < 5000; offset++) {
                        JsonObject row = new JsonObject();
                        row.addProperty("id", start + offset);
                        JsonArray vector = new JsonArray();
                        for (int j = 0; j < dim; j++) {
                            vector.add(random.nextFloat());
                        }
                        row.add("vector", vector);
                        rows.add(row);
                    }
                    client.insert(InsertReq.builder().collectionName(collection).data(rows).build());
                }
                client.flush(FlushReq.builder().collectionNames(Collections.singletonList(collection)).build());
                client.createIndex(CreateIndexReq.builder().collectionName(collection)
                        .indexParams(Collections.singletonList(IndexParam.builder().fieldName("vector")
                                .indexType(IndexParam.IndexType.FLAT).metricType(IndexParam.MetricType.L2).build())).build());
                client.loadCollection(LoadCollectionReq.builder().collectionName(collection).build());
                List<BaseVector> vectors = new ArrayList<>();
                for (int i = 0; i < nq; i++) {
                    float[] vector = new float[dim];
                    for (int j = 0; j < dim; j++) {
                        vector[j] = random.nextFloat();
                    }
                    vectors.add(new FloatVec(vector));
                }
                SearchReq search = SearchReq.builder().collectionName(collection).annsField("vector")
                        .data(vectors).filter("id >= 0").topK(topK).build();
                target = executor.submit(() -> client.search(search));
                RunningRequestInfo running = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (System.nanoTime() < deadline) {
                    ListRunningRequestsResp listed = list(collection);
                    assertAnswered(listed.getNodeResults());
                    if (!listed.getRequests().isEmpty()) {
                        Assert.assertEquals(listed.getRequests().size(), 1);
                        running = listed.getRequests().get(0);
                        break;
                    }
                    if (target.isDone()) {
                        target.get();
                        Assert.fail("Search finished before List captured it");
                    }
                    Thread.sleep(10);
                }
                Assert.assertNotNull(running, "List did not capture Search");
                Assert.assertEquals(running.getType(), "Search");
                Assert.assertEquals(running.getCollectionName(), collection);
                Assert.assertEquals(running.getDatabaseName(), "default");
                Assert.assertEquals(running.getProxyId(), nodeId);
                Assert.assertEquals(running.getNq().longValue(), (long) nq);
                Assert.assertEquals(running.getTopK().longValue(), (long) topK);
                Assert.assertTrue(running.getCancellable());
                String id = running.getRequestId();
                Assert.assertFalse(id.isEmpty());
                CancelRequestsResp canceled = client.cancelRequests(
                        CancelRequestsReq.builder().requestIds(Collections.singletonList(id)).reason(reason).build());
                assertAnswered(canceled.getNodeResults());
                Assert.assertEquals(canceled.getCanceled().size(), 1);
                Assert.assertEquals(canceled.getCanceled().get(0).getRequestId(), id);
                Assert.assertTrue(canceled.getNotFound().isEmpty());
                Assert.assertTrue(canceled.getUndetermined().isEmpty());
                try {
                    target.get(60, TimeUnit.SECONDS);
                    Assert.fail("Canceled Search completed normally");
                } catch (ExecutionException error) {
                    Assert.assertTrue(error.getCause() instanceof MilvusClientException, error.toString());
                    MilvusClientException cause = (MilvusClientException) error.getCause();
                    Assert.assertEquals(cause.getServerErrCode(), 3002);
                    Assert.assertTrue(cause.getMessage().contains(reason), cause.toString());
                }
                ListRunningRequestsResp after = list(collection);
                assertAnswered(after.getNodeResults());
                Assert.assertTrue(after.getRequests().stream().noneMatch(row -> id.equals(row.getRequestId())),
                        "Canceled Search remains registered");
                SearchResp control = client.search(SearchReq.builder().collectionName(collection).annsField("vector")
                        .data(vectors.subList(0, 1)).filter("id >= 0").topK(topK).build());
                Assert.assertEquals(control.getSearchResults().size(), 1);
                Assert.assertEquals(control.getSearchResults().get(0).size(), topK);
            } finally {
                if (target != null && !target.isDone()) {
                    target.cancel(true);
                }
                executor.shutdownNow();
                executor.awaitTermination(120, TimeUnit.SECONDS);
            }
        }
    }
}

package io.milvus.client;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.Rule;
import org.apache.commons.text.RandomStringGenerator;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MilvusGrpcClientTest {

    private MilvusGrpcClient client;

    private RandomStringGenerator generator;

    private String randomTableName;
    private long size;
    private long dimension;
    private TableParam tableParam;
    private TableSchema tableSchema;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws Exception {

        client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                                        .withHost("localhost")
                                        .withPort("19530")
                                        .build();
        client.connect(connectParam);

        generator = new RandomStringGenerator.Builder()
                                            .withinRange('a', 'z').build();
        randomTableName = generator.generate(10);
        size = 100;
        dimension = 128;
        tableParam = new TableParam.Builder(randomTableName).build();
        tableSchema = new TableSchema.Builder(randomTableName, dimension)
                                                    .withIndexFileSize(1024)
                                                    .withMetricType(MetricType.IP)
                                                    .build();
        TableSchemaParam tableSchemaParam = new TableSchemaParam.Builder(tableSchema).build();

        assertTrue(client.createTable(tableSchemaParam).ok());
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws InterruptedException {
        assertTrue(client.dropTable(tableParam).ok());
        client.disconnect();
    }

    @org.junit.jupiter.api.Test
    void connected() {
        assertTrue(client.connected());
    }

    @org.junit.jupiter.api.Test
    void createTable() {
        String invalidTableName = "╯°□°）╯";
        TableSchema invalidTableSchema = new TableSchema.Builder(invalidTableName, dimension).build();
        TableSchemaParam invalidTableSchemaParam = new TableSchemaParam.Builder(invalidTableSchema).withTimeout(20).build();
        Response createTableResponse = client.createTable(invalidTableSchemaParam);
        assertFalse(createTableResponse.ok());
        assertEquals(Response.Status.ILLEGAL_TABLE_NAME, createTableResponse.getStatus());
    }

    @org.junit.jupiter.api.Test
    void hasTable() {
        HasTableResponse hasTableResponse = client.hasTable(tableParam);
        assertTrue(hasTableResponse.getResponse().ok());
    }

    @org.junit.jupiter.api.Test
    void dropTable() {
        String nonExistingTableName = generator.generate(10);
        TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
        Response dropTableResponse = client.dropTable(tableParam);
        assertFalse(dropTableResponse.ok());
        assertEquals(Response.Status.TABLE_NOT_EXISTS, dropTableResponse.getStatus());
    }

    @org.junit.jupiter.api.Test
    void createIndex() {
        Index index = new Index.Builder()
                                .withIndexType(IndexType.IVF_SQ8)
                                .withNList(16384)
                                .build();
        IndexParam indexParam = new IndexParam.Builder(randomTableName)
                                                .withIndex(index)
                                                .build();
        Response createIndexResponse = client.createIndex(indexParam);
        assertTrue(createIndexResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void insert() {
        Random random = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; ++j) {
                vector.add(random.nextFloat());
            }
            vectors.add(vector);
        }
        InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
        InsertResponse insertResponse = client.insert(insertParam);
        assertTrue(insertResponse.getResponse().ok());
        assertEquals(size, insertResponse.getVectorIds().size());
    }

    List<Float> normalize(List<Float> vector) {
        float squareSum = vector.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        vector = vector.stream().map(x -> x / norm).collect(Collectors.toList());
        return vector;
    }

    @org.junit.jupiter.api.Test
    void search() throws InterruptedException {
        Random random = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        List<List<Float>> vectorsToSearch = new ArrayList<>();
        int searchSize = 5;
        for (int i = 0; i < size; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; ++j) {
                vector.add(random.nextFloat());
            }
            if (tableSchema.getMetricType() == MetricType.IP) {
                vector = normalize(vector);
            }
            vectors.add(vector);
            if (i < searchSize) {
                vectorsToSearch.add(vector);
            }
        }
        InsertParam insertParam = new InsertParam.Builder(randomTableName, vectors).build();
        InsertResponse insertResponse = client.insert(insertParam);
        assertTrue(insertResponse.getResponse().ok());
        assertEquals(size, insertResponse.getVectorIds().size());

        TimeUnit.SECONDS.sleep(1);

        List<DateRange> queryRanges = new ArrayList<>();
        Date today = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, -1);
        Date yesterday = c.getTime();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, 1);
        Date tomorrow = c.getTime();
        queryRanges.add(new DateRange(yesterday, tomorrow));
        System.out.println(queryRanges);
        SearchParam searchParam = new SearchParam
                                        .Builder(randomTableName, vectorsToSearch)
                                        .withTopK(1)
                                        .withNProbe(20)
                                        .withDateRanges(queryRanges)
                                        .build();
        SearchResponse searchResponse = client.search(searchParam);
        assertTrue(searchResponse.getResponse().ok());
        System.out.println(searchResponse);
        assertEquals(searchSize, searchResponse.getQueryResultsList().size());
    }

//    @org.junit.jupiter.api.Test
//    void searchInFiles() {
//    }

    @org.junit.jupiter.api.Test
    void describeTable() {
        DescribeTableResponse describeTableResponse = client.describeTable(tableParam);
        assertTrue(describeTableResponse.getResponse().ok());
        assertTrue(describeTableResponse.getTableSchema().isPresent());

        String nonExistingTableName = generator.generate(10);
        TableParam tableParam = new TableParam.Builder(nonExistingTableName).build();
        describeTableResponse = client.describeTable(tableParam);
        assertFalse(describeTableResponse.getResponse().ok());
        assertFalse(describeTableResponse.getTableSchema().isPresent());
    }

    @org.junit.jupiter.api.Test
    void showTables() {
        ShowTablesResponse showTablesResponse = client.showTables();
        assertTrue(showTablesResponse.getResponse().ok());
    }

    @org.junit.jupiter.api.Test
    void serverStatus() {
        Response serverStatusResponse = client.serverStatus();
        assertTrue(serverStatusResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void serverVersion() {
        Response serverVersionResponse = client.serverVersion();
        assertTrue(serverVersionResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void getTableRowCount() throws InterruptedException {
        insert();
        TimeUnit.SECONDS.sleep(1);

        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableParam);
        assertTrue(getTableRowCountResponse.getResponse().ok());
        assertEquals(size, getTableRowCountResponse.getTableRowCount());
    }

    @org.junit.jupiter.api.Test
    void deleteByRange() {
        Date today = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, -1);
        Date yesterday = c.getTime();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, 1);
        Date tomorrow = c.getTime();

        DeleteByRangeParam deleteByRangeParam = new DeleteByRangeParam.Builder(
                new DateRange(yesterday, tomorrow), randomTableName).build();
        Response deleteByRangeResponse = client.deleteByRange(deleteByRangeParam);
        assertTrue(deleteByRangeResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void preloadTable() {
        Response preloadTableResponse = client.preloadTable(tableParam);
        assertTrue(preloadTableResponse.ok());
    }

    @org.junit.jupiter.api.Test
    void describeIndex() {
        DescribeIndexResponse describeIndexResponse = client.describeIndex(tableParam);
        assertTrue(describeIndexResponse.getResponse().ok());
        assertTrue(describeIndexResponse.getIndex().isPresent());
    }

    @org.junit.jupiter.api.Test
    void dropIndex() {
        Response dropIndexResponse = client.dropIndex(tableParam);
        assertTrue(dropIndexResponse.ok());
    }
}
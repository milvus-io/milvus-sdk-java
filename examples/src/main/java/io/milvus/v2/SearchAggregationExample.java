/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SearchAggregationExample {
    private static final String COLLECTION_NAME = "java_sdk_example_search_aggregation_v2";
    private static final String ID_FIELD = "id";
    private static final String CATEGORY_FIELD = "category";
    private static final String BRAND_FIELD = "brand";
    private static final String COLOR_FIELD = "color";
    private static final String SKU_FIELD = "sku";
    private static final String PRICE_FIELD = "price";
    private static final String RATING_FIELD = "rating";
    private static final String IN_STOCK_FIELD = "in_stock";
    private static final String META_FIELD = "meta";
    private static final String EMBEDDING_FIELD = "embedding";
    private static final int DIM = 8;
    private static final int NUM_ENTITIES = 2000;
    private static final int NQ = 3;
    private static final long SEARCH_LIMIT = 10L;
    private static final int INDENT_STEP = 4;
    private static final String BUCKET_MARKER = "●";
    private static final String SUB_MARKER = "└─";

    private static final String[] CATEGORIES = {"electronics", "books", "clothing", "home", "toys"};
    private static final String[] BRANDS = {"BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "BrandG", "BrandH"};
    private static final String[] COLORS = {"red", "blue", "green", "black"};
    private static final String[] SKUS = {
            "sku_000", "sku_001", "sku_002", "sku_003", "sku_004",
            "sku_005", "sku_006", "sku_007", "sku_008", "sku_009",
            "sku_010", "sku_011", "sku_012", "sku_013", "sku_014",
            "sku_015", "sku_016", "sku_017", "sku_018", "sku_019"
    };

    private static final Random RANDOM = new Random(19530L);
    private static final Gson GSON = new Gson();

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            buildCollection(client);

            case1SingleField(client);
            case2CompositeKeyWithMetrics(client);
            // case3JsonField(client);
            case4TwoLevelNested(client);
            case5ThreeLevelNested(client);
        } finally {
            client.close();
        }
    }

    private static void buildCollection(MilvusClientV2 client) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CATEGORY_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(BRAND_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(COLOR_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(16)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(SKU_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(16)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(PRICE_FIELD)
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(RATING_FIELD)
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(IN_STOCK_FIELD)
                .dataType(DataType.Bool)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(META_FIELD)
                .dataType(DataType.JSON)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(EMBEDDING_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(EMBEDDING_FIELD)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(Collections.singletonMap("nlist", 128))
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.println("Collection created");

        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < NUM_ENTITIES; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.addProperty(CATEGORY_FIELD, CATEGORIES[i % CATEGORIES.length]);
            row.addProperty(BRAND_FIELD, BRANDS[i % BRANDS.length]);
            row.addProperty(COLOR_FIELD, COLORS[(i / BRANDS.length) % COLORS.length]);
            row.addProperty(SKU_FIELD, SKUS[(i / (BRANDS.length * COLORS.length)) % SKUS.length]);
            row.addProperty(PRICE_FIELD, 10.0 + (i % 500));
            row.addProperty(RATING_FIELD, (i % 50) / 10.0);
            row.addProperty(IN_STOCK_FIELD, i % 3 != 0);

            JsonObject meta = new JsonObject();
            meta.addProperty("subcategory", "sub_" + (i % 8));
            meta.addProperty("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "apac");
            row.add(META_FIELD, meta);
            row.add(EMBEDDING_FIELD, GSON.toJsonTree(randomVector()));
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        QueryResp countResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted%n",
                (long) countResp.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void case1SingleField(MilvusClientV2 client) {
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors())
                .annsField(EMBEDDING_FIELD)
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(ID_FIELD, BRAND_FIELD, PRICE_FIELD))
                .searchAggregation(SearchAggregation.builder()
                        .fields(Collections.singletonList(BRAND_FIELD))
                        .size(4)
                        .topHits(TopHitsSpec.builder()
                                .size(3)
                                .build())
                        .build())
                .build());
        printBuckets("Case 1: single field grouping", resp.getAggregationBuckets());
    }

    private static void case2CompositeKeyWithMetrics(MilvusClientV2 client) {
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors())
                .annsField(EMBEDDING_FIELD)
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(ID_FIELD, BRAND_FIELD, COLOR_FIELD, PRICE_FIELD, RATING_FIELD))
                .searchAggregation(SearchAggregation.builder()
                        .fields(Arrays.asList(BRAND_FIELD, COLOR_FIELD))
                        .size(5)
                        .addMetric("avg_price", MetricSpec.builder()
                                .op(MetricOps.AVG)
                                .fieldName(PRICE_FIELD)
                                .build())
                        .addMetric("doc_count", MetricSpec.builder()
                                .op(MetricOps.COUNT)
                                .fieldName("*")
                                .build())
                        .addOrder(OrderSpec.builder()
                                .key("avg_price")
                                .direction(AggDirection.DESC)
                                .build())
                        .topHits(TopHitsSpec.builder()
                                .size(2)
                                .addSort(SortSpec.builder()
                                        .fieldName(RATING_FIELD)
                                        .direction(AggDirection.DESC)
                                        .build())
                                .build())
                        .build())
                .build());
        printBuckets("Case 2: composite key + metrics + ordering", resp.getAggregationBuckets());
    }

    private static void case3JsonField(MilvusClientV2 client) {
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors())
                .annsField(EMBEDDING_FIELD)
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(ID_FIELD, CATEGORY_FIELD))
                .searchAggregation(SearchAggregation.builder()
                        .fields(Collections.singletonList("meta['region']"))
                        .size(4)
                        .addMetric("avg_score", MetricSpec.builder()
                                .op(MetricOps.AVG)
                                .fieldName("_score")
                                .build())
                        .addOrder(OrderSpec.builder()
                                .key("avg_score")
                                .direction(AggDirection.DESC)
                                .build())
                        .topHits(TopHitsSpec.builder()
                                .size(2)
                                .build())
                        .build())
                .build());
        printBuckets("Case 3: JSON path grouping", resp.getAggregationBuckets());
    }

    private static void case4TwoLevelNested(MilvusClientV2 client) {
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors())
                .annsField(EMBEDDING_FIELD)
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(ID_FIELD, CATEGORY_FIELD, BRAND_FIELD, PRICE_FIELD, RATING_FIELD))
                .filter(IN_STOCK_FIELD + " == true")
                .searchAggregation(SearchAggregation.builder()
                        .fields(Collections.singletonList(CATEGORY_FIELD))
                        .size(3)
                        .addMetric("total_revenue", MetricSpec.builder()
                                .op(MetricOps.SUM)
                                .fieldName(PRICE_FIELD)
                                .build())
                        .addMetric("item_count", MetricSpec.builder()
                                .op(MetricOps.COUNT)
                                .fieldName("*")
                                .build())
                        .addOrder(OrderSpec.builder()
                                .key("total_revenue")
                                .direction(AggDirection.DESC)
                                .build())
                        .topHits(TopHitsSpec.builder()
                                .size(2)
                                .addSort(SortSpec.builder()
                                        .fieldName("_score")
                                        .direction(AggDirection.DESC)
                                        .build())
                                .build())
                        .subAggregation(SearchAggregation.builder()
                                .fields(Collections.singletonList(BRAND_FIELD))
                                .size(2)
                                .addMetric("avg_rating", MetricSpec.builder()
                                        .op(MetricOps.AVG)
                                        .fieldName(RATING_FIELD)
                                        .build())
                                .addOrder(OrderSpec.builder()
                                        .key("avg_rating")
                                        .direction(AggDirection.DESC)
                                        .build())
                                .topHits(TopHitsSpec.builder()
                                        .size(2)
                                        .addSort(SortSpec.builder()
                                                .fieldName(PRICE_FIELD)
                                                .direction(AggDirection.ASC)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
        printBuckets("Case 4: two-level nested", resp.getAggregationBuckets());
    }

    private static void case5ThreeLevelNested(MilvusClientV2 client) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("nprobe", 16);
        params.put("metric_type", "L2");

        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryVectors())
                .annsField(EMBEDDING_FIELD)
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(ID_FIELD, CATEGORY_FIELD, BRAND_FIELD, SKU_FIELD, COLOR_FIELD, PRICE_FIELD, RATING_FIELD))
                .searchParams(params)
                .searchAggregation(SearchAggregation.builder()
                        .fields(Collections.singletonList(CATEGORY_FIELD))
                        .size(3)
                        .addMetric("total_revenue", MetricSpec.builder()
                                .op(MetricOps.SUM)
                                .fieldName(PRICE_FIELD)
                                .build())
                        .addMetric("item_count", MetricSpec.builder()
                                .op(MetricOps.COUNT)
                                .fieldName("*")
                                .build())
                        .addOrder(OrderSpec.builder()
                                .key("total_revenue")
                                .direction(AggDirection.DESC)
                                .build())
                        .topHits(TopHitsSpec.builder()
                                .size(2)
                                .addSort(SortSpec.builder()
                                        .fieldName("_score")
                                        .direction(AggDirection.ASC)
                                        .build())
                                .build())
                        .subAggregation(SearchAggregation.builder()
                                .fields(Collections.singletonList(BRAND_FIELD))
                                .size(3)
                                .addMetric("brand_revenue", MetricSpec.builder()
                                        .op(MetricOps.SUM)
                                        .fieldName(PRICE_FIELD)
                                        .build())
                                .addMetric("avg_rating", MetricSpec.builder()
                                        .op(MetricOps.AVG)
                                        .fieldName(RATING_FIELD)
                                        .build())
                                .addOrder(OrderSpec.builder()
                                        .key("brand_revenue")
                                        .direction(AggDirection.DESC)
                                        .build())
                                .topHits(TopHitsSpec.builder()
                                        .size(2)
                                        .addSort(SortSpec.builder()
                                                .fieldName(RATING_FIELD)
                                                .direction(AggDirection.DESC)
                                                .build())
                                        .build())
                                .subAggregation(SearchAggregation.builder()
                                        .fields(Arrays.asList(SKU_FIELD, COLOR_FIELD))
                                        .size(3)
                                        .addMetric("min_price", MetricSpec.builder()
                                                .op(MetricOps.MIN)
                                                .fieldName(PRICE_FIELD)
                                                .build())
                                        .addMetric("item_count", MetricSpec.builder()
                                                .op(MetricOps.COUNT)
                                                .fieldName("*")
                                                .build())
                                        .addOrder(OrderSpec.builder()
                                                .key("min_price")
                                                .direction(AggDirection.ASC)
                                                .build())
                                        .topHits(TopHitsSpec.builder()
                                                .size(2)
                                                .addSort(SortSpec.builder()
                                                        .fieldName(PRICE_FIELD)
                                                        .direction(AggDirection.ASC)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());
        printBuckets("Case 5: three-level nested", resp.getAggregationBuckets());
    }

    private static List<BaseVector> queryVectors() {
        List<BaseVector> vectors = new ArrayList<>();
        for (int i = 0; i < NQ; i++) {
            vectors.add(new FloatVec(randomVector()));
        }
        return vectors;
    }

    private static List<Float> randomVector() {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < DIM; i++) {
            vector.add(RANDOM.nextFloat());
        }
        return vector;
    }

    private static void printBuckets(String label, List<List<AggregationBucket>> perQueryBuckets) {
        System.out.println("\n=== " + label + " ===");
        for (int i = 0; i < perQueryBuckets.size(); i++) {
            List<AggregationBucket> buckets = perQueryBuckets.get(i);
            System.out.printf("--- nq[%d] (%d buckets) ---%n", i, buckets.size());
            printBucketsRecursive(buckets, 0);
        }
    }

    private static void printBucketsRecursive(List<AggregationBucket> buckets, int depth) {
        String bucketPad = repeatSpace(depth * INDENT_STEP);
        String labelPad = repeatSpace(depth * INDENT_STEP + INDENT_STEP / 2);
        String hitPad = repeatSpace(depth * INDENT_STEP + INDENT_STEP);
        String marker = depth == 0 ? BUCKET_MARKER : SUB_MARKER;

        for (AggregationBucket bucket : buckets) {
            StringBuilder head = new StringBuilder();
            head.append(bucketPad)
                    .append(marker)
                    .append(" [L")
                    .append(depth)
                    .append("] key[")
                    .append(formatKey(bucket.getKey()))
                    .append("] count=")
                    .append(bucket.getCount());
            if (bucket.getMetrics() != null && !bucket.getMetrics().isEmpty()) {
                head.append("  metrics=").append(bucket.getMetrics());
            }
            System.out.println(head.toString());

            if (bucket.getHits() != null && !bucket.getHits().isEmpty()) {
                System.out.println(labelPad + "top_hits:");
                for (AggregationHit hit : bucket.getHits()) {
                    System.out.printf("%s· pk=%s score=%.4f  fields=%s%n",
                            hitPad, hit.getId(), hit.getScore(), hit.getFields());
                }
            }
            if (bucket.getSubGroups() != null && !bucket.getSubGroups().isEmpty()) {
                System.out.println(labelPad + "sub_groups:");
                printBucketsRecursive(bucket.getSubGroups(), depth + 1);
            }
        }
    }

    private static String formatKey(List<AggregationBucket.KeyEntry> keyEntries) {
        List<String> parts = new ArrayList<>();
        for (AggregationBucket.KeyEntry keyEntry : keyEntries) {
            String name = keyEntry.getFieldName() != null && !keyEntry.getFieldName().isEmpty()
                    ? keyEntry.getFieldName()
                    : String.valueOf(keyEntry.getFieldId());
            parts.add(name + "=" + String.valueOf(keyEntry.getValue()));
        }
        return String.join(", ", parts);
    }

    private static String repeatSpace(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(' ');
        }
        return builder.toString();
    }
}

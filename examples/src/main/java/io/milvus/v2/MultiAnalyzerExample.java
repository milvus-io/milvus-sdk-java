package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
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
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class MultiAnalyzerExample {
    private static final String COLLECTION_NAME = "java_sdk_example_multi_analyzer_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final String TEXT_FIELD = "text";
    private static final String LANGUAGE_FIELD = "language";

    private static void buildCollection(MilvusClientV2 client) {
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());

        // apply multiple analyzers to the text field, so that insert data can specify different tokenizers for each row.
        // in this example, texts are written by multiple languages, so we use multiple analyzers to handle different texts.
        // to use multiple analyzers, there must be a field to specify the language type, in this example, the "language"
        // field is used for this purpose. multiple analyzers is optional, no need to set it if the data only contains one
        // language, no need to add the "language" field if the data only contains one language.
        // tokenizer:
        //  english: https://milvus.io/docs/english-analyzer.md
        //  chinese: https://milvus.io/docs/chinese-analyzer.md
        //  lindera: https://milvus.io/docs/lindera-tokenizer.md
        //  icu: https://milvus.io/docs/icu-tokenizer.md
        // filter:
        //  lowercase: https://milvus.io/docs/lowercase-filter.md
        //  removepunct: https://milvus.io/docs/removepunct-filter.md
        //  asciifolding: https://milvus.io/docs/ascii-folding-filter.md
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("analyzers", new HashMap<String, Object>() {{
            put("english", new HashMap<String, Object>() {{
                put("type", "english");
            }});
            put("chinese", new HashMap<String, Object>() {{
                put("tokenizer", "jieba");
                put("filter", Arrays.asList("lowercase", "removepunct"));
            }});
            put("japanese", new HashMap<String, Object>() {{
                put("tokenizer", new HashMap<String, Object>() {{
                    put("type", "lindera");
                    put("dict_kind", "ipadic");
                }});
            }});
            put("default", new HashMap<String, Object>() {{
                put("tokenizer", "icu");
                put("filter", Arrays.asList("lowercase", "removepunct", "asciifolding"));
            }});
        }});
        analyzerParams.put("by_field", "language");
        analyzerParams.put("alias", new HashMap<String, Object>() {{
            put("cn", "chinese");
            put("en", "english");
            put("jap", "japanese");
        }});

        schema.addField(AddFieldReq.builder()
                .fieldName(TEXT_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true) // must enable this if you use Function
                .multiAnalyzerParams(analyzerParams)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(LANGUAGE_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());

        // With this function, milvus will convert the strings of "text" field to sparse vectors of "vector" field
        // by built-in tokenizer and analyzer
        // Read the link for more info: https://milvus.io/docs/full-text-search.md
        schema.addFunction(CreateCollectionReq.Function.builder()
                .functionType(FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList(TEXT_FIELD))
                .outputFieldNames(Collections.singletonList(VECTOR_FIELD))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25) // to use full text search, metric type must be "BM25"
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                gson.fromJson("{\"language\": \"en\", \"text\": \"Milvus is an open-source vector database\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"AI applications help people better life\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"Will the electric car replace gas-powered car?\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"LangChain is a composable framework to build with LLMs. Milvus is integrated into LangChain.\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"RAG is the process of optimizing the output of a large language model\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"Newton is one of the greatest scientist of human history\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"Metric type L2 is Euclidean distance\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"Embeddings represent real-world objects, like words, images, or videos, in a form that computers can process.\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"The moon is 384,400 km distance away from earth\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"en\", \"text\": \"Milvus supports L2 distance and IP similarity for float vector.\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"cn\", \"text\": \"人工智能正在改变技术领域\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"cn\", \"text\": \"机器学习模型需要大型数据集\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"cn\", \"text\": \"Milvus 是一个高性能、可扩展的向量数据库！\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"jap\", \"text\": \"Milvusの新機能をご確認くださいこのページでは\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"jap\", \"text\": \"非構造化データやマルチモーダルデータを構造化されたコレクションに整理することができます\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"jap\", \"text\": \"主な利点はデータアクセスパターンにある\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"default\", \"text\": \"토큰화 도구는 소프트웨어 국제화를 위한 핵심 도구를 제공하는\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"default\", \"text\": \"Les applications qui suivent le temps à travers les régions\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"default\", \"text\": \"Sin embargo, esto puede aumentar la complejidad de las consultas y de la gestión\"}", JsonObject.class),
                gson.fromJson("{\"language\": \"default\", \"text\": \"المثال، يوضح الرمز التالي كيفية إضافة عامل تصفية الحقل القياسي إلى بحث متجه\"}", JsonObject.class)
        );

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows in collection\n", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void searchByText(MilvusClientV2 client, String text, String language) {
        System.out.printf("\n===============================Language:%s==============================%n", language);
        System.out.println("Text: " + text);
        // The text is tokenized inside server and turned into a sparse embedding to compare with the vector field
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("analyzer_name", language);
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new EmbeddedText(text)))
                .limit(5)
                .searchParams(searchParams)
                .outputFields(Arrays.asList(TEXT_FIELD, LANGUAGE_FIELD))
                .build());
        System.out.println("Search results:");
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        buildCollection(client);

        // Query by filtering expression
        searchByText(client, "Milvus vector database", "english");
        searchByText(client, "人工智能与机器学习", "chinese");
        searchByText(client, "非構造化データ", "japanese");
        searchByText(client, "Gestion des applications", "default");

        client.close();
    }
}

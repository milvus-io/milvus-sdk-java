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
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.ranker.BoostRanker;
import io.milvus.v2.service.vector.request.ranker.DecayRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class RankerExample {
    private static final MilvusClientV2 client;

    static {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        client = new MilvusClientV2(config);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_ranker_v2";
    private static final String NAME_FIELD = "name";
    private static final String BIRTH_YEAR_FIELD = "birth_year";
    private static final String LIFESPAN_FIELD = "lifespan";
    private static final String SPARSE_VECTOR_FIELD = "sparse_vector";

    private static class Person {
        public String name;
        public int fromYear;
        public int toYear;
        public Person(String name, int from, int to) {
            this.name = name;
            this.fromYear = from;
            this.toYear = to;
        }
    }

    private static List<Person> genData() {
        List<Person> persons = new ArrayList<>();
        persons.add(new Person("Isaac Newton", 1643, 1727));
        persons.add(new Person("Albert Einstein", 1879, 1955));
        persons.add(new Person("Marie Curie", 1867, 1934));
        persons.add(new Person("Charles Darwin", 1809, 1882));
        persons.add(new Person("Galileo Galilei", 1564, 1642));
        persons.add(new Person("Nikola Tesla", 1856, 1943));
        persons.add(new Person("James Clerk Maxwell", 1831, 1879));
        persons.add(new Person("Thomas Edison", 1847, 1931));
        persons.add(new Person("Alexander Fleming", 1881, 1955));
        persons.add(new Person("Louis Pasteur", 1822, 1895));
        persons.add(new Person("Werner Heisenberg", 1901, 1976));
        persons.add(new Person("Stephen Hawking", 1942, 2018));
        persons.add(new Person("Dmitri Mendeleev", 1834, 1907));
        persons.add(new Person("Max Planck", 1858, 1947));
        persons.add(new Person("Niels Bohr", 1885, 1962));
        persons.add(new Person("Richard Feynman", 1918, 1988));
        persons.add(new Person("Carl Sagan", 1934, 1996));
        persons.add(new Person("Francis Crick", 1916, 2004));
        persons.add(new Person("Rosalind Franklin", 1920, 1958));
        persons.add(new Person("Edwin Hubble", 1889, 1953));
        persons.add(new Person("Linus Pauling", 1901, 1994));
        persons.add(new Person("Alan Turing", 1912, 1954));
        persons.add(new Person("Guglielmo Marconi", 1874, 1937));
        persons.add(new Person("Michael Faraday", 1791, 1867));
        persons.add(new Person("Enrico Fermi", 1901, 1954));
        persons.add(new Person("Johannes Kepler", 1571, 1630));
        persons.add(new Person("Edwin Schrödinger", 1887, 1961));
        persons.add(new Person("Werner von Braun", 1912, 1977));
        persons.add(new Person("Albert Hofmann", 1906, 2008));
        persons.add(new Person("Robert Oppenheimer", 1904, 1967));
        persons.add(new Person("Edwin Land", 1909, 1991));
        persons.add(new Person("Rachel Carson", 1907, 1964));
        persons.add(new Person("Ernest Rutherford", 1871, 1937));
        persons.add(new Person("Hans Geiger", 1882, 1945));
        persons.add(new Person("John Bardeen", 1908, 1991));
        persons.add(new Person("George Washington Carver", 1864, 1943));
        return persons;
    }

    private static void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(NAME_FIELD)
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .maxLength(1024)
                .enableAnalyzer(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(BIRTH_YEAR_FIELD)
                .dataType(DataType.Int64)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(LIFESPAN_FIELD)
                .dataType(DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(SPARSE_VECTOR_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());

        collectionSchema.addFunction(CreateCollectionReq.Function.builder()
                .functionType(FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList(NAME_FIELD))
                .outputFieldNames(Collections.singletonList(SPARSE_VECTOR_FIELD))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(SPARSE_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");
    }

    private static void insertData() {
        List<JsonObject> rows = new ArrayList<>();
        List<Person> data = genData();
        for (Person person : data) {
            JsonObject row = new JsonObject();
            row.addProperty(NAME_FIELD, person.name);
            row.addProperty(BIRTH_YEAR_FIELD, person.fromYear);
            row.addProperty(LIFESPAN_FIELD, person.toYear - person.fromYear);
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        printRowCount();
    }

    private static void printRowCount() {
        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void dropCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");
    }

    private static void searchWithRanker(String text, CreateCollectionReq.Function rankerFunction) {
        System.out.println("\n=============================================================");
        SearchReq.SearchReqBuilder builder = SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new EmbeddedText(text)))
                .limit(100)
                .outputFields(Arrays.asList(BIRTH_YEAR_FIELD, LIFESPAN_FIELD));

        if (rankerFunction != null) {
            builder.functionScore(FunctionScore.builder()
                    .addFunction(rankerFunction)
                    .build());
            System.out.printf("Search text '%s' with ranker '%s'\n\n", text, rankerFunction.getName());
        } else {
            System.out.printf("Search text '%s' without ranker\n\n", text);
        }

        // The text is tokenized inside server and turned into a sparse embedding to compare with the vector field
        SearchResp searchResp = client.search(builder.build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    private static void searchWithoutRanker(String text) {
        searchWithRanker(text, null);
    }

    public static void main(String[] args) {
        createCollection();
        insertData();

        // Search scientists with name or surname
        String scientists = "Albert, Charles, Darwin and Edwin";
        searchWithoutRanker(scientists);

        // Search scientists with name or surname
        // Rerank the results by linear decay, the scores are rearranged according to the birth years
        // Read the doc for more info: https://milvus.io/docs/decay-ranker-overview.md
        // The scientist whose birth year is close to 1900 will get a high score
        DecayRanker decay = DecayRanker.builder()
                .name("birth_year_linear_decay")
                .inputFieldNames(Collections.singletonList(BIRTH_YEAR_FIELD))
                .function("linear")
                .origin(1900)
                .scale(50)
                .offset(0)
                .decay(0.1)
                .build();
        searchWithRanker(scientists, decay);

        // Search scientists with name or surname
        // Rerank the results by boost, the scores are rearranged according to the birth years
        // Read the doc for more info: https://milvus.io/docs/boost-ranker.md
        // The scientist whose lifespan is between 60 and 70 will get a high score
        BoostRanker boost = BoostRanker.builder()
                .name("boost_on_lifespan")
                .filter(String.format("%s > 60 and %s < 70", LIFESPAN_FIELD, LIFESPAN_FIELD))
                .weight(5.0f)
                .build();
        searchWithRanker(scientists, boost);

        dropCollection();
        client.close();
    }
}

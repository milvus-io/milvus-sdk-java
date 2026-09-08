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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.index.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class AnalyzerDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testRunAnalyzer() {
        List<String> texts = new ArrayList<>();
        texts.add("Analyzers (tokenizers) for multi languages");
        texts.add("2.5 to take advantage of enhancements and fixes!");

        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        Map<String, Object> lowercaseFilter = new HashMap<>();
        lowercaseFilter.put("type", "stop");
        lowercaseFilter.put("stop_words", Arrays.asList("to", "of", "for", "the"));
        analyzerParams.put("filter", Arrays.asList("lowercase", lowercaseFilter));

        RunAnalyzerResp resp = client.runAnalyzer(RunAnalyzerReq.builder()
                .texts(texts)
                .analyzerParams(analyzerParams)
                .withDetail(true)
                .withHash(true)
                .build());

        List<RunAnalyzerResp.AnalyzerResult> results = resp.getResults();
        Assertions.assertEquals(texts.size(), results.size());

        {
            List<String> tokens1 = Arrays.asList("analyzers", "tokenizers", "multi", "languages");
            List<Long> startOffset1 = Arrays.asList(0L, 11L, 27L, 33L);
            List<Long> endOffset1 = Arrays.asList(9L, 21L, 32L, 42L);
            List<Long> position1 = Arrays.asList(0L, 1L, 3L, 4L);
            List<Long> positionLen1 = Arrays.asList(1L, 1L, 1L, 1L);
            List<Long> hash1 = Arrays.asList(1356745679L, 4089107865L, 3314631429L, 2698072953L);

            List<RunAnalyzerResp.AnalyzerToken> outTokens1 = results.get(0).getTokens();
            System.out.printf("%d tokens%n", outTokens1.size());
            Assertions.assertEquals(tokens1.size(), outTokens1.size());
            for (int i = 0; i < outTokens1.size(); i++) {
                RunAnalyzerResp.AnalyzerToken token = outTokens1.get(i);
                System.out.println(token);
                Assertions.assertEquals(tokens1.get(i), token.getToken());
                Assertions.assertEquals(startOffset1.get(i), token.getStartOffset());
                Assertions.assertEquals(endOffset1.get(i), token.getEndOffset());
                Assertions.assertEquals(position1.get(i), token.getPosition());
                Assertions.assertEquals(positionLen1.get(i), token.getPositionLength());
                Assertions.assertEquals(hash1.get(i), token.getHash());
            }
        }

        {
            List<String> tokens2 = Arrays.asList("2", "5", "take", "advantage", "enhancements", "and", "fixes");
            List<Long> startOffset2 = Arrays.asList(0L, 2L, 7L, 12L, 25L, 38L, 42L);
            List<Long> endOffset2 = Arrays.asList(1L, 3L, 11L, 21L, 37L, 41L, 47L);
            List<Long> position2 = Arrays.asList(0L, 1L, 3L, 4L, 6L, 7L, 8L);
            List<Long> positionLen2 = Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L, 1L);
            List<Long> hash2 = Arrays.asList(450215437L, 2226203566L, 937258619L, 697180577L, 3403941281L, 133536621L, 488262645L);

            List<RunAnalyzerResp.AnalyzerToken> outTokens2 = results.get(1).getTokens();
            System.out.printf("%d tokens%n", outTokens2.size());
            Assertions.assertEquals(tokens2.size(), outTokens2.size());
            for (int i = 0; i < outTokens2.size(); i++) {
                RunAnalyzerResp.AnalyzerToken token = outTokens2.get(i);
                System.out.println(token);
                Assertions.assertEquals(tokens2.get(i), token.getToken());
                Assertions.assertEquals(startOffset2.get(i), token.getStartOffset());
                Assertions.assertEquals(endOffset2.get(i), token.getEndOffset());
                Assertions.assertEquals(position2.get(i), token.getPosition());
                Assertions.assertEquals(positionLen2.get(i), token.getPositionLength());
                Assertions.assertEquals(hash2.get(i), token.getHash());
            }
        }
    }

    @Test
    void testRunAnalyzerCollectionMode() {
        String collectionName = generator.generate(10);
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .enableAnalyzer(true)
                .analyzerParams(analyzerParams)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(collectionSchema)
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        try {
            RunAnalyzerResp resp = client.runAnalyzer(RunAnalyzerReq.builder()
                    .collectionName(collectionName)
                    .fieldName("text")
                    .texts(Arrays.asList("Analyzers tokenizers for multi languages"))
                    .build());
            List<RunAnalyzerResp.AnalyzerResult> results = resp.getResults();
            Assertions.assertEquals(1, results.size());
            Assertions.assertFalse(results.get(0).getTokens().isEmpty());
        } finally {
            client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

}

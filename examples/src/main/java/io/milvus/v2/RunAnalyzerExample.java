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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.RunAnalyzerReq;
import io.milvus.v2.service.vector.response.RunAnalyzerResp;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates the runAnalyzer API on {@link MilvusClientV2}: the analyzer configuration is
 * passed as analyzer parameters (or a named analyzer) and the server returns the tokenized
 * result for the given texts, optionally with token offsets and hashes.
 *
 * <p>Prerequisites: a running Milvus at {@code http://localhost:19530} (the default in
 * {@link ConnectConfig} below).</p>
 */
public class RunAnalyzerExample {

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Server version: " + client.getServerVersion());

        try {
            runAnalyzer(client, standardAnalyzerWithStopWords(), "Milvus supports L2 distance and IP similarity for float vector.");
            runAnalyzer(client, jiebaAnalyzer(), "Milvus 是 LF AI & Data Foundation 下的一个开源项目，以 Apache 2.0 许可发布。");
            runAnalyzer(client, linderaAnalyzer(), "東京スカイツリーの最寄り駅はとうきょうスカイツリー駅で");
            runAnalyzer(client, icuAnalyzer(), "Привет! Как дела?");
        } finally {
            client.close(5);
        }
    }

    /**
     * Standard tokenizer with a stop-word filter.
     */
    private static Map<String, Object> standardAnalyzerWithStopWords() {
        Map<String, Object> filter = new HashMap<>();
        filter.put("type", "stop");
        filter.put("stop_words", Arrays.asList("and", "for"));

        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "standard");
        analyzerParams.put("filter", Collections.singletonList(filter));
        return analyzerParams;
    }

    /**
     * Jieba tokenizer with the cnalphanumonly filter.
     */
    private static Map<String, Object> jiebaAnalyzer() {
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "jieba");
        analyzerParams.put("filter", Collections.singletonList("cnalphanumonly"));
        return analyzerParams;
    }

    /**
     * Lindera tokenizer with the ipadic dictionary.
     */
    private static Map<String, Object> linderaAnalyzer() {
        Map<String, Object> tokenizer = new HashMap<>();
        tokenizer.put("type", "lindera");
        tokenizer.put("dict_kind", "ipadic");

        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", tokenizer);
        return analyzerParams;
    }

    /**
     * ICU tokenizer.
     */
    private static Map<String, Object> icuAnalyzer() {
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("tokenizer", "icu");
        return analyzerParams;
    }

    private static void runAnalyzer(MilvusClientV2 client, Map<String, Object> analyzerParams, String text) {
        System.out.println("========== runAnalyzer() ==========");
        System.out.println("Analyzer params: " + analyzerParams);
        System.out.println("Text: " + text);

        RunAnalyzerResp resp = client.runAnalyzer(RunAnalyzerReq.builder()
                .texts(Collections.singletonList(text))
                .analyzerParams(analyzerParams)
                .withDetail(Boolean.TRUE)
                .withHash(Boolean.TRUE)
                .build());
        printAnalyzerResults(resp);
    }

    private static void printAnalyzerResults(RunAnalyzerResp resp) {
        for (RunAnalyzerResp.AnalyzerResult result : resp.getResults()) {
            System.out.println("  ------------------------------");
            for (RunAnalyzerResp.AnalyzerToken token : result.getTokens()) {
                System.out.printf("  {token: %s, start: %d, end: %d, position: %d, position_len: %d, hash: %s}%n",
                        token.getToken(),
                        token.getStartOffset(),
                        token.getEndOffset(),
                        token.getPosition(),
                        token.getPositionLength(),
                        token.getHash());
            }
            System.out.println("  ------------------------------");
        }
    }
}

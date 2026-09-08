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

package io.milvus.unit.common.utils;

import io.milvus.common.utils.URLParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class URLParserTest {

    @Test
    void parsesHttpsUrlWithPortAndDatabase() {
        URLParser parser = new URLParser("https://milvus.example.com:19531/db1");
        Assertions.assertEquals("milvus.example.com", parser.getHostname());
        Assertions.assertEquals(19531, parser.getPort());
        Assertions.assertEquals("db1", parser.getDatabase());
        Assertions.assertTrue(parser.isSecure());
    }

    @Test
    void defaultsPortTo19530() {
        URLParser parser = new URLParser("http://milvus.example.com");
        Assertions.assertEquals("milvus.example.com", parser.getHostname());
        Assertions.assertEquals(19530, parser.getPort());
        Assertions.assertFalse(parser.isSecure());
        Assertions.assertNull(parser.getDatabase());
    }

    @Test
    void httpsWithoutPortIsSecureAndDefaultsPort() {
        URLParser parser = new URLParser("https://milvus.example.com/db");
        Assertions.assertEquals(19530, parser.getPort());
        Assertions.assertTrue(parser.isSecure());
        Assertions.assertEquals("db", parser.getDatabase());
    }

    @Test
    void rootPathMeansNoDatabase() {
        URLParser parser = new URLParser("http://host:19530/");
        Assertions.assertEquals("host", parser.getHostname());
        Assertions.assertNull(parser.getDatabase());
    }

    @Test
    void emptyPathMeansNoDatabase() {
        URLParser parser = new URLParser("http://host:19530");
        Assertions.assertNull(parser.getDatabase());
    }

    @Test
    void userInfoIsIgnored() {
        URLParser parser = new URLParser("http://user:secret@host:19530/db");
        Assertions.assertEquals("host", parser.getHostname());
        Assertions.assertEquals(19530, parser.getPort());
        Assertions.assertEquals("db", parser.getDatabase());
    }

    @Test
    void nestedDatabasePathIsKept() {
        URLParser parser = new URLParser("http://host:19530/nested/db");
        Assertions.assertEquals("nested/db", parser.getDatabase());
    }

    @Test
    void missingHostnameRejected() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new URLParser("https://"));
        Assertions.assertTrue(ex.getMessage().startsWith("Invalid url:"));
        Assertions.assertTrue(ex.getMessage().contains("https://"));
    }

    @Test
    void invalidUriRejected() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new URLParser("ht!tp://host"));
        Assertions.assertTrue(ex.getMessage().startsWith("Invalid url:"));
        Assertions.assertTrue(ex.getMessage().contains("ht!tp://host"));
    }

    @Test
    void toStringContainsFields() {
        String s = new URLParser("http://host:19530/db").toString();
        Assertions.assertTrue(s.contains("hostname='host'"));
        Assertions.assertTrue(s.contains("port=19530"));
        Assertions.assertTrue(s.contains("database='db'"));
        Assertions.assertTrue(s.contains("secure=false"));
    }
}

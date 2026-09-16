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
    void parseHostnamePortAndDatabase() {
        URLParser httpsWithPort = new URLParser("https://milvus.example.com:19531/db1");
        Assertions.assertEquals("milvus.example.com", httpsWithPort.getHostname());
        Assertions.assertEquals(19531, httpsWithPort.getPort());
        Assertions.assertEquals("db1", httpsWithPort.getDatabase());
        Assertions.assertTrue(httpsWithPort.isSecure());

        URLParser httpDefaultPort = new URLParser("http://milvus.example.com");
        Assertions.assertEquals("milvus.example.com", httpDefaultPort.getHostname());
        Assertions.assertEquals(19530, httpDefaultPort.getPort());
        Assertions.assertFalse(httpDefaultPort.isSecure());
        Assertions.assertNull(httpDefaultPort.getDatabase());

        URLParser httpsNoPort = new URLParser("https://milvus.example.com/db");
        Assertions.assertEquals(19530, httpsNoPort.getPort());
        Assertions.assertTrue(httpsNoPort.isSecure());
        Assertions.assertEquals("db", httpsNoPort.getDatabase());

        URLParser userInfoIgnored = new URLParser("http://user:secret@host:19530/db");
        Assertions.assertEquals("host", userInfoIgnored.getHostname());
        Assertions.assertEquals(19530, userInfoIgnored.getPort());
        Assertions.assertEquals("db", userInfoIgnored.getDatabase());
    }

    @Test
    void parseDatabaseFromRootOrNestedPaths() {
        URLParser rootPath = new URLParser("http://host:19530/");
        Assertions.assertEquals("host", rootPath.getHostname());
        Assertions.assertNull(rootPath.getDatabase());

        URLParser emptyPath = new URLParser("http://host:19530");
        Assertions.assertNull(emptyPath.getDatabase());

        URLParser nested = new URLParser("http://host:19530/nested/db");
        Assertions.assertEquals("nested/db", nested.getDatabase());
    }

    @Test
    void rejectInvalidUrls() {
        IllegalArgumentException missingHost = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new URLParser("https://"));
        Assertions.assertTrue(missingHost.getMessage().startsWith("Invalid url:"));
        Assertions.assertTrue(missingHost.getMessage().contains("https://"));

        IllegalArgumentException invalidUri = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new URLParser("ht!tp://host"));
        Assertions.assertTrue(invalidUri.getMessage().startsWith("Invalid url:"));
        Assertions.assertTrue(invalidUri.getMessage().contains("ht!tp://host"));
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

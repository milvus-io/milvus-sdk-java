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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.common.utils.RoaringBitmapUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Tag("unit")
class RoaringGoldenVectorTest {

    /**
     * The blob is the whole contract: five SDKs interoperate only if they emit the same bytes for
     * the same members. These fixtures come from the shipped Go reference
     * (<code>client/roaringfilter.Build</code>); every one of them has additionally been checked
     * against the server's own validator and decoded with CRoaring — the library segcore probes
     * with — for exact membership. Matching them byte for byte is therefore the evidence the
     * design doc demands before Java interop may be claimed, so the whole blob is compared rather
     * than a digest of the members.
     */
    @Test
    void testGoldenVectors() throws Exception {
        JsonObject fixture = loadFixture("roaring/roaring_golden_vectors.json");
        Assertions.assertEquals("MRB1", fixture.get("spec").getAsString());
        Assertions.assertEquals(1, fixture.get("version").getAsInt());
        JsonArray cases = fixture.getAsJsonArray("cases");
        Assertions.assertEquals(29, cases.size(), "expected the full shared golden vector suite");

        int checked = 0;
        for (JsonElement element : cases) {
            JsonObject testCase = element.getAsJsonObject();
            String name = testCase.get("name").getAsString();
            long[] members = expandMembers(testCase);
            Assertions.assertEquals(testCase.get("member_count").getAsInt(), members.length,
                    "member expansion disagrees with member_count for " + name);

            byte[] expected = Base64.getDecoder().decode(testCase.get("blob_base64").getAsString());
            byte[] actual = RoaringBitmapUtils.buildRoaringBitmap(members);

            assertBlobEquals(name, expected, actual);
            Assertions.assertEquals(testCase.get("blob_sha256").getAsString(), sha256(actual),
                    "sha256 mismatch for golden vector " + name);

            // The header fields are compared separately so a mismatch names the broken rule
            // rather than only an offset.
            Assertions.assertEquals(testCase.get("cardinality").getAsLong(), cardinalityOf(actual),
                    "header cardinality mismatch for " + name);
            Assertions.assertEquals(testCase.get("body_length").getAsLong(), bodyLengthOf(actual),
                    "header body_length mismatch for " + name);
            Assertions.assertEquals(testCase.get("high_container_count").getAsLong(),
                    readUint64(actual, RoaringBitmapUtils.HEADER_SIZE),
                    "high container count mismatch for " + name);

            // Every entry point must agree with the fixture, not just the long[] one.
            Assertions.assertArrayEquals(expected, buildViaList(members),
                    "List overload diverged for " + name);
            Assertions.assertArrayEquals(expected, buildViaBuilder(members),
                    "Builder diverged for " + name);
            checked++;
        }
        Assertions.assertEquals(29, checked);
    }

    private static byte[] buildViaList(long[] members) {
        List<Long> boxed = new ArrayList<>(members.length);
        for (long member : members) {
            boxed.add(member);
        }
        return RoaringBitmapUtils.buildRoaringBitmap(boxed);
    }

    private static byte[] buildViaBuilder(long[] members) {
        RoaringBitmapUtils.Builder builder = new RoaringBitmapUtils.Builder();
        for (long member : members) {
            builder.add(member);
        }
        return builder.build();
    }

    /** Expands the fixture's {start, count, step} runs into the member list. */
    private static long[] expandMembers(JsonObject testCase) {
        JsonElement spec = testCase.get("members");
        List<Long> members = new ArrayList<>();
        if (spec != null && !spec.isJsonNull()) {
            for (JsonElement element : spec.getAsJsonArray()) {
                JsonObject run = element.getAsJsonObject();
                // Decimal strings, because an int64 does not survive a JSON number.
                long start = Long.parseLong(run.get("start").getAsString());
                long step = Long.parseLong(run.get("step").getAsString());
                int count = run.get("count").getAsInt();
                for (int i = 0; i < count; i++) {
                    members.add(start + (long) i * step);
                }
            }
        }
        long[] result = new long[members.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = members.get(i);
        }
        return result;
    }

    /** Reports the first differing offset, which points straight at the rule that is wrong. */
    private static void assertBlobEquals(String name, byte[] expected, byte[] actual) {
        int limit = Math.min(expected.length, actual.length);
        for (int i = 0; i < limit; i++) {
            if (expected[i] != actual[i]) {
                Assertions.fail(String.format(
                        "golden vector %s differs at byte %d (of %d expected / %d actual): "
                                + "expected 0x%02x, got 0x%02x",
                        name, i, expected.length, actual.length, expected[i], actual[i]));
            }
        }
        Assertions.assertEquals(expected.length, actual.length,
                "golden vector " + name + " has the wrong length");
    }

    private static long cardinalityOf(byte[] blob) {
        return readUint64(blob, 8);
    }

    private static long bodyLengthOf(byte[] blob) {
        return readUint64(blob, 16);
    }

    private static long readUint16(byte[] blob, int position) {
        return (blob[position] & 0xFFL) | ((blob[position + 1] & 0xFFL) << 8);
    }

    private static long readUint32(byte[] blob, int position) {
        return readUint16(blob, position) | (readUint16(blob, position + 2) << 16);
    }

    private static long readUint64(byte[] blob, int position) {
        return readUint32(blob, position) | (readUint32(blob, position + 4) << 32);
    }

    private static String sha256(byte[] data) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
        StringBuilder builder = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            builder.append(Character.forDigit((b >> 4) & 0xF, 16));
            builder.append(Character.forDigit(b & 0xF, 16));
        }
        return builder.toString();
    }

    private static JsonObject loadFixture(String resource) throws Exception {
        try (InputStream stream = RoaringGoldenVectorTest.class.getClassLoader()
                .getResourceAsStream(resource)) {
            Assertions.assertNotNull(stream, "missing test resource " + resource);
            return JsonParser.parseReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
                    .getAsJsonObject();
        }
    }
}

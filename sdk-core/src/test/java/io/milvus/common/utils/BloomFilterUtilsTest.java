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

package io.milvus.common.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.grpc.TemplateValue;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.utils.VectorUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Tag("unit")
class BloomFilterUtilsTest {

    /**
     * The blob is the whole contract: two SDKs interoperate only if they emit the same bytes for
     * the same members. These fixtures are the ones the Go SDK (client/sbbf/testdata) and the
     * segcore C++ unit tests (internal/core/unittest/testdata/bloom) check themselves against, so
     * comparing the full hex here pins Java to the same wire format rather than merely to a
     * self-consistent one.
     */
    @Test
    void testGoldenVectors() throws Exception {
        JsonObject fixture = loadFixture("bloom/golden_vectors.json");
        JsonArray cases = fixture.getAsJsonArray("cases");
        Assertions.assertTrue(cases.size() >= 3, "expected the shared golden vector cases");

        for (JsonElement element : cases) {
            JsonObject testCase = element.getAsJsonObject();
            String name = testCase.get("name").getAsString();
            double fpr = testCase.get("fpr").getAsDouble();
            JsonArray ints = testCase.getAsJsonArray("int_values");
            JsonArray strings = testCase.getAsJsonArray("string_values");

            BloomFilterUtils.Builder builder =
                    new BloomFilterUtils.Builder(ints.size() + strings.size(), fpr);
            for (JsonElement v : ints) {
                builder.addInt64(Long.parseLong(v.getAsString()));
            }
            for (JsonElement v : strings) {
                builder.addString(v.getAsString());
            }

            Assertions.assertEquals(testCase.get("blob_hex").getAsString(), toHex(builder.build()),
                    "blob mismatch for golden vector " + name);
        }
    }

    /** The same blob Arrow C++'s parquet::BlockSplitBloomFilter produces for 100 int64 values. */
    @Test
    void testMatchesArrowCppFixture() throws Exception {
        JsonObject fixture = loadFixture("bloom/cpp_generated_100_int64.json");
        Assertions.assertEquals("apache_arrow_parquet_block_split_bloom_filter",
                fixture.get("generator").getAsString());

        List<Long> members = new ArrayList<>();
        for (JsonElement v : fixture.getAsJsonArray("int_values")) {
            members.add(Long.parseLong(v.getAsString()));
        }
        Assertions.assertEquals(100, members.size());

        byte[] blob = BloomFilterUtils.buildBloomFilter(members, fixture.get("fpr").getAsDouble());
        Assertions.assertEquals(fixture.get("blob_hex").getAsString(), toHex(blob));
    }

    /**
     * The int64 fast path skips the generic hash's stripe and tail loops on the grounds that an
     * 8-byte input can never reach them. If that reasoning ever breaks, every int64 filter goes
     * silently wrong while the string ones stay correct, so it is checked directly.
     */
    @Test
    void testHashInt64MatchesGenericHash() {
        List<Long> values = new ArrayList<>(Arrays.asList(
                0L, 1L, -1L, 2L, -2L, 42L, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 62, -(1L << 62)));
        Random random = new Random(20260731L);
        for (int i = 0; i < 5000; i++) {
            values.add(random.nextLong());
        }

        for (long value : values) {
            byte[] littleEndian =
                    ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
            Assertions.assertEquals(XXHash64.hash(littleEndian), XXHash64.hashInt64(value),
                    "int64 fast path diverged for " + value);
        }
    }

    /** Every input length must agree with the reference, especially around the 32-byte stripe. */
    @Test
    void testHashCoversAllInputShapes() {
        // The generic hash has four length-dependent branches (32-byte stripes, then 8/4/1-byte
        // tails). Lengths 0..80 walk every combination of them; the golden vectors above only
        // reach a handful.
        Random random = new Random(20260731L);
        for (int length = 0; length <= 80; length++) {
            byte[] data = new byte[length];
            random.nextBytes(data);
            long hash = XXHash64.hash(data);
            // Re-hashing a copy must be stable, and the digest must not collapse to a constant.
            Assertions.assertEquals(hash, XXHash64.hash(Arrays.copyOf(data, length)));
        }
        // Known-answer checks against the reference C implementation (libxxhash, seed 0); the
        // empty-input digest is the value published in the xxHash spec itself.
        Assertions.assertEquals(0xEF46DB3751D8E999L, XXHash64.hash(new byte[0]));
        Assertions.assertEquals(0xD24EC4F1A98C6E5BL,
                XXHash64.hash("a".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals(0x44BC2CF5AD770999L,
                XXHash64.hash("abc".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals(0xD8EC969D7FA9836FL,
                XXHash64.hash("milvus".getBytes(StandardCharsets.UTF_8)));

        // Independent XXH64(seed=0) answers from the reference xxHash implementation
        // (`xxhsum -H1`). These straddle the 32-byte boundary that enters the stripe/merge path.
        String[] stripeBoundaryPayloads = {"a".repeat(31), "a".repeat(32), "a".repeat(33)};
        long[] stripeBoundaryHashes = {
                0xFE47067CDA802916L,
                0x856E843298F99AD7L,
                0x18F3FF0C21E3B24BL,
        };
        for (int i = 0; i < stripeBoundaryPayloads.length; i++) {
            Assertions.assertEquals(stripeBoundaryHashes[i],
                    XXHash64.hash(stripeBoundaryPayloads[i].getBytes(StandardCharsets.UTF_8)),
                    "XXH64 mismatch at stripe-boundary length "
                            + stripeBoundaryPayloads[i].length());
        }
    }

    @Test
    void testDomainsAreRecorded() {
        Assertions.assertEquals(BloomFilterUtils.DOMAIN_INT64,
                domainOf(BloomFilterUtils.buildBloomFilter(Collections.singletonList(1L))));
        Assertions.assertEquals(BloomFilterUtils.DOMAIN_UTF8,
                domainOf(BloomFilterUtils.buildBloomFilter(Collections.singletonList("a"))));
        // An empty set records no domain, so the server skips the probe entirely and the filter
        // matches nothing -- rather than aliasing into whichever domain is probed.
        byte[] empty = BloomFilterUtils.buildBloomFilter(Collections.emptyList());
        Assertions.assertEquals(0, domainOf(empty));
        Assertions.assertEquals(BloomFilterUtils.HEADER_SIZE + BloomFilterUtils.MIN_FILTER_BYTES,
                empty.length);

        // A Builder may record both domains; the convenience method may not.
        BloomFilterUtils.Builder mixed = new BloomFilterUtils.Builder(2, 0.001);
        mixed.addInt64(1L).addString("a");
        Assertions.assertEquals(BloomFilterUtils.DOMAIN_INT64 | BloomFilterUtils.DOMAIN_UTF8,
                mixed.getDomains());
    }

    @Test
    void testHeaderLayout() {
        byte[] blob = BloomFilterUtils.buildBloomFilter(Collections.singletonList(42L), 0.01);
        ByteBuffer header = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);

        byte[] magic = new byte[4];
        header.get(magic);
        Assertions.assertEquals(BloomFilterUtils.MAGIC, new String(magic, StandardCharsets.US_ASCII));
        Assertions.assertEquals(BloomFilterUtils.VERSION, header.getShort());
        Assertions.assertEquals(BloomFilterUtils.ALGO_PARQUET_SBBF_XXH64, header.getShort());
        Assertions.assertEquals(1L, header.getLong());
        Assertions.assertEquals(0.01, header.getDouble());
        int numBlocks = header.getInt();
        Assertions.assertEquals(BloomFilterUtils.DOMAIN_INT64, header.get());
        // Reserved bytes must be zero: the server rejects a blob that sets them.
        Assertions.assertEquals(0, blob[29] | blob[30] | blob[31]);
        // Body length must match the declared block count exactly.
        Assertions.assertEquals(blob.length - BloomFilterUtils.HEADER_SIZE,
                numBlocks * BloomFilterUtils.BYTES_PER_BLOCK);
        Assertions.assertEquals(0, numBlocks & (numBlocks - 1), "num_blocks must be a power of two");
    }

    @Test
    void testIntegerTypesWidenToTheSameFilter() {
        byte[] fromLong = BloomFilterUtils.buildBloomFilter(Arrays.asList(1L, 2L, 3L), 0.001);
        byte[] fromInteger = BloomFilterUtils.buildBloomFilter(Arrays.asList(1, 2, 3), 0.001);
        byte[] fromShort =
                BloomFilterUtils.buildBloomFilter(Arrays.asList((short) 1, (short) 2, (short) 3),
                        0.001);
        byte[] fromByte = BloomFilterUtils.buildBloomFilter(
                Arrays.asList((byte) 1, (byte) 2, (byte) 3), 0.001);

        Assertions.assertArrayEquals(fromLong, fromInteger);
        Assertions.assertArrayEquals(fromLong, fromShort);
        Assertions.assertArrayEquals(fromLong, fromByte);
    }

    @Test
    void testEstimateBlobSizeMatchesBuiltSize() {
        double[] fprs = {BloomFilterUtils.MIN_FPR, 0.001, BloomFilterUtils.DEFAULT_FPR,
                BloomFilterUtils.MAX_FPR};
        int[] counts = {0, 1, 2, 100, 5000, 100000};
        for (double fpr : fprs) {
            for (int count : counts) {
                List<Long> members = new ArrayList<>(count);
                for (long i = 0; i < count; i++) {
                    members.add(i);
                }
                Assertions.assertEquals(BloomFilterUtils.estimateBlobSize(count, fpr),
                        BloomFilterUtils.buildBloomFilter(members, fpr).length,
                        "estimate diverged at count=" + count + " fpr=" + fpr);
            }
        }
        // The estimate is what callers check against the proxy's 64 MiB body limit, so the
        // clamp at the top end has to hold.
        Assertions.assertEquals(BloomFilterUtils.HEADER_SIZE + BloomFilterUtils.MAX_FILTER_BYTES,
                BloomFilterUtils.estimateBlobSize(Long.MAX_VALUE, BloomFilterUtils.MIN_FPR));
    }

    @Test
    void testFprBoundariesAreAccepted() {
        for (double fpr : new double[] {BloomFilterUtils.MIN_FPR, BloomFilterUtils.MAX_FPR}) {
            byte[] blob = BloomFilterUtils.buildBloomFilter(Collections.singletonList(1L), fpr);
            Assertions.assertEquals(fpr,
                    ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN).getDouble(16));
        }
    }

    @Test
    void testRejectsInvalidFpr() {
        List<Long> members = Collections.singletonList(1L);
        double[] invalid = {0.00009, 0.051, Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, 0.0, -1.0};
        for (double fpr : invalid) {
            Assertions.assertThrows(MilvusClientException.class,
                    () -> BloomFilterUtils.buildBloomFilter(members, fpr),
                    "fpr " + fpr + " should be rejected");
            Assertions.assertThrows(MilvusClientException.class,
                    () -> BloomFilterUtils.estimateBlobSize(1, fpr));
            Assertions.assertThrows(MilvusClientException.class,
                    () -> new BloomFilterUtils.Builder(1, fpr));
        }
    }

    @Test
    void testRejectsNegativeMemberCounts() {
        for (long count : new long[] {-1L, Long.MIN_VALUE}) {
            Assertions.assertThrows(MilvusClientException.class,
                    () -> BloomFilterUtils.estimateBlobSize(count, 0.001));
            Assertions.assertThrows(MilvusClientException.class,
                    () -> new BloomFilterUtils.Builder(count, 0.001));
        }
    }

    @Test
    void testRejectsInvalidMembers() {
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(null, 0.001));
        // Mixed domains: a filter that recorded both would match members of either type, which
        // is never what a single-field bloom_match wants.
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(Arrays.asList(1L, "mixed"), 0.001));
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(Arrays.asList("mixed", 1L), 0.001));
        // Boolean is rejected rather than widened to 0/1, matching pymilvus.
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(Collections.singletonList(true), 0.001));
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(Arrays.asList(1L, 2.5), 0.001));
        Assertions.assertThrows(MilvusClientException.class,
                () -> BloomFilterUtils.buildBloomFilter(Collections.singletonList(null), 0.001));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new BloomFilterUtils.Builder(1, 0.001).addString(null));
    }

    /**
     * The blob is useless unless it survives the trip into a filter template parameter as raw
     * bytes -- a String round trip would base64-inflate it by a third and corrupt it besides,
     * since the body is not valid UTF-8.
     */
    @Test
    void testBlobTravelsAsTemplateBytesValue() {
        byte[] blob = BloomFilterUtils.buildBloomFilter(Arrays.asList("alice", "bob", "小明"), 0.01);

        TemplateValue value = VectorUtils.deduceAndCreateTemplateValue(blob);

        Assertions.assertEquals(TemplateValue.ValCase.BYTES_VAL, value.getValCase());
        Assertions.assertArrayEquals(blob, value.getBytesVal().toByteArray());
    }

    private static byte domainOf(byte[] blob) {
        return blob[28];
    }

    private static JsonObject loadFixture(String resource) throws Exception {
        try (InputStream stream =
                     BloomFilterUtilsTest.class.getClassLoader().getResourceAsStream(resource)) {
            Assertions.assertNotNull(stream, "missing test resource " + resource);
            return JsonParser.parseReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
                    .getAsJsonObject();
        }
    }

    private static String toHex(byte[] data) {
        StringBuilder builder = new StringBuilder(data.length * 2);
        for (byte b : data) {
            builder.append(Character.forDigit((b >> 4) & 0xF, 16));
            builder.append(Character.forDigit(b & 0xF, 16));
        }
        return builder.toString();
    }
}

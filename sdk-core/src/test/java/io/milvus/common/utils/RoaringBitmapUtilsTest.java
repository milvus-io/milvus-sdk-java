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
import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.TemplateValue;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.VectorUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Random;

class RoaringBitmapUtilsTest {

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

    /**
     * Section 1 of the spec: a member is sign-extended to int64 and its two's-complement bit
     * pattern becomes the unsigned key. Zero-extending a narrow value, ZigZag encoding it, or
     * biasing it by 2^63 all produce a bitmap that silently matches the wrong rows, so the key
     * each boundary value lands on is asserted directly rather than inferred from a digest.
     */
    @Test
    void testSignedValuesMapToTwosComplementKeys() {
        long[][] table = {
                {-1L, 0xffffffffffffffffL},
                {-128L, 0xffffffffffffff80L},
                {-32768L, 0xffffffffffff8000L},
                {Integer.MIN_VALUE, 0xffffffff80000000L},
                {Long.MIN_VALUE, 0x8000000000000000L},
                {0L, 0x0000000000000000L},
                {42L, 0x000000000000002aL},
                {Long.MAX_VALUE, 0x7fffffffffffffffL},
        };
        for (long[] row : table) {
            byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(new long[] {row[0]});
            Assertions.assertEquals(row[1], singleMemberKeyOf(blob),
                    "wrong key for member " + row[0]);
        }

        // Narrow signed types must widen, never zero-extend: INT8(-1) is key 0xffff...ff, not 0xff.
        byte[] fromLong = RoaringBitmapUtils.buildRoaringBitmap(new long[] {-1L});
        Assertions.assertArrayEquals(fromLong,
                RoaringBitmapUtils.buildRoaringBitmap(Collections.singletonList((byte) -1)));
        Assertions.assertArrayEquals(fromLong,
                RoaringBitmapUtils.buildRoaringBitmap(Collections.singletonList((short) -1)));
        Assertions.assertArrayEquals(fromLong,
                RoaringBitmapUtils.buildRoaringBitmap(Collections.singletonList(-1)));
        Assertions.assertArrayEquals(fromLong,
                RoaringBitmapUtils.buildRoaringBitmap(new int[] {-1}));
        // ...and 255 is a different member entirely, which is what a zero-extending build would
        // have collapsed it into.
        Assertions.assertFalse(Arrays.equals(fromLong,
                RoaringBitmapUtils.buildRoaringBitmap(new long[] {255L})));
    }

    /**
     * Ordering and grouping are on the unsigned key, so a negative member is the <b>largest</b>
     * key, not the smallest. Sorting signed puts -1 in the first high container and reverses the
     * whole blob; nothing else in the format catches that, because the result is still a
     * structurally valid bitmap.
     */
    @Test
    void testKeysAreOrderedUnsigned() {
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(new long[] {-1L, 5L});
        Assertions.assertEquals(2, readUint64(blob, RoaringBitmapUtils.HEADER_SIZE));
        // {-1, 5} must be stored as {5, 0xffffffffffffffff}: high key 0 first, 0xffffffff second.
        Assertions.assertEquals(0x00000000L,
                readUint32(blob, RoaringBitmapUtils.HEADER_SIZE + 8));

        // The same claim over a random set: the high keys written to the blob must be exactly the
        // distinct high halves in Long.compareUnsigned order.
        Random random = new Random(20260817L);
        List<Long> members = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            members.add(random.nextLong());
        }
        List<Long> expectedHighKeys = new ArrayList<>();
        List<Long> sorted = new ArrayList<>(members);
        sorted.sort(Long::compareUnsigned);
        for (long key : sorted) {
            long high = key >>> 32;
            if (expectedHighKeys.isEmpty()
                    || expectedHighKeys.get(expectedHighKeys.size() - 1) != high) {
                expectedHighKeys.add(high);
            }
        }

        byte[] randomBlob = RoaringBitmapUtils.buildRoaringBitmap(members);
        Assertions.assertEquals(expectedHighKeys.size(),
                readUint64(randomBlob, RoaringBitmapUtils.HEADER_SIZE));
        Assertions.assertEquals(expectedHighKeys, highKeysOf(randomBlob));
    }

    /** Duplicates collapse and input order is irrelevant: one set, one blob. */
    @Test
    void testDuplicatesCollapseAndOrderDoesNotMatter() {
        long[] canonical = {-9223372036854775808L, -70000L, -1L, 0L, 1L, 42L, 70000L,
                4294967296L, 9223372036854775807L};
        byte[] expected = RoaringBitmapUtils.buildRoaringBitmap(canonical);
        Assertions.assertEquals(canonical.length, cardinalityOf(expected));

        // Every member twice, in a shuffled order, must still be the same blob.
        List<Long> doubled = new ArrayList<>();
        for (long member : canonical) {
            doubled.add(member);
            doubled.add(member);
        }
        Collections.shuffle(doubled, new Random(20260817L));
        Assertions.assertArrayEquals(expected, RoaringBitmapUtils.buildRoaringBitmap(doubled));

        // Ten independent shufflings, so this cannot pass by luck of one permutation.
        Random random = new Random(7L);
        for (int i = 0; i < 10; i++) {
            List<Long> shuffled = new ArrayList<>(doubled);
            Collections.shuffle(shuffled, random);
            Assertions.assertArrayEquals(expected,
                    RoaringBitmapUtils.buildRoaringBitmap(shuffled),
                    "blob depended on input order at iteration " + i);
        }
    }

    /** An empty set is a valid bitmap that matches nothing, not an error and not a zero-length body. */
    @Test
    void testEmptyMemberSet() {
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(new long[0]);
        Assertions.assertEquals(RoaringBitmapUtils.HEADER_SIZE + 8, blob.length);
        Assertions.assertEquals(0L, cardinalityOf(blob));
        // body_length is 8, never 0: the body still carries the high-container count.
        Assertions.assertEquals(8L, bodyLengthOf(blob));
        Assertions.assertEquals(0L, readUint64(blob, RoaringBitmapUtils.HEADER_SIZE));

        Assertions.assertArrayEquals(blob,
                RoaringBitmapUtils.buildRoaringBitmap(Collections.<Long>emptyList()));
        Assertions.assertArrayEquals(blob, RoaringBitmapUtils.buildRoaringBitmap(new int[0]));
        Assertions.assertArrayEquals(blob, new RoaringBitmapUtils.Builder().build());
    }

    /** The 32-byte MRB1 envelope, field by field. */
    @Test
    void testHeaderLayout() {
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(new long[] {1L, 2L, 3L, 4L});
        Assertions.assertEquals(RoaringBitmapUtils.MAGIC,
                new String(Arrays.copyOf(blob, 4), StandardCharsets.US_ASCII));
        Assertions.assertEquals(RoaringBitmapUtils.VERSION, readUint16(blob, 4));
        Assertions.assertEquals(RoaringBitmapUtils.FORMAT_PORTABLE_ROARING64, readUint16(blob, 6));
        Assertions.assertEquals(4L, cardinalityOf(blob));
        Assertions.assertEquals(blob.length - RoaringBitmapUtils.HEADER_SIZE, bodyLengthOf(blob));
        // Reserved bytes must be zero: the server rejects a blob that sets them.
        for (int i = 24; i < 32; i++) {
            Assertions.assertEquals(0, blob[i], "reserved byte " + i + " is not zero");
        }
    }

    /** Every entry point is the same builder underneath and must agree on the bytes. */
    @Test
    void testOverloadsAgree() {
        long[] members = {-2147483648L, -1L, 0L, 7L, 8L, 9L, 65535L, 65536L, 2147483647L};
        byte[] expected = RoaringBitmapUtils.buildRoaringBitmap(members);

        int[] asInts = new int[members.length];
        List<Long> asLongs = new ArrayList<>();
        List<Number> asMixedNumbers = new ArrayList<>();
        for (int i = 0; i < members.length; i++) {
            asInts[i] = (int) members[i];
            asLongs.add(members[i]);
            asMixedNumbers.add(members[i]);
        }
        // A byte member and a long member with the same value must land on the same key.
        asMixedNumbers.set(3, (byte) 7);
        asMixedNumbers.set(4, (short) 8);
        asMixedNumbers.set(5, 9);

        Assertions.assertArrayEquals(expected, RoaringBitmapUtils.buildRoaringBitmap(asInts));
        Assertions.assertArrayEquals(expected, RoaringBitmapUtils.buildRoaringBitmap(asLongs));
        Assertions.assertArrayEquals(expected,
                RoaringBitmapUtils.buildRoaringBitmap(asMixedNumbers));
        Assertions.assertArrayEquals(expected,
                new RoaringBitmapUtils.Builder().addAll(members).build());
        Assertions.assertArrayEquals(expected,
                new RoaringBitmapUtils.Builder().addAll(asInts).build());
        Assertions.assertArrayEquals(expected,
                new RoaringBitmapUtils.Builder(members.length).addAll(asLongs).build());
    }

    /** The array the caller keeps must not be reordered or aliased under them. */
    @Test
    void testInputArrayIsNotMutatedAndBlobIsIndependent() {
        long[] members = {5L, -1L, 3L};
        long[] copy = Arrays.copyOf(members, members.length);
        byte[] first = RoaringBitmapUtils.buildRoaringBitmap(members);
        Assertions.assertArrayEquals(copy, members, "the caller's array was sorted in place");

        // A Builder is reusable: build twice, add more, build again -- and the first blob must
        // not change under the caller, since the point of the API is to build once and reuse.
        RoaringBitmapUtils.Builder builder = new RoaringBitmapUtils.Builder();
        builder.addAll(members);
        byte[] before = builder.build();
        Assertions.assertArrayEquals(first, before);
        Assertions.assertArrayEquals(before, builder.build(), "build() is not repeatable");
        Assertions.assertEquals(3, builder.size());

        builder.add(1L << 40);
        byte[] after = builder.build();
        Assertions.assertArrayEquals(first, before, "an earlier blob was mutated by a later add");
        Assertions.assertEquals(4L, cardinalityOf(after));
        Assertions.assertEquals(4, builder.size());
    }

    /**
     * Section 5 limits. The server enforces these too, but failing here costs one exception
     * instead of a multi-megabyte round trip that is going to be rejected anyway.
     */
    @Test
    void testRejectsTooManyHighContainers() {
        // One member per 2^32 window: 262144 windows is the maximum, 262145 is one too many.
        long[] atLimit = new long[RoaringBitmapUtils.MAX_HIGH_CONTAINERS];
        for (int i = 0; i < atLimit.length; i++) {
            atLimit[i] = (long) i << 32;
        }
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(atLimit);
        Assertions.assertEquals(RoaringBitmapUtils.MAX_HIGH_CONTAINERS,
                readUint64(blob, RoaringBitmapUtils.HEADER_SIZE));

        long[] overLimit = Arrays.copyOf(atLimit, atLimit.length + 1);
        overLimit[atLimit.length] = (long) atLimit.length << 32;
        MilvusClientException error = Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(overLimit));
        Assertions.assertTrue(error.getMessage().contains("high-container count"),
                "message should name the limit, was: " + error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("262144"), error.getMessage());
    }

    @Test
    void testRejectsOversizedEstimatedDecodedSize() {
        // 16 high containers x 65536 low containers, one member each: the per-low-container decode
        // overhead alone (64 bytes) blows past the 64 MiB budget, so this is rejected before any
        // body is allocated.
        long[] members = new long[16 * 65536];
        int index = 0;
        for (int high = 0; high < 16; high++) {
            for (int low = 0; low < 65536; low++) {
                members[index++] = ((long) high << 32) | ((long) low << 16);
            }
        }
        MilvusClientException error = Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(members));
        Assertions.assertTrue(error.getMessage().contains("estimated decoded size"),
                "message should name the limit, was: " + error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("67108864"), error.getMessage());

        // Just under the fast pre-check but still over once the body is counted: the exact check
        // after measuring has to catch this one, not the cheap lower-bound one.
        long[] nearMiss = new long[16 * 62500];
        index = 0;
        for (int high = 0; high < 16; high++) {
            for (int low = 0; low < 62500; low++) {
                nearMiss[index++] = ((long) high << 32) | ((long) low << 16);
            }
        }
        Assertions.assertTrue(8L + 128L * 16 + 64L * nearMiss.length
                        <= RoaringBitmapUtils.MAX_DECODED_BYTES,
                "the near-miss case must pass the lower-bound pre-check to be meaningful");
        MilvusClientException exact = Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(nearMiss));
        Assertions.assertTrue(exact.getMessage().contains("estimated decoded size"),
                exact.getMessage());
    }

    /** A bitmap is an integer set; anything else is a caller mistake, never a silent coercion. */
    @Test
    void testRejectsInvalidMembers() {
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap((List<Number>) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap((long[]) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap((int[]) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new RoaringBitmapUtils.Builder().addAll((long[]) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new RoaringBitmapUtils.Builder().addAll((int[]) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new RoaringBitmapUtils.Builder().addAll((List<Number>) null));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new RoaringBitmapUtils.Builder(-1));

        // A null element, and the float types: truncating 2.5 to 2 would build a bitmap that
        // matches a row the caller never asked for.
        List<Number> withNull = new ArrayList<>();
        withNull.add(1L);
        withNull.add(null);
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(withNull));
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(Arrays.<Number>asList(1L, 2.5d)));
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(Arrays.<Number>asList(1L, 2.5f)));
        // BigInteger is a Number but not a 64-bit signed one, so it is rejected rather than
        // silently wrapped.
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap(
                        Collections.singletonList(java.math.BigInteger.valueOf(1))));
        Assertions.assertThrows(MilvusClientException.class,
                () -> new RoaringBitmapUtils.Builder().addAll(
                        Arrays.<Number>asList(1L, Double.valueOf(1))));

        // Boolean and String cannot reach the API through its declared type, but a raw-typed List
        // can still smuggle one in, so the runtime guard is exercised too.
        List raw = new ArrayList();
        raw.add(1L);
        raw.add("not a number");
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap((List<? extends Number>) raw));
        List rawBoolean = new ArrayList();
        rawBoolean.add(Boolean.TRUE);
        Assertions.assertThrows(MilvusClientException.class,
                () -> RoaringBitmapUtils.buildRoaringBitmap((List<? extends Number>) rawBoolean));
    }

    /**
     * The blob is useless unless it survives the trip into a filter template parameter as raw
     * bytes -- a String round trip would base64-inflate it by a third and corrupt it besides,
     * since the body is not valid UTF-8. The same byte[] branch already carries bloom filters;
     * this asserts it carries a roaring blob through every expression-bearing request too.
     */
    @Test
    void testBlobTravelsAsTemplateBytesValue() {
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(
                Arrays.asList(-1L, 0L, 1L, 42L, 1L << 40));
        // A roaring body carries bytes that are not valid UTF-8, so a String round trip would
        // replace them and silently corrupt the bitmap. Asserting it here first means the rest of
        // this test is actually proving something rather than passing on an ASCII-safe payload.
        Assertions.assertFalse(
                Arrays.equals(blob, new String(blob, StandardCharsets.UTF_8)
                        .getBytes(StandardCharsets.UTF_8)),
                "the fixture blob survives a UTF-8 round trip, so this test proves nothing");

        TemplateValue value = VectorUtils.deduceAndCreateTemplateValue(blob);
        Assertions.assertEquals(TemplateValue.ValCase.BYTES_VAL, value.getValCase());
        Assertions.assertArrayEquals(blob, value.getBytesVal().toByteArray());

        QueryRequest query = new VectorUtils().ConvertToGrpcQueryRequest(QueryReq.builder()
                .collectionName("coll")
                .filter("roaring_match(user_id, {ids})")
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .build());
        assertCarriesBlob(query.getExprTemplateValuesMap().get("ids"), blob);

        SearchRequest search = new VectorUtils().ConvertToGrpcSearchRequest(SearchReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .filter("roaring_match(user_id, {ids})")
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .topK(10)
                .build());
        assertCarriesBlob(search.getExprTemplateValuesMap().get("ids"), blob);

        SearchRequest hybrid = VectorUtils.convertAnnSearchParam(AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .filter("roaring_match(user_id, {ids})")
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .limit(10)
                .build(), ConsistencyLevel.BOUNDED);
        assertCarriesBlob(hybrid.getExprTemplateValuesMap().get("ids"), blob);

        DeleteRequest delete = new DataUtils().ConvertToGrpcDeleteRequest(DeleteReq.builder()
                .collectionName("coll")
                .filter("roaring_match(user_id, {ids})")
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .build());
        assertCarriesBlob(delete.getExprTemplateValuesMap().get("ids"), blob);
    }

    private static void assertCarriesBlob(TemplateValue value, byte[] blob) {
        Assertions.assertNotNull(value, "the template value never reached the request");
        Assertions.assertEquals(TemplateValue.ValCase.BYTES_VAL, value.getValCase());
        Assertions.assertArrayEquals(blob, value.getBytesVal().toByteArray());
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

    /** Reads back the single key of a one-member blob, to pin the signed to unsigned mapping. */
    private static long singleMemberKeyOf(byte[] blob) {
        Assertions.assertEquals(1L, cardinalityOf(blob));
        Assertions.assertEquals(1L, readUint64(blob, RoaringBitmapUtils.HEADER_SIZE));
        long high = readUint32(blob, RoaringBitmapUtils.HEADER_SIZE + 8);
        // A one-value container is an array container: cookie(4) + count(4) fills 8 bytes, then
        // the 4-byte descriptor, the 4-byte offset entry, and finally the single uint16 value.
        Assertions.assertEquals(12346L, readUint32(blob, RoaringBitmapUtils.HEADER_SIZE + 12));
        Assertions.assertEquals(1L, readUint32(blob, RoaringBitmapUtils.HEADER_SIZE + 16));
        long containerKey = readUint16(blob, RoaringBitmapUtils.HEADER_SIZE + 20);
        long value = readUint16(blob, RoaringBitmapUtils.HEADER_SIZE + 28);
        return (high << 32) | (containerKey << 16) | value;
    }

    private static List<Long> highKeysOf(byte[] blob) {
        int highCount = (int) readUint64(blob, RoaringBitmapUtils.HEADER_SIZE);
        List<Long> keys = new ArrayList<>(highCount);
        int position = RoaringBitmapUtils.HEADER_SIZE + 8;
        for (int i = 0; i < highCount; i++) {
            keys.add(readUint32(blob, position));
            position += 4;
            position += roaring32Length(blob, position);
        }
        return keys;
    }

    /** Walks one portable Roaring32 blob far enough to find where the next one starts. */
    private static int roaring32Length(byte[] blob, int start) {
        long cookie = readUint32(blob, start);
        boolean hasRun = (cookie & 0xFFFF) == 12347;
        int containers;
        int position;
        int runBitmapSize = 0;
        if (hasRun) {
            containers = (int) readUint16(blob, start + 2) + 1;
            runBitmapSize = (containers + 7) / 8;
            position = start + 4 + runBitmapSize;
        } else {
            Assertions.assertEquals(12346L, cookie);
            containers = (int) readUint32(blob, start + 4);
            position = start + 8;
        }

        boolean[] isRun = new boolean[containers];
        if (hasRun) {
            for (int i = 0; i < containers; i++) {
                isRun[i] = (blob[start + 4 + i / 8] & (1 << (i % 8))) != 0;
            }
        }
        int[] cardinalities = new int[containers];
        for (int i = 0; i < containers; i++) {
            cardinalities[i] = (int) readUint16(blob, position + 4 * i + 2) + 1;
        }
        position += 4 * containers;
        if (!hasRun || containers >= 4) {
            position += 4 * containers;
        }
        for (int i = 0; i < containers; i++) {
            if (isRun[i]) {
                int runs = (int) readUint16(blob, position);
                position += 2 + 4 * runs;
            } else if (cardinalities[i] > 4096) {
                position += 8192;
            } else {
                position += 2 * cardinalities[i];
            }
        }
        return position - start;
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
        try (InputStream stream = RoaringBitmapUtilsTest.class.getClassLoader()
                .getResourceAsStream(resource)) {
            Assertions.assertNotNull(stream, "missing test resource " + resource);
            return JsonParser.parseReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
                    .getAsJsonObject();
        }
    }
}

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

import io.milvus.common.utils.RoaringBitmapUtils;
import io.milvus.v2.exception.MilvusClientException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tag("unit")
class RoaringValidationTest {

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

    private static long readUint16(byte[] blob, int position) {
        return (blob[position] & 0xFFL) | ((blob[position + 1] & 0xFFL) << 8);
    }

    private static long readUint32(byte[] blob, int position) {
        return readUint16(blob, position) | (readUint16(blob, position + 2) << 16);
    }

    private static long readUint64(byte[] blob, int position) {
        return readUint32(blob, position) | (readUint32(blob, position + 4) << 32);
    }
}

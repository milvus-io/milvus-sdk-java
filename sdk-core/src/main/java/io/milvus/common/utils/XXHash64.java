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

/**
 * XXH64 with seed 0 — the hash the Parquet Split-Block Bloom Filter spec mandates, and thus the
 * one {@link BloomFilterUtils} must reproduce bit-for-bit to stay interoperable with the Milvus
 * server, pymilvus and the Go SDK.
 *
 * <p>Implemented here rather than pulled in as a dependency: sdk-core has no XXH64 on its
 * classpath (Guava's Hashing offers murmur3/sipHash/farmHash but not XXH64, and parquet-column's
 * copy lives in sdk-bulkwriter behind hadoop).
 *
 * <p>Java's long arithmetic is two's complement and wraps modulo 2^64, so every multiply and add
 * below is already the unsigned operation the spec calls for; only the shifts need care
 * ({@code >>>} for the logical shift, never {@code >>}).
 */
final class XXHash64 {
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private XXHash64() {
    }

    /**
     * Returns XXH64(data, seed=0).
     *
     * @param data the bytes to hash
     * @return the 64-bit digest
     */
    static long hash(byte[] data) {
        final int length = data.length;
        int index = 0;
        long result;

        if (length >= 32) {
            long v1 = PRIME64_1 + PRIME64_2;
            long v2 = PRIME64_2;
            long v3 = 0;
            long v4 = -PRIME64_1;
            final int limit = length - 32;
            while (index <= limit) {
                v1 = round(v1, readLong(data, index));
                v2 = round(v2, readLong(data, index + 8));
                v3 = round(v3, readLong(data, index + 16));
                v4 = round(v4, readLong(data, index + 24));
                index += 32;
            }
            result = Long.rotateLeft(v1, 1)
                    + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12)
                    + Long.rotateLeft(v4, 18);
            result = mergeRound(result, v1);
            result = mergeRound(result, v2);
            result = mergeRound(result, v3);
            result = mergeRound(result, v4);
        } else {
            result = PRIME64_5;
        }

        result += length;

        while (index + 8 <= length) {
            result ^= round(0, readLong(data, index));
            result = Long.rotateLeft(result, 27) * PRIME64_1 + PRIME64_4;
            index += 8;
        }
        if (index + 4 <= length) {
            result ^= readInt(data, index) * PRIME64_1;
            result = Long.rotateLeft(result, 23) * PRIME64_2 + PRIME64_3;
            index += 4;
        }
        while (index < length) {
            result ^= (data[index] & 0xFFL) * PRIME64_5;
            result = Long.rotateLeft(result, 11) * PRIME64_1;
            index++;
        }
        return avalanche(result);
    }

    /**
     * Returns XXH64 over v's 8-byte little-endian encoding, without materialising those bytes.
     *
     * <p>Specialisation of {@link #hash(byte[])} for the one input length the int64 domain ever
     * produces: the 32-byte stripe loop and the 4-byte / 1-byte tails are all unreachable,
     * leaving a single 8-byte lane. Kept honest by BloomFilterUtilsTest, which cross-checks it
     * against the generic path over thousands of values.
     *
     * @param v the value to hash
     * @return the 64-bit digest
     */
    static long hashInt64(long v) {
        long acc = v * PRIME64_2;
        acc = Long.rotateLeft(acc, 31);
        acc *= PRIME64_1;
        long result = (PRIME64_5 + 8) ^ acc;
        result = Long.rotateLeft(result, 27) * PRIME64_1 + PRIME64_4;
        return avalanche(result);
    }

    private static long round(long acc, long value) {
        acc += value * PRIME64_2;
        acc = Long.rotateLeft(acc, 31);
        return acc * PRIME64_1;
    }

    private static long mergeRound(long acc, long value) {
        acc ^= round(0, value);
        return acc * PRIME64_1 + PRIME64_4;
    }

    private static long avalanche(long h) {
        h ^= h >>> 33;
        h *= PRIME64_2;
        h ^= h >>> 29;
        h *= PRIME64_3;
        h ^= h >>> 32;
        return h;
    }

    /** Reads a little-endian int64 at the given offset. */
    private static long readLong(byte[] data, int index) {
        return (data[index] & 0xFFL)
                | (data[index + 1] & 0xFFL) << 8
                | (data[index + 2] & 0xFFL) << 16
                | (data[index + 3] & 0xFFL) << 24
                | (data[index + 4] & 0xFFL) << 32
                | (data[index + 5] & 0xFFL) << 40
                | (data[index + 6] & 0xFFL) << 48
                | (data[index + 7] & 0xFFL) << 56;
    }

    /** Reads a little-endian uint32 at the given offset, zero-extended to long. */
    private static long readInt(byte[] data, int index) {
        return (data[index] & 0xFFL)
                | (data[index + 1] & 0xFFL) << 8
                | (data[index + 2] & 0xFFL) << 16
                | (data[index + 3] & 0xFFL) << 24;
    }
}

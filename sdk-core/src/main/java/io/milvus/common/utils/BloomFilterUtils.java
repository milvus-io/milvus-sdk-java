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

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds a client-side Split-Block Bloom Filter (SBBF) blob for the
 * <code>bloom_match(field, {blob})</code> filter expression.
 *
 * <p>Building the filter on the client and shipping the compact blob (about 32 MB for ~24M
 * members at the default false-positive rate) lets large membership sets pass the proxy gRPC
 * receive limit, which a raw value list (about 90 MB for 10M int64) would exceed. Pass the
 * result through <code>filterTemplateValues</code>; it travels as a native protobuf bytes
 * value with no base64 inflation, and the server embeds it verbatim after validating the
 * envelope — it never rebuilds the filter.
 *
 * <pre>{@code
 * byte[] blob = BloomFilterUtils.buildBloomFilter(userIds, BloomFilterUtils.DEFAULT_FPR);
 * QueryReq req = QueryReq.builder()
 *         .collectionName("docs")
 *         .filter("bloom_match(user_id, {bf})")
 *         .filterTemplateValues(Collections.singletonMap("bf", blob))
 *         .build();
 * }</pre>
 *
 * <p><b>Build the blob from the same value domain as the target field</b>: integer fields hash
 * int64, VARCHAR fields hash UTF-8. The envelope records which domains were inserted, so a
 * wrong-domain blob is rejected by the proxy with a parameter error rather than silently
 * matching nothing. JSON paths are strictly typed per row: JSON strings behave like VARCHAR and
 * JSON integers like int64, but a JSON double NEVER matches — a stored 5.0 does not match an
 * int64 member 5, unlike exact <code>in</code> (which unifies 5.0 == 5).
 *
 * <p>The bit layout is bit-identical to Arrow C++'s
 * <code>parquet::BlockSplitBloomFilter</code> and therefore to the parquet-format
 * BloomFilter.md spec, wrapped in the Milvus MBF1 envelope. Blobs built here are byte-identical
 * to the ones pymilvus (<code>build_bloom_filter</code>) and the Go SDK
 * (<code>sbbf.Builder</code>) produce for the same members. See
 * <code>docs/design-docs/design_docs/20260707-bloom-filter-expression.md</code> in the milvus
 * repository.
 */
public class BloomFilterUtils {
    /** The 4-byte MBF1 envelope magic. */
    public static final String MAGIC = "MBF1";
    /** The MBF1 envelope version implemented by this class. */
    public static final short VERSION = 1;
    /** Identifies the parquet SBBF + XXH64 algorithm. */
    public static final short ALGO_PARQUET_SBBF_XXH64 = 1;
    /** Size in bytes of the MBF1 envelope header. */
    public static final int HEADER_SIZE = 32;
    /** Size of one SBBF block (parquet-format spec). */
    public static final int BYTES_PER_BLOCK = 32;

    /** Marks a filter that recorded int64 values (8-byte little-endian hash domain). */
    public static final byte DOMAIN_INT64 = 1;
    /** Marks a filter that recorded string values (raw UTF-8 hash domain). */
    public static final byte DOMAIN_UTF8 = 2;

    /**
     * Minimum / maximum filter body size, mirroring Arrow's
     * BlockSplitBloomFilter::kMinimumBloomFilterBytes / kMaximumBloomFilterBytes.
     */
    public static final int MIN_FILTER_BYTES = 32;
    /** @see #MIN_FILTER_BYTES */
    public static final int MAX_FILTER_BYTES = 128 * 1024 * 1024;

    /** Lowest accepted false-positive rate. */
    public static final double MIN_FPR = 0.0001;
    /** Highest accepted false-positive rate. */
    public static final double MAX_FPR = 0.05;
    /**
     * Recommended false-positive rate when a caller has no specific target. Sizing follows the
     * Arrow formula, so a body holds roughly 0.72 members per byte at this rate: a 64 MiB body
     * (the default proxy.maxBloomFilterSize) holds ~48.6M members. Because bodies are powers of
     * two, a member count just past a tier boundary doubles the blob; raising fpr is usually the
     * cheaper fix.
     */
    public static final double DEFAULT_FPR = 0.005;

    private static final int WORDS_PER_BLOCK = 8;

    /**
     * The eight odd constants used to derive one bit position per word inside a block. Fixed by
     * the parquet-format spec and mirrored from Arrow C++'s BlockSplitBloomFilter::SALT.
     */
    private static final int[] SALT = {
            0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
    };

    private BloomFilterUtils() {
    }

    /**
     * Builds a filter over the given members at {@link #DEFAULT_FPR}.
     *
     * @param members all-integer (Byte/Short/Integer/Long) or all-String members
     * @return the MBF1 blob to pass as a filter template value
     * @see #buildBloomFilter(List, double)
     */
    public static byte[] buildBloomFilter(List<?> members) {
        return buildBloomFilter(members, DEFAULT_FPR);
    }

    /**
     * Builds a filter over the given members.
     *
     * <p>Members must be homogeneous: either all integers (Byte/Short/Integer/Long, widened to
     * int64) or all Strings. Boolean is rejected rather than widened to 0/1. An empty list
     * produces a valid blob that records no domain and therefore matches nothing.
     *
     * @param members the membership set
     * @param fpr     the false-positive rate, in [{@link #MIN_FPR}, {@link #MAX_FPR}]
     * @return the MBF1 blob to pass as a filter template value
     * @throws MilvusClientException if fpr is out of range, or members is null, mixed-type or
     *                               contains an unsupported element type
     */
    public static byte[] buildBloomFilter(List<?> members, double fpr) {
        if (members == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Bloom filter members cannot be null.");
        }
        Builder builder = new Builder(members.size(), fpr);
        int index = 0;
        for (Object member : members) {
            if (member instanceof String) {
                builder.addString((String) member);
            } else if (member instanceof Long || member instanceof Integer
                    || member instanceof Short || member instanceof Byte) {
                builder.addInt64(((Number) member).longValue());
            } else {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                        "Bloom filter members must be all integer or all string, "
                                + "but element %d is %s.",
                        index, member == null ? "null" : member.getClass().getSimpleName()));
            }
            // Checked per element rather than once at the end so the error names the element
            // that broke homogeneity. A mixed filter is representable in the envelope, but the
            // convenience API keeps the pymilvus/Go contract: one list, one domain.
            if (builder.domains == (DOMAIN_INT64 | DOMAIN_UTF8)) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                        "Bloom filter members must be all integer or all string, "
                                + "but element %d changed the value domain.", index));
            }
            index++;
        }
        return builder.build();
    }

    /**
     * Returns the exact number of bytes {@link #buildBloomFilter(List, double)} would produce for
     * n members at the given false-positive rate, without allocating the filter or hashing
     * anything. Use it to reject an over-large filter before spending time and memory building
     * it: the body must fit <code>proxy.maxBloomFilterSize</code> (64 MiB by default) and the
     * whole request must fit <code>proxy.grpc.serverMaxRecvSize</code> (128 MiB by default).
     *
     * @param n   the planned member count
     * @param fpr the false-positive rate, in [{@link #MIN_FPR}, {@link #MAX_FPR}]
     * @return the blob size in bytes, header included
     * @throws MilvusClientException if fpr is out of range
     */
    public static int estimateBlobSize(long n, double fpr) {
        validateFpr(fpr);
        return HEADER_SIZE + optimalNumOfBytes(n, fpr);
    }

    private static void validateFpr(double fpr) {
        if (Double.isNaN(fpr) || fpr < MIN_FPR || fpr > MAX_FPR) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                    "Bloom filter fpr %s is out of range [%s, %s].", fpr, MIN_FPR, MAX_FPR));
        }
    }

    /**
     * Mirrors Arrow's BlockSplitBloomFilter::OptimalNumOfBytes: the classic blocked-bloom sizing
     * formula m = -8n / ln(1 - fpp^(1/8)), rounded up to the next power of two and clamped to
     * [{@link #MIN_FILTER_BYTES}, {@link #MAX_FILTER_BYTES}]. The result is always a power of two
     * and a multiple of {@link #BYTES_PER_BLOCK}.
     */
    private static int optimalNumOfBytes(long ndv, double fpp) {
        final int minBits = MIN_FILTER_BYTES << 3;
        final int maxBits = MAX_FILTER_BYTES << 3;
        double m = -8.0 * (double) ndv / Math.log(1.0 - Math.pow(fpp, 1.0 / 8.0));

        int numBits;
        if (m < 0 || m > (double) maxBits) {
            numBits = maxBits;
        } else {
            numBits = (int) m;
        }
        if (numBits < minBits) {
            numBits = minBits;
        }
        if ((numBits & (numBits - 1)) != 0) {
            numBits = nextPowerOfTwo(numBits);
        }
        if (numBits > maxBits) {
            numBits = maxBits;
        }
        return numBits >>> 3;
    }

    /** Returns the smallest power of two greater than or equal to v (1 &lt; v &lt;= 2^30). */
    private static int nextPowerOfTwo(int v) {
        v--;
        v |= v >>> 1;
        v |= v >>> 2;
        v |= v >>> 4;
        v |= v >>> 8;
        v |= v >>> 16;
        return v + 1;
    }

    /**
     * Reduces a hash to a block index via the multiply-shift scheme used by Arrow:
     * ((hash &gt;&gt;&gt; 32) * numBlocks) &gt;&gt;&gt; 32. numBlocks is at most 2^22, so the
     * product cannot overflow.
     */
    private static int blockIndex(long hash, int numBlocks) {
        return (int) (((hash >>> 32) * (numBlocks & 0xFFFFFFFFL)) >>> 32);
    }

    /**
     * Incrementally constructs an SBBF and serializes it into an MBF1 envelope. Not thread-safe.
     *
     * <p>Prefer this over {@link #buildBloomFilter(List, double)} for very large membership sets:
     * {@code addInt64(long)} takes a primitive, so a 10M-member filter costs no boxed
     * {@code List<Long>} (which would be roughly 240 MB of {@code Long} objects on its own).
     *
     * <p>Unlike the convenience method, a Builder accepts both domains, which produces a filter
     * that matches integer and string members alike.
     */
    public static class Builder {
        /** HEADER_SIZE + numBlocks * BYTES_PER_BLOCK: the blob {@link #build()} returns. */
        private final byte[] buf;
        private final int numBlocks;
        private final long nDeclared;
        private final double fpr;
        private byte domains;

        /**
         * Creates a builder sized for n distinct values at false-positive rate fpr.
         *
         * @param n   the expected number of distinct members; it only drives sizing, so inserting
         *            more is allowed and simply raises the effective false-positive rate
         * @param fpr the false-positive rate, in [{@link #MIN_FPR}, {@link #MAX_FPR}]
         * @throws MilvusClientException if fpr is out of range
         */
        public Builder(long n, double fpr) {
            validateFpr(fpr);
            int numBytes = optimalNumOfBytes(n, fpr);
            this.buf = new byte[HEADER_SIZE + numBytes];
            this.numBlocks = numBytes / BYTES_PER_BLOCK;
            this.nDeclared = n;
            this.fpr = fpr;
        }

        /** Inserts an integer value, hashed as its 8-byte little-endian encoding. */
        public Builder addInt64(long value) {
            domains |= DOMAIN_INT64;
            addHash(XXHash64.hashInt64(value));
            return this;
        }

        /** Inserts a string value, hashed as its raw UTF-8 bytes. */
        public Builder addString(String value) {
            if (value == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Bloom filter members cannot be null.");
            }
            domains |= DOMAIN_UTF8;
            addHash(XXHash64.hash(value.getBytes(StandardCharsets.UTF_8)));
            return this;
        }

        /** Returns the value domains inserted so far. Zero means nothing was inserted. */
        public byte getDomains() {
            return domains;
        }

        /** Returns the number of 32-byte blocks in the filter body. */
        public int getNumBlocks() {
            return numBlocks;
        }

        /**
         * Stamps the MBF1 header onto the filter and returns the envelope.
         *
         * <p>The returned array is the Builder's own buffer, not a copy — a filter therefore
         * costs one allocation of its final size rather than a body plus an equal-sized
         * serialization buffer, which matters at 64 MiB. Treat it as read-only, and copy it if
         * you keep inserting afterwards: a later {@code add} mutates an array already handed out.
         * Calling {@code build()} repeatedly is fine; each call re-stamps the header.
         */
        public byte[] build() {
            ByteBuffer header = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            header.put(MAGIC.getBytes(StandardCharsets.US_ASCII));
            header.putShort(VERSION);
            header.putShort(ALGO_PARQUET_SBBF_XXH64);
            header.putLong(nDeclared);
            header.putDouble(fpr);
            header.putInt(numBlocks);
            header.put(domains);
            // buf[29..31] stays zero (reserved), and the body is already in place.
            return buf;
        }

        /**
         * Sets this hash's eight bits directly in the final MBF1 buffer. Words are
         * read-modify-written little-endian byte by byte so the body keeps the spec's layout
         * regardless of the host's native byte order.
         */
        private void addHash(long hash) {
            int offset = HEADER_SIZE + blockIndex(hash, numBlocks) * BYTES_PER_BLOCK;
            int key = (int) hash;
            for (int i = 0; i < WORDS_PER_BLOCK; i++) {
                int mask = 1 << ((key * SALT[i]) >>> 27);
                int p = offset + i * 4;
                int word = (buf[p] & 0xFF)
                        | (buf[p + 1] & 0xFF) << 8
                        | (buf[p + 2] & 0xFF) << 16
                        | (buf[p + 3] & 0xFF) << 24;
                word |= mask;
                buf[p] = (byte) word;
                buf[p + 1] = (byte) (word >>> 8);
                buf[p + 2] = (byte) (word >>> 16);
                buf[p + 3] = (byte) (word >>> 24);
            }
        }
    }
}

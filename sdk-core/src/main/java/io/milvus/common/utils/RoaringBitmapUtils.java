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

import java.util.Arrays;
import java.util.List;

/**
 * Builds a client-side roaring bitmap blob for the
 * <code>roaring_match(field, {blob})</code> filter expression.
 *
 * <p>Where {@link BloomFilterUtils} trades exactness for size, this is the exact-membership
 * sibling: <code>roaring_match</code> never produces a false positive, and a dense integer set
 * compresses far better than a bloom filter of the same set — one million consecutive ids is a
 * 274-byte blob here versus roughly 2 MB as a bloom filter at a 0.005 false-positive rate.
 * Sparse random sets are the other way round, so pick per workload: a roaring blob's size follows
 * the value distribution, not the member count.
 * Pass the result through <code>filterTemplateValues</code>; it
 * travels as a native protobuf bytes value with no base64 inflation, and the server embeds it
 * verbatim after validating the envelope.
 *
 * <pre>{@code
 * byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(userIds);
 * QueryReq req = QueryReq.builder()
 *         .collectionName("docs")
 *         .filter("roaring_match(user_id, {ids})")
 *         .filterTemplateValues(Collections.singletonMap("ids", blob))
 *         .build();
 * }</pre>
 *
 * <p><b>Members are signed integers only</b>, and the target field must be an integer field (or an
 * integer-valued JSON path). A member is sign-extended to int64 and its two's-complement bit
 * pattern is reinterpreted as the unsigned bitmap key, so an <code>INT8</code> -1 and a
 * <code>long</code> -1 land on the same key <code>0xffffffffffffffff</code> and produce the same
 * blob. All ordering inside the bitmap is on that unsigned key, which is why <code>{-1, 5}</code>
 * is stored as <code>{5, 0xffffffffffffffff}</code>.
 *
 * <p>Building a large bitmap is the expensive part of a <code>roaring_match</code> query, so the
 * returned array is meant to be built once and reused across many requests rather than rebuilt per
 * call.
 *
 * <p>The body is the standard portable Roaring64 serialization (the same one CRoaring's
 * <code>portable</code> format and the Go <code>roaring/v2</code> library's
 * <code>WriteTo</code> emit), wrapped in the Milvus MRB1 envelope. Blobs built here are
 * byte-identical to the ones the Go SDK (<code>roaringfilter.Build</code>) produces for the same
 * members — the golden vectors in
 * <code>src/test/resources/roaring/roaring_golden_vectors.json</code> are generated from it and
 * pin that. The other SDKs are held to the same vectors as their support lands. See
 * <code>docs/design-docs/design_docs/20260714-roaring-exact-membership-expression.md</code> in the
 * milvus repository.
 */
public class RoaringBitmapUtils {
    /** The 4-byte MRB1 envelope magic. */
    public static final String MAGIC = "MRB1";
    /** The MRB1 envelope version implemented by this class. */
    public static final short VERSION = 1;
    /** Identifies the portable Roaring64 body format. */
    public static final short FORMAT_PORTABLE_ROARING64 = 1;
    /** Size in bytes of the MRB1 envelope header. */
    public static final int HEADER_SIZE = 32;

    /**
     * Maximum number of high (upper 32 bit) containers the server accepts. Reached only by
     * extremely sparse sets: it takes members spread over more than 262144 distinct 2^32 windows.
     */
    public static final int MAX_HIGH_CONTAINERS = 1 << 18;
    /**
     * Maximum estimated decoded size the server accepts, mirroring its own admission check.
     * The estimate is <code>body_length + 128 * high_containers + 64 * low_containers</code>:
     * the wire body plus the per-container bookkeeping the server allocates when it decodes.
     */
    public static final int MAX_DECODED_BYTES = 64 * 1024 * 1024;
    /** Maximum serialized body length, matching the default grpc receive limit. */
    public static final int MAX_BODY_BYTES = 128 * 1024 * 1024;

    /** Per-high-container decode overhead used by {@link #MAX_DECODED_BYTES}. */
    private static final int HIGH_CONTAINER_OVERHEAD = 128;
    /** Per-low-container decode overhead used by {@link #MAX_DECODED_BYTES}. */
    private static final int LOW_CONTAINER_OVERHEAD = 64;

    /** Roaring32 cookie for a bitmap with no run container. */
    private static final int COOKIE_NO_RUN = 12346;
    /** Roaring32 cookie for a bitmap containing at least one run container. */
    private static final int COOKIE_RUN = 12347;
    /** Largest cardinality still stored as an array container. */
    private static final int ARRAY_MAX_CARDINALITY = 4096;
    /** Serialized size of a bitmap container body: 1024 little-endian uint64 words. */
    private static final int BITMAP_BODY_BYTES = 8192;
    /**
     * A run-bearing Roaring32 omits the offset header below this many containers.
     * With four or more it writes it, exactly like a run-free one.
     */
    private static final int NO_OFFSET_THRESHOLD = 4;
    /**
     * The size a bitmap container is <i>assumed</i> to occupy when choosing between a run and a
     * bitmap encoding. It is deliberately not {@link #BITMAP_BODY_BYTES}: the reference
     * implementation compares against <code>bitmapContainerSizeInBytes()</code>, which is the
     * in-memory footprint <code>unsafe.Sizeof(bitmapContainer{}) + 65536/8</code> — 32 + 8192 on
     * every 64-bit platform. Using 8192 here instead flips the choice for containers with
     * 2048..2055 runs and makes this SDK emit blobs that differ from every other one. The golden
     * vectors <code>run_bitmap_tiebreak_run</code> and <code>run_bitmap_tiebreak_bitmap</code>
     * sit either side of that boundary.
     */
    private static final int BITMAP_CONTAINER_IN_MEMORY_BYTES = 32 + BITMAP_BODY_BYTES;

    private static final byte KIND_ARRAY = 0;
    private static final byte KIND_BITMAP = 1;
    private static final byte KIND_RUN = 2;

    /**
     * Sentinel for "no value seen yet" while scanning a container. It has to sit outside
     * <code>[-1, 65535]</code>, not merely outside <code>[0, 65535]</code>: the scans ask both
     * <code>value == previous</code> and <code>value == previous + 1</code>, so a -1 sentinel
     * would make the very first value of a container starting at 0 look like the continuation of
     * a run that never started, and every such container would be written with one run too few.
     */
    private static final long NO_PREVIOUS_VALUE = Long.MIN_VALUE;

    private RoaringBitmapUtils() {
    }

    /**
     * Builds a bitmap over the given members.
     *
     * <p>Members must be Byte/Short/Integer/Long; each is sign-extended to int64. Duplicates and
     * unsorted input are fine — the same set always produces the same bytes. An empty list
     * produces a valid 40-byte blob that matches nothing.
     *
     * @param members the membership set
     * @return the MRB1 blob to pass as a filter template value
     * @throws MilvusClientException if members is null, contains a null or a non-integer element,
     *                               or the resulting bitmap exceeds a server limit
     */
    public static byte[] buildRoaringBitmap(List<? extends Number> members) {
        if (members == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Roaring bitmap members cannot be null.");
        }
        long[] keys = new long[members.size()];
        int index = 0;
        // Iterated as Object, not Number: a raw-typed List can hold anything, and the loop's own
        // implicit cast would then throw a bare ClassCastException before the check below could
        // say which element is wrong.
        for (Object member : members) {
            keys[index] = toKey(member, index);
            index++;
        }
        return build(keys, keys.length);
    }

    /**
     * Builds a bitmap over the given members.
     *
     * @param members the membership set; duplicates and unsorted input are allowed
     * @return the MRB1 blob to pass as a filter template value
     * @throws MilvusClientException if members is null or the bitmap exceeds a server limit
     */
    public static byte[] buildRoaringBitmap(long[] members) {
        if (members == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Roaring bitmap members cannot be null.");
        }
        return build(Arrays.copyOf(members, members.length), members.length);
    }

    /**
     * Builds a bitmap over the given members, each sign-extended to int64.
     *
     * @param members the membership set; duplicates and unsorted input are allowed
     * @return the MRB1 blob to pass as a filter template value
     * @throws MilvusClientException if members is null or the bitmap exceeds a server limit
     */
    public static byte[] buildRoaringBitmap(int[] members) {
        if (members == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Roaring bitmap members cannot be null.");
        }
        long[] keys = new long[members.length];
        for (int i = 0; i < members.length; i++) {
            keys[i] = members[i];
        }
        return build(keys, keys.length);
    }

    /**
     * Incrementally collects members and serializes them into an MRB1 blob. Not thread-safe.
     *
     * <p>Prefer this over {@link #buildRoaringBitmap(List)} for very large membership sets:
     * {@link #add(long)} takes a primitive, so a 10M-member bitmap costs no boxed
     * {@code List<Long>} (which would be roughly 240 MB of {@code Long} objects on its own).
     *
     * <p>{@link #build()} may be called repeatedly and after further {@code add} calls; it returns
     * a fresh array each time and never mutates one it already handed out.
     */
    public static class Builder {
        private long[] keys;
        private int count;
        /** Whether {@link #keys} is already in ascending unsigned order. */
        private boolean sorted = true;

        /** Creates a builder with a default initial capacity. */
        public Builder() {
            this(16);
        }

        /**
         * Creates a builder sized for the expected member count, avoiding regrowth.
         *
         * @param expectedMembers the number of members that will be added; only a capacity hint,
         *                        adding more or fewer is allowed
         * @throws MilvusClientException if expectedMembers is negative
         */
        public Builder(int expectedMembers) {
            if (expectedMembers < 0) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Roaring bitmap expected member count cannot be negative: "
                                + expectedMembers + ".");
            }
            this.keys = new long[Math.max(expectedMembers, 1)];
        }

        /** Adds one member. Duplicates collapse when the bitmap is built. */
        public Builder add(long member) {
            ensureCapacity(count + 1);
            // Appending can only break the ordering; keeping the flag exact lets a caller that
            // already feeds sorted members skip the sort entirely.
            if (count > 0 && Long.compareUnsigned(keys[count - 1], member) > 0) {
                sorted = false;
            }
            keys[count++] = member;
            return this;
        }

        /** Adds every member of the array. */
        public Builder addAll(long[] members) {
            if (members == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Roaring bitmap members cannot be null.");
            }
            ensureCapacity(count + members.length);
            for (long member : members) {
                add(member);
            }
            return this;
        }

        /** Adds every member of the array, each sign-extended to int64. */
        public Builder addAll(int[] members) {
            if (members == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Roaring bitmap members cannot be null.");
            }
            ensureCapacity(count + members.length);
            for (int member : members) {
                add(member);
            }
            return this;
        }

        /**
         * Adds every member of the list.
         *
         * @throws MilvusClientException if members is null or holds a non-integer element
         */
        public Builder addAll(List<? extends Number> members) {
            if (members == null) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Roaring bitmap members cannot be null.");
            }
            ensureCapacity(count + members.size());
            int index = 0;
            for (Object member : members) {
                add(toKey(member, index));
                index++;
            }
            return this;
        }

        /** Returns the number of members added so far, duplicates included. */
        public int size() {
            return count;
        }

        /**
         * Serializes the members collected so far into an MRB1 blob.
         *
         * @return the MRB1 blob to pass as a filter template value
         * @throws MilvusClientException if the bitmap exceeds a server limit
         */
        public byte[] build() {
            if (!sorted) {
                sortUnsigned(keys, count);
                sorted = true;
            }
            return serialize(keys, count);
        }

        private void ensureCapacity(int required) {
            if (required <= keys.length) {
                return;
            }
            int capacity = keys.length;
            while (capacity < required) {
                // Cap the doubling so a large builder cannot overflow into a negative capacity.
                capacity = capacity > (Integer.MAX_VALUE >> 1) ? Integer.MAX_VALUE : capacity << 1;
            }
            keys = Arrays.copyOf(keys, capacity);
        }
    }

    /**
     * Widens one list element to its int64 bitmap key.
     *
     * <p>Boolean and String cannot reach here through the declared {@code List<? extends Number>},
     * but a raw-typed list can, so the element type is checked rather than trusted. Float, Double
     * and BigInteger are rejected outright: silently truncating 2.5 to 2, or wrapping a 2^64
     * BigInteger, would build a bitmap that quietly matches the wrong rows.
     */
    private static long toKey(Object member, int index) {
        if (member instanceof Long || member instanceof Integer
                || member instanceof Short || member instanceof Byte) {
            return ((Number) member).longValue();
        }
        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                "Roaring bitmap members must be signed integers (Byte/Short/Integer/Long), "
                        + "but element %d is %s.",
                index, member == null ? "null" : member.getClass().getSimpleName()));
    }

    /** Sorts, then serializes; {@code keys} is scratch owned by the caller and is reordered. */
    private static byte[] build(long[] keys, int length) {
        sortUnsigned(keys, length);
        return serialize(keys, length);
    }

    /**
     * Sorts keys into ascending <b>unsigned</b> order — the order the bitmap format is defined in,
     * under which {@code -1} is the largest key rather than the smallest.
     *
     * <p>Flipping the sign bit maps the unsigned order onto the signed one, which lets the
     * primitive {@link Arrays#sort(long[], int, int)} do the work; sorting by
     * {@link Long#compareUnsigned} directly would need a {@code Comparator} and therefore a boxed
     * {@code Long} per member. The two orders are identical, and a test asserts it.
     */
    private static void sortUnsigned(long[] keys, int length) {
        for (int i = 0; i < length; i++) {
            keys[i] ^= Long.MIN_VALUE;
        }
        Arrays.sort(keys, 0, length);
        for (int i = 0; i < length; i++) {
            keys[i] ^= Long.MIN_VALUE;
        }
    }

    /** The container structure of one bitmap, measured before any body is allocated. */
    private static final class Layout {
        /** Upper 32 bits of each high container's key, ascending. */
        int[] highKeys;
        /** Index into the container arrays where each high container's low containers start. */
        int[] highContainerOffsets;
        int highCount;

        /** Bits 16..31 of the keys in this low container. */
        int[] containerKeys;
        /** Distinct value count, always at least 1. */
        int[] containerCardinalities;
        /** Number of maximal consecutive runs. */
        int[] containerRuns;
        /** Where this container's keys start in the sorted key array. */
        int[] containerKeyStarts;
        /** One past where this container's keys end in the sorted key array. */
        int[] containerKeyEnds;
        byte[] containerKinds;
        int containerCount;

        long cardinality;
        int bodyLength;
    }

    private static byte[] serialize(long[] sortedKeys, int length) {
        Layout layout = measure(sortedKeys, length);

        byte[] blob = new byte[HEADER_SIZE + layout.bodyLength];
        blob[0] = 'M';
        blob[1] = 'R';
        blob[2] = 'B';
        blob[3] = '1';
        putUint16(blob, 4, VERSION);
        putUint16(blob, 6, FORMAT_PORTABLE_ROARING64);
        putUint64(blob, 8, layout.cardinality);
        putUint64(blob, 16, layout.bodyLength);
        // blob[24..31] stays zero (reserved); the server rejects a blob that sets it.

        int position = HEADER_SIZE;
        putUint64(blob, position, layout.highCount);
        position += 8;
        for (int high = 0; high < layout.highCount; high++) {
            putUint32(blob, position, layout.highKeys[high]);
            position += 4;
            position = writeRoaring32(blob, position, sortedKeys, layout,
                    layout.highContainerOffsets[high], layout.highContainerOffsets[high + 1]);
        }

        if (position != blob.length) {
            // Unreachable: the measuring pass and the writing pass would have to disagree. It is
            // checked anyway because a short body is a silently corrupt blob, not a crash.
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, String.format(
                    "Roaring bitmap wrote %d body bytes but measured %d.",
                    position - HEADER_SIZE, layout.bodyLength));
        }
        return blob;
    }

    /**
     * Walks the sorted keys twice — once to count containers, once to describe them — so an
     * oversized bitmap is rejected before anything the size of the body is allocated.
     */
    private static Layout measure(long[] sortedKeys, int length) {
        Layout layout = new Layout();

        int highCount = 0;
        int containerCount = 0;
        long cardinality = 0;
        for (int i = 0; i < length; ) {
            int high = (int) (sortedKeys[i] >>> 32);
            highCount++;
            do {
                int containerKey = (int) ((sortedKeys[i] >>> 16) & 0xFFFF);
                containerCount++;
                long previous = NO_PREVIOUS_VALUE;
                do {
                    long value = sortedKeys[i] & 0xFFFF;
                    if (value != previous) {
                        cardinality++;
                        previous = value;
                    }
                    i++;
                } while (i < length && (int) ((sortedKeys[i] >>> 16) & 0xFFFF) == containerKey
                        && (int) (sortedKeys[i] >>> 32) == high);
            } while (i < length && (int) (sortedKeys[i] >>> 32) == high);
        }

        if (highCount > MAX_HIGH_CONTAINERS) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                    "Roaring bitmap high-container count %d exceeds maximum %d.",
                    highCount, MAX_HIGH_CONTAINERS));
        }
        // The body is at least the 8-byte high-container count, so this is a genuine lower bound
        // on the estimate computed below. Checking it here keeps a set that is already far over
        // the limit from allocating per-container bookkeeping first.
        long minimumEstimate = 8L + (long) HIGH_CONTAINER_OVERHEAD * highCount
                + (long) LOW_CONTAINER_OVERHEAD * containerCount;
        if (minimumEstimate > MAX_DECODED_BYTES) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                    "Roaring bitmap estimated decoded size %d exceeds maximum %d.",
                    minimumEstimate, MAX_DECODED_BYTES));
        }

        layout.highCount = highCount;
        layout.containerCount = containerCount;
        layout.cardinality = cardinality;
        layout.highKeys = new int[highCount];
        layout.highContainerOffsets = new int[highCount + 1];
        layout.containerKeys = new int[containerCount];
        layout.containerCardinalities = new int[containerCount];
        layout.containerRuns = new int[containerCount];
        layout.containerKeyStarts = new int[containerCount];
        layout.containerKeyEnds = new int[containerCount];
        layout.containerKinds = new byte[containerCount];

        long bodyLength = 8;
        int highIndex = 0;
        int containerIndex = 0;
        for (int i = 0; i < length; ) {
            int high = (int) (sortedKeys[i] >>> 32);
            layout.highKeys[highIndex] = high;
            layout.highContainerOffsets[highIndex] = containerIndex;
            int groupFirstContainer = containerIndex;
            do {
                int containerKey = (int) ((sortedKeys[i] >>> 16) & 0xFFFF);
                int start = i;
                int distinct = 0;
                int runs = 0;
                long previous = NO_PREVIOUS_VALUE;
                do {
                    long value = sortedKeys[i] & 0xFFFF;
                    if (value != previous) {
                        distinct++;
                        // Values arrive ascending, so a run continues exactly when this value is
                        // the previous one plus one.
                        if (value != previous + 1) {
                            runs++;
                        }
                        previous = value;
                    }
                    i++;
                } while (i < length && (int) ((sortedKeys[i] >>> 16) & 0xFFFF) == containerKey
                        && (int) (sortedKeys[i] >>> 32) == high);

                layout.containerKeys[containerIndex] = containerKey;
                layout.containerCardinalities[containerIndex] = distinct;
                layout.containerRuns[containerIndex] = runs;
                layout.containerKeyStarts[containerIndex] = start;
                layout.containerKeyEnds[containerIndex] = i;
                layout.containerKinds[containerIndex] = decideKind(distinct, runs);
                containerIndex++;
            } while (i < length && (int) (sortedKeys[i] >>> 32) == high);

            bodyLength += 4 + roaring32Size(layout, groupFirstContainer, containerIndex);
            highIndex++;
        }
        layout.highContainerOffsets[highCount] = containerIndex;

        if (bodyLength > MAX_BODY_BYTES) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                    "Roaring bitmap body too large: %d bytes exceeds maximum %d.",
                    bodyLength, MAX_BODY_BYTES));
        }
        long estimated = bodyLength + (long) HIGH_CONTAINER_OVERHEAD * highCount
                + (long) LOW_CONTAINER_OVERHEAD * containerCount;
        if (estimated > MAX_DECODED_BYTES) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, String.format(
                    "Roaring bitmap estimated decoded size %d exceeds maximum %d.",
                    estimated, MAX_DECODED_BYTES));
        }
        layout.bodyLength = (int) bodyLength;
        return layout;
    }

    /**
     * Picks the encoding for one 16-bit container, exactly as the reference implementation does.
     * Note that the run/bitmap comparison is against the in-memory
     * {@link #BITMAP_CONTAINER_IN_MEMORY_BYTES}, not the 8192-byte serialized body — see that
     * constant. A run encoding can therefore win while still being larger on the wire.
     */
    private static byte decideKind(int cardinality, int runs) {
        int sizeAsRun = 2 + 4 * runs;
        int sizeAsArray = 2 * cardinality;
        int limit = Math.min(BITMAP_CONTAINER_IN_MEMORY_BYTES, sizeAsArray);
        if (sizeAsRun < limit) {
            return KIND_RUN;
        }
        return cardinality <= ARRAY_MAX_CARDINALITY ? KIND_ARRAY : KIND_BITMAP;
    }

    private static int containerBodySize(Layout layout, int container) {
        switch (layout.containerKinds[container]) {
            case KIND_RUN:
                return 2 + 4 * layout.containerRuns[container];
            case KIND_BITMAP:
                return BITMAP_BODY_BYTES;
            default:
                return 2 * layout.containerCardinalities[container];
        }
    }

    private static boolean hasRun(Layout layout, int from, int to) {
        for (int container = from; container < to; container++) {
            if (layout.containerKinds[container] == KIND_RUN) {
                return true;
            }
        }
        return false;
    }

    /** Serialized size of the portable Roaring32 blob covering containers {@code [from, to)}. */
    private static int roaring32Size(Layout layout, int from, int to) {
        int containers = to - from;
        boolean hasRun = hasRun(layout, from, to);
        int size = hasRun ? 4 + (containers + 7) / 8 : 8;
        size += 4 * containers; // descriptive header: key + cardinality-1 per container
        if (!hasRun || containers >= NO_OFFSET_THRESHOLD) {
            size += 4 * containers; // offset header
        }
        for (int container = from; container < to; container++) {
            size += containerBodySize(layout, container);
        }
        return size;
    }

    /** Writes the portable Roaring32 blob for containers {@code [from, to)}; returns the new position. */
    private static int writeRoaring32(byte[] blob, int position, long[] sortedKeys, Layout layout,
                                      int from, int to) {
        int start = position;
        int containers = to - from;
        boolean hasRun = hasRun(layout, from, to);

        int cookieSize;
        int runBitmapSize;
        if (hasRun) {
            cookieSize = 4;
            runBitmapSize = (containers + 7) / 8;
            putUint16(blob, position, COOKIE_RUN);
            putUint16(blob, position + 2, containers - 1);
            position += 4;
            for (int i = 0; i < containers; i++) {
                if (layout.containerKinds[from + i] == KIND_RUN) {
                    blob[position + i / 8] |= (byte) (1 << (i % 8));
                }
            }
            position += runBitmapSize;
        } else {
            cookieSize = 8;
            runBitmapSize = 0;
            putUint32(blob, position, COOKIE_NO_RUN);
            putUint32(blob, position + 4, containers);
            position += 8;
        }

        for (int container = from; container < to; container++) {
            putUint16(blob, position, layout.containerKeys[container]);
            // The descriptive header stores cardinality minus one, which is why a container can
            // hold all 65536 values and still fit a uint16 here.
            putUint16(blob, position + 2, layout.containerCardinalities[container] - 1);
            position += 4;
        }

        if (!hasRun || containers >= NO_OFFSET_THRESHOLD) {
            // Offsets are relative to the start of this Roaring32 blob, not the MRB1 blob.
            int offset = cookieSize + runBitmapSize + 8 * containers;
            for (int container = from; container < to; container++) {
                putUint32(blob, position, offset);
                position += 4;
                offset += containerBodySize(layout, container);
            }
        }

        for (int container = from; container < to; container++) {
            int keyStart = layout.containerKeyStarts[container];
            int keyEnd = layout.containerKeyEnds[container];
            switch (layout.containerKinds[container]) {
                case KIND_RUN: {
                    putUint16(blob, position, layout.containerRuns[container]);
                    position += 2;
                    long previous = NO_PREVIOUS_VALUE;
                    int runStart = 0;
                    for (int i = keyStart; i < keyEnd; i++) {
                        long value = sortedKeys[i] & 0xFFFF;
                        if (value == previous) {
                            continue;
                        }
                        if (value != previous + 1) {
                            if (previous != NO_PREVIOUS_VALUE) {
                                putUint16(blob, position, runStart);
                                putUint16(blob, position + 2, (int) previous - runStart);
                                position += 4;
                            }
                            runStart = (int) value;
                        }
                        previous = value;
                    }
                    // The last run is only closed once the scan ends.
                    putUint16(blob, position, runStart);
                    putUint16(blob, position + 2, (int) previous - runStart);
                    position += 4;
                    break;
                }
                case KIND_BITMAP: {
                    // The body is 1024 little-endian uint64 words, so word v>>6 / bit v&63 is
                    // byte v>>3 / bit v&7 of the body. The array is already zero filled.
                    for (int i = keyStart; i < keyEnd; i++) {
                        int value = (int) (sortedKeys[i] & 0xFFFF);
                        blob[position + (value >>> 3)] |= (byte) (1 << (value & 7));
                    }
                    position += BITMAP_BODY_BYTES;
                    break;
                }
                default: {
                    long previous = NO_PREVIOUS_VALUE;
                    for (int i = keyStart; i < keyEnd; i++) {
                        long value = sortedKeys[i] & 0xFFFF;
                        if (value == previous) {
                            continue;
                        }
                        putUint16(blob, position, (int) value);
                        position += 2;
                        previous = value;
                    }
                    break;
                }
            }
        }

        if (position - start != roaring32Size(layout, from, to)) {
            throw new MilvusClientException(ErrorCode.CLIENT_ERROR, String.format(
                    "Roaring bitmap wrote %d bytes for a group measured at %d.",
                    position - start, roaring32Size(layout, from, to)));
        }
        return position;
    }

    private static void putUint16(byte[] blob, int position, int value) {
        blob[position] = (byte) value;
        blob[position + 1] = (byte) (value >>> 8);
    }

    private static void putUint32(byte[] blob, int position, int value) {
        blob[position] = (byte) value;
        blob[position + 1] = (byte) (value >>> 8);
        blob[position + 2] = (byte) (value >>> 16);
        blob[position + 3] = (byte) (value >>> 24);
    }

    private static void putUint64(byte[] blob, int position, long value) {
        for (int i = 0; i < 8; i++) {
            blob[position + i] = (byte) (value >>> (8 * i));
        }
    }
}

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

package io.milvus.orm.iterator;

import io.milvus.exception.ParamException;
import io.milvus.grpc.QueryCursor;

/**
 * A serializable snapshot of a {@link QueryIterator} position, used to resume
 * pagination from where a previous iterator left off (pymilvus parity:
 * {@code QueryIterator.get_cursor()} / {@code QueryIteratorCursor}).
 *
 * <p>The cursor captures the session timestamp of the original iterator, the last
 * primary key returned, and (for element-filter iterators) the last matched element
 * offset. Resume by passing it back through {@code QueryIteratorReq.cursor(...)}.
 */


public class QueryIteratorCursor {
    private final long sessionTs;
    private final Long intPk;
    private final String strPk;
    private final Long lastElementOffset;

    private QueryIteratorCursor(QueryIteratorCursorBuilder builder) {
        this.sessionTs = builder.sessionTs;
        this.intPk = builder.intPk;
        this.strPk = builder.strPk;
        this.lastElementOffset = builder.lastElementOffset;
    }

    /**
     * Creates a new builder.
     *
     * @return the builder
     */


    public static QueryIteratorCursorBuilder builder() {
        return new QueryIteratorCursorBuilder();
    }

    /**
     * Returns the sessionTs.
     *
     * @return the sessionTs
     */


    public long getSessionTs() {
        return sessionTs;
    }

    /**
     * Returns the intPk.
     *
     * @return the intPk
     */


    public Long getIntPk() {
        return intPk;
    }

    /**
     * Returns the strPk.
     *
     * @return the strPk
     */


    public String getStrPk() {
        return strPk;
    }

    /**
     * Returns the lastElementOffset.
     *
     * @return the lastElementOffset
     */


    public Long getLastElementOffset() {
        return lastElementOffset;
    }

    /**
     * Serialize this cursor to the gRPC {@code QueryCursor} message.
     *
     * <p>The gRPC {@code QueryCursor} message has no field for the last element offset, so
     * serializing an element-filter cursor would silently drop the element position and cause
     * the resumed iterator to skip rows. Reject such cursors to avoid the data loss.
     *
     * @return the serialized gRPC {@code QueryCursor} message
     * @throws ParamException if this cursor carries an element offset
     */


    public QueryCursor toProto() {
        if (lastElementOffset != null) {
            throw new ParamException("Cannot serialize an element-filter cursor to QueryCursor: "
                    + "the gRPC message has no element-offset field, resuming would skip rows. "
                    + "Resume in memory via QueryIteratorReq.cursor(...) instead.");
        }
        QueryCursor.Builder builder = QueryCursor.newBuilder().setSessionTs(sessionTs);
        if (strPk != null) {
            builder.setStrPk(strPk);
        } else if (intPk != null) {
            builder.setIntPk(intPk);
        }
        return builder.build();
    }

    /**
     * Reconstruct a cursor from a gRPC {@code QueryCursor} message, or {@code null}
     * when the input is null.
     *
     * @param cursor the gRPC {@code QueryCursor} message, or {@code null}
     * @return the reconstructed cursor, or {@code null} when the input is {@code null}
     */


    public static QueryIteratorCursor fromProto(QueryCursor cursor) {
        if (cursor == null) {
            return null;
        }
        QueryIteratorCursorBuilder builder = QueryIteratorCursor.builder()
                .sessionTs(cursor.getSessionTs());
        switch (cursor.getCursorPkCase()) {
            case STR_PK:
                builder.strPk(cursor.getStrPk());
                break;
            case INT_PK:
                builder.intPk(cursor.getIntPk());
                break;
            default:
                break;
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "QueryIteratorCursor{" +
                "sessionTs=" + sessionTs +
                ", intPk=" + intPk +
                ", strPk='" + strPk + '\'' +
                ", lastElementOffset=" + lastElementOffset +
                '}';
    }

    /**
     * Builder for {@link QueryIteratorCursor} class.
     */


    public static class QueryIteratorCursorBuilder {
        private long sessionTs;
        private Long intPk;
        private String strPk;
        private Long lastElementOffset;

        /**
         * Sets the sessionTs.
         *
         * @param sessionTs the sessionTs
         * @return this builder
         */


        public QueryIteratorCursorBuilder sessionTs(long sessionTs) {
            this.sessionTs = sessionTs;
            return this;
        }

        /**
         * Sets the intPk.
         *
         * @param intPk the intPk
         * @return this builder
         */


        public QueryIteratorCursorBuilder intPk(Long intPk) {
            this.intPk = intPk;
            return this;
        }

        /**
         * Sets the strPk.
         *
         * @param strPk the strPk
         * @return this builder
         */


        public QueryIteratorCursorBuilder strPk(String strPk) {
            this.strPk = strPk;
            return this;
        }

        /**
         * Sets the lastElementOffset.
         *
         * @param lastElementOffset the lastElementOffset
         * @return this builder
         */


        public QueryIteratorCursorBuilder lastElementOffset(Long lastElementOffset) {
            this.lastElementOffset = lastElementOffset;
            return this;
        }

        /**
         * Builds the QueryIteratorCursor.
         *
         * @return the built QueryIteratorCursor
         */


        public QueryIteratorCursor build() {
            return new QueryIteratorCursor(this);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Base class for Ignite cache queries which can merge tables.
 *
 */
public abstract class MergeQuery<R> extends Query<R> {
    /** Maximum number of SQL result rows which can be fetched into a merge table. */
    public static final int DFLT_SQL_MERGE_TABLE_MAX_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, 10_000);

    /**
     * Number of SQL result rows that will be fetched into a merge table
     * at once before applying binary search for the bounds.
     */
    public static final int DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, 1024);

    static {
        if (!U.isPow2(DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE)) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE +
                " (" + DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE + ") must be positive and a power of 2.");
        }

        if (DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE >= DFLT_SQL_MERGE_TABLE_MAX_SIZE) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE +
                " (" + DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE + ") must be less than " + IGNITE_SQL_MERGE_TABLE_MAX_SIZE +
                " (" + DFLT_SQL_MERGE_TABLE_MAX_SIZE + ").");
        }
    }


    /** Maximum number of SQL result rows which can be fetched into a merge table. */
    private int sqlMergeTableMaxSize = DFLT_SQL_MERGE_TABLE_MAX_SIZE;

    /**
     * Number of SQL result rows that will be fetched into a merge table
     * at once before applying binary search for the bounds.
     */
    private int sqlMergeTablePrefetchSize = DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE;


    /**
     * Gets property controlling maximum number of SQL result rows which can be fetched into a merge table.
     * If there are less rows than this threshold then multiple passes throw a table will be possible,
     * otherwise only one pass (e.g. only result streaming is possible).
     *
     * @return Maximum number of SQL result rows which can be fetched into a merge table.
     */
    public int getSqlMergeTableMaxSize() {
        return sqlMergeTableMaxSize;
    }

    /**
     * Sets property controlling maximum number of SQL result rows which can be fetched into a merge table.
     * If there are less rows than this threshold then multiple passes throw a table will be possible,
     * otherwise only one pass (e.g. only result streaming is possible).
     *
     *  @param sqlMergeTableMaxSize Maximum number of SQL result rows which can be fetched into a merge table.
     *  @return {@code this} for chaining.
     */
    public MergeQuery<R> setSqlMergeTableMaxSize(int sqlMergeTableMaxSize) {
        if (this.sqlMergeTablePrefetchSize >= sqlMergeTableMaxSize) {
            throw new IllegalArgumentException("Cache configuration parameter sqlMergeTableMaxSize (" +
                sqlMergeTableMaxSize + ") must be greater than sqlMergeTablePrefetchSize (" +
                this.sqlMergeTablePrefetchSize + ").");
        }

        this.sqlMergeTableMaxSize = sqlMergeTableMaxSize;

        return this;
    }

    /**
     * Gets number of SQL result rows that will be fetched into a merge table
     * at once before applying binary search for the bounds.
     *
     * @return Number of SQL result rows that will be fetched into a merge table.
     */
    public int getSqlMergeTablePrefetchSize() {
        return sqlMergeTablePrefetchSize;
    }

    /**
     * Sets number of SQL result rows that will be fetched into a merge table
     * at once before applying binary search for the bounds.
     *
     *  @param sqlMergeTablePrefetchSize Number of SQL result rows that will be fetched into a merge table.
     *  @return {@code this} for chaining.
     */
    public MergeQuery<R> setSqlMergeTablePrefetchSize(int sqlMergeTablePrefetchSize) {
        if (!U.isPow2(sqlMergeTablePrefetchSize)) {
            throw new IllegalArgumentException("Cache configuration parameter sqlMergeTablePrefetchSize (" +
                sqlMergeTablePrefetchSize + ") must be positive and a power of 2.");
        }

        if (sqlMergeTablePrefetchSize >= this.sqlMergeTableMaxSize) {
            throw new IllegalArgumentException("Cache configuration parameter sqlMergeTablePrefetchSize (" +
                sqlMergeTablePrefetchSize + ") must be less than sqlMergeTableMaxSize (" +
                this.sqlMergeTableMaxSize + ").");
        }

        this.sqlMergeTablePrefetchSize = sqlMergeTablePrefetchSize;

        return this;
    }
}

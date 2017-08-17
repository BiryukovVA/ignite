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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DFLT_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery {
    /** */
    public static final int DFLT_PAGE_SIZE = 1000;

    /** */
    @GridToStringInclude
    private List<GridCacheSqlQuery> mapQrys = new ArrayList<>();

    /** */
    @GridToStringInclude
    private GridCacheSqlQuery rdc;

    /** */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Maximum number of SQL result rows which can be fetched into a merge table. */
    private int sqlMergeTblMaxSize = DFLT_SQL_MERGE_TABLE_MAX_SIZE;

    /**
     * Number of SQL result rows that will be fetched into a merge table at once before applying binary search for the
     * bounds.
     */
    private int sqlMergeTblPrefetchSize = DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE;

    /** */
    private boolean explain;

    /** */
    private String originalSql;

    /** */
    private Set<QueryTable> tbls;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean skipMergeTbl;

    /** */
    private List<Integer> cacheIds;

    /** */
    private boolean local;

    /** */
    private CacheQueryPartitionInfo[] derivedPartitions;

    /**
     * @param originalSql Original query SQL.
     * @param tbls Tables in query.
     */
    public GridCacheTwoStepQuery(String originalSql, Set<QueryTable> tbls) {
        this.originalSql = originalSql;
        this.tbls = tbls;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * @param distributedJoins Distributed joins enabled.
     */
    public void distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joins enabled.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return {@code True} if reduce query can skip merge table creation and get data directly from merge index.
     */
    public boolean skipMergeTable() {
        return skipMergeTbl;
    }

    /**
     * @param skipMergeTbl Skip merge table.
     */
    public void skipMergeTable(boolean skipMergeTbl) {
        this.skipMergeTbl = skipMergeTbl;
    }

    /**
     * @return If this is explain query.
     */
    public boolean explain() {
        return explain;
    }

    /**
     * @param explain If this is explain query.
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Maximum number of SQL result rows which can be fetched into a merge table.
     */
    public int sqlMergeTableMaxSize() {
        return sqlMergeTblMaxSize;
    }

    /**
     * @param sqlMergeTblMaxSize New maximum number of SQL result rows which can be fetched into a merge table. Must be
     * positive and greater than {@link GridCacheTwoStepQuery#sqlMergeTblPrefetchSize}.
     */
    public void sqlMergeTableMaxSize(int sqlMergeTblMaxSize) {
        this.sqlMergeTblMaxSize = sqlMergeTblMaxSize;
    }

    /**
     * @return Number of SQL result rows that will be fetched into a merge table at once before applying binary search
     * for the bounds.
     */
    public int sqlMergeTablePrefetchSize() {
        return sqlMergeTblPrefetchSize;
    }

    /**
     * @param sqlMergeTblPrefetchSize New number of SQL result rows that will be fetched into a merge table at once
     * before applying binary search for the bounds. Must be positive and power of 2 and less than {@link
     * GridCacheTwoStepQuery#sqlMergeTblMaxSize}.
     */
    public void sqlMergeTablePrefetchSize(int sqlMergeTblPrefetchSize) {
        this.sqlMergeTblPrefetchSize = sqlMergeTblPrefetchSize;
    }

    /**
     * @param qry SQL Query.
     */
    public void addMapQuery(GridCacheSqlQuery qry) {
        mapQrys.add(qry);
    }

    /**
     * @return {@code true} If all the map queries contain only replicated tables.
     */
    public boolean isReplicatedOnly() {
        assert !mapQrys.isEmpty();

        for (GridCacheSqlQuery mapQry : mapQrys) {
            if (mapQry.isPartitioned())
                return false;
        }

        return true;
    }

    /**
     * @return Reduce query.
     */
    public GridCacheSqlQuery reduceQuery() {
        return rdc;
    }

    /**
     * @param rdc Reduce query.
     */
    public void reduceQuery(GridCacheSqlQuery rdc) {
        this.rdc = rdc;
    }

    /**
     * @return Map queries.
     */
    public List<GridCacheSqlQuery> mapQueries() {
        return mapQrys;
    }

    /**
     * @return Cache IDs.
     */
    public List<Integer> cacheIds() {
        return cacheIds;
    }

    /**
     * @param cacheIds Cache IDs.
     */
    public void cacheIds(List<Integer> cacheIds) {
        this.cacheIds = cacheIds;
    }

    /**
     * @return Original query SQL.
     */
    public String originalSql() {
        return originalSql;
    }

    /**
     * @return {@code True} If query is local.
     */
    public boolean isLocal() {
        return local;
    }

    /**
     * @param local Local query flag.
     */
    public void local(boolean local) {
        this.local = local;
    }

    /**
     * @return Query derived partitions info.
     */
    public CacheQueryPartitionInfo[] derivedPartitions() {
        return this.derivedPartitions;
    }

    /**
     * @param derivedPartitions Query derived partitions info.
     */
    public void derivedPartitions(CacheQueryPartitionInfo[] derivedPartitions) {
        this.derivedPartitions = derivedPartitions;
    }

    /**
     * @return Copy.
     */
    public GridCacheTwoStepQuery copy() {
        assert !explain;

        GridCacheTwoStepQuery cp = new GridCacheTwoStepQuery(originalSql, tbls);

        cp.cacheIds = cacheIds;
        cp.rdc = rdc.copy();
        cp.skipMergeTbl = skipMergeTbl;
        cp.pageSize = pageSize;
        cp.distributedJoins = distributedJoins;
        cp.derivedPartitions = derivedPartitions;
        cp.local = local;

        for (int i = 0; i < mapQrys.size(); i++)
            cp.mapQrys.add(mapQrys.get(i).copy());

        return cp;
    }

    /**
     * @return Nuumber of tables.
     */
    public int tablesCount() {
        return tbls.size();
    }

    /**
     * @return Tables.
     */
    public Set<QueryTable> tables() {
        return tbls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}

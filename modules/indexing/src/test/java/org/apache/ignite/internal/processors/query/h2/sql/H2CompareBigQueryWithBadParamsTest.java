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

package org.apache.ignite.internal.processors.query.h2.sql;

import javax.cache.CacheException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Executes SQL big query test using cache configuration with small property controlling maximum
 * number of SQL result rows which can be fetched into a merge table.
 */
public class H2CompareBigQueryWithBadParamsTest extends H2CompareBigQueryTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(
        String name, CacheMode mode, Class<?> clsK, Class<?> clsV) {

        return super.cacheConfiguration(name, mode, clsK, clsV)
            .setSqlMergeTablePrefetchSize(1)
            .setSqlMergeTableMaxSize(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Override public void testBigQuery() throws Exception {
        try {
            super.testBigQuery();
            fail("Test must fail");
        }
        catch (CacheException e) {
            assertTrue(e.getCause().getCause().getMessage().contains("Fetched result set was too large"));
        }
    }
}

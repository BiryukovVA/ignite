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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class DataStreamProcessorClassLoaderTest extends GridCommonAbstractTest {
    /** Test p2p entry processor. */
    private static final String TEST_ENTRY_PROCESSOR = "org.apache.ignite.tests.p2p.CacheDeploymentEntryProcessor";

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testClassLoading() throws Exception {
        ClassLoader ldr = getExternalClassLoader();
        Class processorCls = ldr.loadClass(TEST_ENTRY_PROCESSOR);

        try {
            Ignite grid = startGrid(0);
            startGrid(1);//Remote grid.
            CacheEntryProcessor processor = (CacheEntryProcessor)processorCls.newInstance();

            IgniteCache<String, Long> cache = grid.getOrCreateCache("myCache");
            IgniteDataStreamer<Object, Object> dataLdr = grid.dataStreamer(cache.getName());

            dataLdr.allowOverwrite(true);
            dataLdr.receiver(StreamTransformer.from(processor));
            dataLdr.addData("word", "test");
            dataLdr.close(false);
        }
        finally {
            stopAllGrids();
        }
    }
}

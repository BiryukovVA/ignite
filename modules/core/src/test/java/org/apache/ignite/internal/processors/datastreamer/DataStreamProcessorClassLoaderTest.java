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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
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

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testClassLoading() throws Exception {
        try {
            Ignite grid = startGrid(0);
            startGrid(1);//Remote grid.
            CacheEntryProcessor processor = loadCacheEntryProcessor();

            IgniteCache<String, Long> cache = grid.getOrCreateCache("mycache");
            IgniteDataStreamer<Object, Object> dataLdr = grid.dataStreamer(cache.getName());

            dataLdr.allowOverwrite(true);
            dataLdr.receiver(StreamTransformer.from(processor));
            dataLdr.addData("word", 1L);
            dataLdr.close(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return loaded CacheEntryProcessor
     * @throws Exception If failed.
     */
    private CacheEntryProcessor loadCacheEntryProcessor() throws Exception {
        File file = new File("src/test/resources/classes");
        URL url = file.toURI().toURL();
        URL[] urls = new URL[] {url};

        ClassLoader clsLdr = new URLClassLoader(urls);
        Class cls = clsLdr.loadClass("CacheEntryProcessorTestImpl");
        return (CacheEntryProcessor)cls.newInstance();
    }
}

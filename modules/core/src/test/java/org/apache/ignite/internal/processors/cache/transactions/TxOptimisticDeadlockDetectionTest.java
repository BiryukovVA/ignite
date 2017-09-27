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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.util.typedef.X.cause;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxOptimisticDeadlockDetectionTest extends AbstractDeadlockDetectionTest {
    /** Ip finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count (actually two times more nodes will started: server + client). */
    private static final int NODES_CNT = 4;

    /** Ordinal start key. */
    private static final Integer ORDINAL_START_KEY = 1;

    /** Custom start key. */
    private static final IncrementalTestObject CUSTOM_START_KEY = new KeyObject(1);

    /** Client mode flag. */
    private static boolean client;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        if (isDebug()) {
            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        TcpCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setClientMode(client);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGrids(NODES_CNT);

        client = true;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i + NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitioned() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), ORDINAL_START_KEY);
//            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), CUSTOM_START_KEY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitionedNear() throws Exception {
//        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, true), ORDINAL_START_KEY);
//            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), CUSTOM_START_KEY);
//        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksReplicated() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), ORDINAL_START_KEY);
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), CUSTOM_START_KEY);
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write sync mode.
     * @param near Near.
     * @return Created cache.
     */
    @SuppressWarnings("unchecked")
    private IgniteCache createCache(CacheMode cacheMode, CacheWriteSynchronizationMode syncMode, boolean near) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);
        ccfg.setWriteSynchronizationMode(syncMode);

        IgniteCache cache = ignite(0).createCache(ccfg);

        if (near) {
            for (int i = 0; i < NODES_CNT; i++) {
                Ignite client = ignite(i + NODES_CNT);

                assertTrue(client.configuration().isClientMode());

                client.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
            }
        }

        return cache;
    }

    /**
     * @param cache Cache.
     * @param startKey Transformer Start key.
     * @throws Exception If failed.
     */
    private void doTestDeadlocks(IgniteCache cache, Object startKey) throws Exception {
        try {
            awaitPartitionMapExchange();

//            doTestDeadlock(3, true, true, startKey);
            doTestDeadlock(3, false, false, startKey);
//            doTestDeadlock(3, false, true, startKey);

//            doTestDeadlock(4, true, true, startKey);
//            doTestDeadlock(4, false, false, startKey);
//            doTestDeadlock(4, false, true, startKey);
        }
        catch (Throwable e) {
            U.error(log, "Unexpected exception: ", e);

            fail();
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlock(
        final int txCnt,
        boolean lockPrimaryFirst,
        final boolean clientTx,
        Object startKey
    ) throws Exception {
        log.info(">>> Test deadlock [txCnt=" + txCnt + ", lockPrimaryFirst=" + lockPrimaryFirst +
            ", clientTx=" + clientTx + ", startKey=" + startKey + ']');

        CyclicBarrier barrier = new CyclicBarrier(txCnt);

        for (int i = 0; i < txCnt; i++) {
            GridCacheSharedContext<Object, Object> ctx = grid(i).context().cache().context();

            IgniteTxManager tm = ctx.tm();

            TxDeadlockDetection dd = GridTestUtils.getFieldValue(tm, "txDeadlockDetection");

            TxDeadlockDetection detection = new TxDeadlockDetection(ctx) {


                @Override TxDeadlockFuture detectDeadlock(IgniteInternalTx tx, Set<IgniteTxKey> keys) {
                    try {
                        barrier.await();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    return dd.detectDeadlock(tx, keys);
                }


            };

            GridTestUtils.setFieldValue(tm, "txDeadlockDetection", detection);

        }

        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicReference<TransactionDeadlockException> deadlockErr = new AtomicReference<>();

        final List<Map<Object, Integer>> keySets = generateKeys(txCnt, startKey, !lockPrimaryFirst);

        IgniteCacheProxy cache = (IgniteCacheProxy)(ignite(0).cache(CACHE_NAME));
        Object[] keys = keySets.get(txCnt - 1).keySet().toArray();

        CacheObjectContext ctx = cache.context().cacheObjectContext();
//        KeyCacheObject obj1 = grid(0).context().cacheObjects().toCacheKeyObject(ctx, cache.context(), keys[keys.length - 2], false);
//        KeyCacheObject obj2 = grid(0).context().cacheObjects().toCacheKeyObject(ctx, cache.context(), keys[keys.length - 4], false);
//        KeyCacheObject obj3 = grid(0).context().cacheObjects().toCacheKeyObject(ctx, cache.context(), keys[keys.length - 6], false);

        Set<Object> objects = new HashSet<>();
/*

        for (Object o : keySets.get(txCnt - 1).keySet())
            objects.add(grid(0).context().cacheObjects().toCacheKeyObject(ctx, cache.context(), o, false));
*/

        for (int i = 1; i < keys.length - 1; i++)
            objects.add(grid(0).context().cacheObjects().toCacheKeyObject(ctx, cache.context(), keys[i], false));

//        KeyCacheObjectImpl object = new KeyCacheObjectImpl();
//        GridTestUtils.setFieldValue(object, CacheObjectAdapter.class, "val", keys[keys.length - 3]);

        TestCommunicationSpi.init(txCnt, objects);


        /*Object[] keys = keySets.get(txCnt - 1).keySet().toArray();
        Map<Object, Integer> map = keySets.get(2);
        Object key1 = keys[keys.length - 1];
        map.remove(key1);

        Object o = incrementKey(startKey, 10000);
        for (int i = 0; i < 10000; i++)
            map.put(incrementKey(o, i), 3);

        map.put(key1, 0);*/

        Set<Object> involvedKeys = new HashSet<>();
        Set<Object> involvedLockedKeys = new HashSet<>();

        for (Map<Object, Integer> set : keySets){
            involvedKeys.addAll(set.keySet());
            involvedLockedKeys.add(set.keySet().iterator().next());
        }

        final Set<IgniteInternalTx> involvedTxs = new GridConcurrentHashSet<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.incrementAndGet();

                Ignite ignite = ignite(clientTx ? threadNum - 1 + txCnt : threadNum - 1);

                IgniteCache<Object, Integer> cache = ignite.cache(CACHE_NAME);

                Map<Object, Integer> keys = keySets.get(threadNum - 1);

                int txTimeout = 1000 + txCnt * 200;

                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                    IgniteInternalTx tx0 = ((TransactionProxyImpl)tx).tx();

                    involvedTxs.add(tx0);


                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode().id() +
                        ", tx=" + tx.xid() + ", entries=" + keys + ']');

                    cache.putAll(keys);

                    tx.commit();
                }
                catch (Throwable e) {
                    log.info("Expected exception: " + e);

                    e.printStackTrace(System.out);

                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class)) {
                        if (deadlockErr.compareAndSet(null, cause(e, TransactionDeadlockException.class))) {
                            log.info("At least one stack trace should contain " +
                                TransactionDeadlockException.class.getSimpleName());

                            e.printStackTrace(System.out);
                        }
                    }
                }
            }
        }, txCnt, "tx-thread");

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            U.error(null, "Unexpected exception", e);

            fail();
        }

        U.sleep(1000);

        TransactionDeadlockException deadlockE = deadlockErr.get();

        assertNotNull("Failed to detect deadlock", deadlockE);

        checkAllTransactionsCompleted(involvedKeys, NODES_CNT * 2, CACHE_NAME);

        // Check deadlock report
        String msg = deadlockE.getMessage();

        for (IgniteInternalTx tx : involvedTxs)
            assertTrue(msg.contains(
                "[txId=" + tx.xidVersion() + ", nodeId=" + tx.nodeId() + ", threadId=" + tx.threadId() + ']'));

        for (Object key : involvedKeys) {
            if (involvedLockedKeys.contains(key))
                assertTrue(msg.contains("[key=" + key + ", cache=" + CACHE_NAME + ']'));
            else
                assertFalse(msg.contains("[key=" + key));
        }
    }

    /**
     * @param nodesCnt Nodes count.
     */
    @SuppressWarnings("unchecked")
    private <T> List<Map<T, Integer>> generateKeys(int nodesCnt, T startKey, boolean reverse) throws IgniteCheckedException {
        List<Map<T, Integer>> keySets = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            Map<T, Integer> keys = new ConcurrentLinkedHashMap<>();

            int n1 = i + 1;
            int n2 = n1 + 1;

            int i1 = n1 < nodesCnt ? n1 : n1 - nodesCnt;
            int i2 = n2 < nodesCnt ? n2 : n2 - nodesCnt;

            keys.put(primaryKey(ignite(reverse ? i2 : i1).cache(CACHE_NAME), startKey), 0);

            List<T> list = primaryKeys(ignite(reverse ? i2 : i1).cache(CACHE_NAME), 6, (T)incrementKey(startKey, (i + 1) * 100));

            for (int j = 1; j < 6; j++) {
                keys.put(list.get(j), 1);
                keys.put((T)incrementKey(list.get(j), 13), 2);
            }

            keys.put(primaryKey(ignite(reverse ? i1 : i2).cache(CACHE_NAME), startKey), 0);

            keySets.add(keys);
        }

        return keySets;
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx count. */
        private static volatile Set<Object> KEY;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /**
         * @param txCnt Tx count.
         */
       /* private static void init(int txCnt) {
            init(txCnt);
        }
*/

        private static void init(int txCnt, Set<Object> key) {
            TX_CNT = txCnt;
            TX_IDS.clear();
            KEY = key;

        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    final GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg0;

                    GridCacheVersion txId = req.version();


                     /*if (KEY != null) {
                        for (IgniteTxEntry entry : req.writes()) {
                            if (entry.key().equals(KEY)) {
                                try {
                                    U.sleep(2000);
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }*/

                    if (KEY != null) {
                        for (IgniteTxEntry entry : req.writes()) {
                            if (KEY.contains(entry.key())) {

                                System.out.println("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq");

                                //TestCommunicationSpi.super.sendMessage(node, msg, ackC);

                                GridTestUtils.runAsync(new Runnable() {
                                    @Override public void run() {
                                        try {
                                            U.sleep(1000);
                                        }
                                        catch (IgniteInterruptedCheckedException e) {
                                            e.printStackTrace();
                                        }

                                        TestCommunicationSpi.super.sendMessage(node, msg, ackC);
                                    }

                                });

                                return;
                            }

                        }
                    }


                    if (TX_IDS.contains(txId) && TX_IDS.size() < TX_CNT) {
                        GridTestUtils.runAsync(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                while (TX_IDS.size() < TX_CNT) {
                                    try {
                                        U.sleep(50);
                                    }
                                    catch (IgniteInterruptedCheckedException e) {
                                        e.printStackTrace();
                                    }
                                }

                                TestCommunicationSpi.super.sendMessage(node, msg, ackC);

                                return null;
                            }
                        });

                        return;
                    }
                }
                else if (msg0 instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse res = (GridNearTxPrepareResponse)msg0;

                    GridCacheVersion txId = res.version();

                    TX_IDS.add(txId);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
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
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
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
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), CUSTOM_START_KEY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitionedNear() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), ORDINAL_START_KEY);
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), CUSTOM_START_KEY);
        }
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
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitionedNearTxOnPrimary() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocksTxOnPrimary(createCache(PARTITIONED, syncMode, true), 3, ORDINAL_START_KEY);
            doTestDeadlocksTxOnPrimary(createCache(PARTITIONED, syncMode, true), 3, CUSTOM_START_KEY);
            doTestDeadlocksTxOnPrimary(createCache(PARTITIONED, syncMode, true), 4, ORDINAL_START_KEY);
            doTestDeadlocksTxOnPrimary(createCache(PARTITIONED, syncMode, true), 4, CUSTOM_START_KEY);
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

            doTestDeadlock(3, true, true, startKey);
            doTestDeadlock(3, false, false, startKey);
            doTestDeadlock(3, false, true, startKey);

            doTestDeadlock(4, true, true, startKey);
            doTestDeadlock(4, false, false, startKey);
            doTestDeadlock(4, false, true, startKey);
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
     * @param cache Cache.
     * @param startKey Start key.
     */
    private void doTestDeadlocksTxOnPrimary(IgniteCache cache, int txCnt, Object startKey) {
        try {
            awaitPartitionMapExchange();

            getOrCreateStrLoggerForGrid(0).reset();

            List<List<Object>> ordinalKeys = generateKeys(txCnt, startKey, false, true);

            startWaiterThread(ordinalKeys.get(0).get(0));

            doTestDeadlock(txCnt, false, ordinalKeys, 0);

        }
        catch (Throwable e) {
            assertTrue("Deadlock detection has not been called on 0 node.",
                getOrCreateStrLoggerForGrid(0).toString().contains("Deadlock detection started"));
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlock(int txCnt, boolean lockPrimaryFirst, boolean clientTx,
        Object startKey) throws Exception {
        log.info(">>> Test deadlock [txCnt=" + txCnt + ", lockPrimaryFirst=" + lockPrimaryFirst +
            ", clientTx=" + clientTx + ", startKey=" + startKey + ']');

        doTestDeadlock(txCnt, clientTx, generateKeys(txCnt, startKey, !lockPrimaryFirst, false), -10);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlock(
        final int txCnt,
        final boolean clientTx,
        final List<List<Object>> keySets,
        final int lastNodeId
    ) throws Exception {

        TestCommunicationSpi.init(txCnt);

        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicReference<TransactionDeadlockException> deadlockErr = new AtomicReference<>();

        final Set<Object> involvedKeys = new GridConcurrentHashSet<>();
        final Set<Object> involvedLockedKeys = new GridConcurrentHashSet<>();
        final Set<IgniteInternalTx> involvedTxs = new GridConcurrentHashSet<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.incrementAndGet();

                Ignite ignite = ignite(clientTx ? threadNum - 1 + txCnt : threadNum - 1);

                IgniteCache<Object, Integer> cache = ignite.cache(CACHE_NAME);

                List<Object> keys = keySets.get(threadNum - 1);

                int txTimeout = 1000 + txCnt * 200;

                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                    IgniteInternalTx tx0 = ((TransactionProxyImpl)tx).tx();

                    involvedTxs.add(tx0);

                    Object key = keys.get(0);

                    involvedKeys.add(key);

                    Object k;

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode().id() +
                        ", tx=" + tx.xid() + ", key=" + key + ']');

                    cache.put(key, 0);

                    involvedLockedKeys.add(key);

                    key = keys.get(1);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)cache).context().affinity().primaryByKey(key, NONE);

                    List<Object> primaryKeys = primaryKeys(
                        grid(primaryNode).cache(CACHE_NAME), 5, incrementKey(key , (100 * threadNum)));

                    Map<Object, Integer> entries = new HashMap<>();

                    involvedKeys.add(key);

                    entries.put(key, 0);

                    for (Object o : primaryKeys) {
                        involvedKeys.add(o);

                        entries.put(o, 1);

                        k = incrementKey(o, 13);

                        involvedKeys.add(k);

                        entries.put(k, 2);
                    }

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode().id() +
                        ", tx=" + tx.xid() + ", entries=" + entries + ']');

                    cache.putAll(entries);

                    if (threadNum == lastNodeId + 1) {
                        while (TestCommunicationSpi.TX_IDS.size() < TestCommunicationSpi.TX_CNT - 1)
                            U.sleep(50);
                    }

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
    private <T> List<List<T>> generateKeys(int nodesCnt, T startKey, boolean reverse,
        boolean txOnPrimary) throws IgniteCheckedException {
        List<List<T>> keySets = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            List<T> keys = new ArrayList<>(2);

            int n1 = i + 1;
            int n2 = n1 + 1;

            int i1 = n1 < nodesCnt ? n1 : n1 - nodesCnt;
            int i2 = n2 < nodesCnt ? n2 : n2 - nodesCnt;

            if(txOnPrimary){
                i1 = i1 >=1 && i1 <= 2 ? 0 : i1;
                i2 = i2 >=1 && i2 <= 2 ? 0 : i2;
            }

            keys.add(primaryKey(ignite(i1).cache(CACHE_NAME), startKey));
            keys.add(primaryKey(ignite(i2).cache(CACHE_NAME), startKey));

            if (reverse)
                Collections.reverse(keys);
            keySets.add(keys);
        }

        return keySets;

    }

    /**
     * Start a thread that waits for the key to lock.
     *
     * @param key Key.
     */
    private void startWaiterThread(final Object key) {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    U.sleep(100);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)grid(0).cache(CACHE_NAME)).context().affinity().primaryByKey(key, NONE);

                    IgniteEx grid = (IgniteEx)grid(primaryNode);

                    IgniteTxManager tm = grid.context().cache().context().tm();

                    CacheObjectContext ctx =
                        ((IgniteCacheProxy)grid.cache(CACHE_NAME)).context().cacheObjectContext();

                    UUID nodeId = grid.context().localNodeId();

                    IgniteTxEntry entry = null;

                    while (entry == null) {
                        for (IgniteInternalTx internalTx : tm.activeTransactions()) {
                            if ((internalTx instanceof GridDhtTxLocal && internalTx.nodeId().equals(nodeId))) {
                                for (IgniteTxEntry e : internalTx.allEntries()) {
                                    Object val = e.key().value(ctx, false);

                                    if (val != null && val.equals(key)) {
                                        entry = e;

                                        break;
                                    }

                                }

                                if (entry != null)
                                    break;
                            }
                        }

                        U.sleep(50);
                    }
                    while (((GridCacheMapEntry)entry.cached()).mvccAllLocal() == null)
                        U.sleep(50);

                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace(System.out);

                }

                TestCommunicationSpi.TX_IDS.add(new GridCacheVersion());
            }
        });
    }

    /**
     * @param gridId Grid id.
     */
    private GridStringLogger getOrCreateStrLoggerForGrid(int gridId) {
        IgniteTxManager tm = grid(gridId).context().cache().context().tm();

        TxDeadlockDetection dd = GridTestUtils.getFieldValue(tm, IgniteTxManager.class, "txDeadlockDetection");

        IgniteLogger log = GridTestUtils.getFieldValue(dd, TxDeadlockDetection.class, "log");

        if (log instanceof GridStringLogger)
            return (GridStringLogger)log;
        else {
            GridStringLogger strLog = new GridStringLogger(true, log);

            GridTestUtils.setFieldValue(dd, "log", strLog);

            return strLog;
        }

    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /**
         * @param txCnt Tx count.
         */
        private static void init(int txCnt) {
            TX_CNT = txCnt;
            TX_IDS.clear();
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

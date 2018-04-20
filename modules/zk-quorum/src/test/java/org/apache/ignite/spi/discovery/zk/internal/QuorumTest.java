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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.quorum.CommunicationResolverConfiguration;
import org.apache.ignite.quorum.CustomCommunicationFailureResolver;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestSuite2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;
import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 *
 */
public class QuorumTest extends GridCommonAbstractTest {
    /** */
    private static final String IGNITE_ZK_ROOT = ZookeeperDiscoverySpi.DFLT_ROOT_PATH;

    /** */
    private static final int ZK_SRVS = 3;

    /** */
    private static TestingCluster zkCluster;

    /** To run test with real local ZK. */
    private static final boolean USE_TEST_CLUSTER = true;

    /** */
    private boolean client;

    /** */
    private static ThreadLocal<Boolean> clientThreadLoc = new ThreadLocal<>();

    /** */
    private static ConcurrentHashMap<UUID, Map<Long, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** */
    private static volatile boolean err;

    /** */
    private boolean testSockNio;

    /** */
    private boolean testCommSpi;

    /** */
    private long sesTimeout;

    /** */
    private long joinTimeout;

    /** */
    private boolean clientReconnectDisabled;

    /** */
    private ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis = new ConcurrentHashMap<>();

    /** */
    private Map<String, Object> userAttrs;

    /** */
    private boolean dfltConsistenId;

    /** */
    private UUID nodeId;

    /** */
    private boolean persistence;

    /** */
    private IgniteOutClosure<CommunicationFailureResolver> commFailureRslvr;

    /** */
    private IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth;

    /** */
    private String zkRootPath;

    /** Check consistency. */
    boolean checkConsistency = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        if (testSockNio)
            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, ZkTestClientCnxnSocketNIO.class.getName());

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        /*cfg.setPluginConfigurations(new GridGainConfiguration()
            .setRollingUpdatesEnabled(false)
            .setSnapshotConfiguration(new SnapshotConfiguration()
                .setSnapshotsPath("/home/vitaliy/Desktop/123")
            )
        );*/

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        if (!dfltConsistenId)
            cfg.setConsistentId(igniteInstanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        if (joinTimeout != 0)
            zkSpi.setJoinTimeout(joinTimeout);

        zkSpi.setSessionTimeout(sesTimeout > 0 ? sesTimeout : 10_000);

        zkSpi.setClientReconnectDisabled(clientReconnectDisabled);

        // Set authenticator for basic sanity tests.
        if (auth != null) {
            zkSpi.setAuthenticator(auth.apply());

            zkSpi.setInternalListener(new IgniteDiscoverySpiInternalListener() {
                @Override public void beforeJoin(ClusterNode locNode, IgniteLogger log) {
                    ZookeeperClusterNode locNode0 = (ZookeeperClusterNode)locNode;

                    Map<String, Object> attrs = new HashMap<>(locNode0.getAttributes());

                    attrs.put(ATTR_SECURITY_CREDENTIALS, new SecurityCredentials(null, null, igniteInstanceName));

                    locNode0.setAttributes(attrs);
                }

                @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log,
                    DiscoverySpiCustomMessage msg) {
                    return false;
                }
            });
        }

        spis.put(igniteInstanceName, zkSpi);

        if (USE_TEST_CLUSTER) {
            assert zkCluster != null;

            zkSpi.setZkConnectionString(zkCluster.getConnectString());

            if (zkRootPath != null)
                zkSpi.setZkRootPath(zkRootPath);
        }
        else
            zkSpi.setZkConnectionString("localhost:2181");

        cfg.setDiscoverySpi(zkSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(3);

        cfg.setCacheConfiguration(ccfg);

        Boolean clientMode = clientThreadLoc.get();

        if (clientMode != null)
            cfg.setClientMode(clientMode);
        else
            cfg.setClientMode(client);

        if (userAttrs != null)
            cfg.setUserAttributes(userAttrs);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(new IgnitePredicate<Event>() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply(Event evt) {
                try {
                    DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                    UUID locId = ((IgniteKernal)ignite).context().localNodeId();

                    Map<Long, DiscoveryEvent> nodeEvts = evts.get(locId);

                    if (nodeEvts == null) {
                        Object old = evts.put(locId, nodeEvts = new TreeMap<>());

                        assertNull(old);

                        synchronized (nodeEvts) {
                            DiscoveryLocalJoinData locJoin = ((IgniteKernal)ignite).context().discovery().localJoin();

                            nodeEvts.put(locJoin.event().topologyVersion(), locJoin.event());
                        }
                    }

                    synchronized (nodeEvts) {
                        DiscoveryEvent old = nodeEvts.put(discoveryEvt.topologyVersion(), discoveryEvt);

                        assertNull(old);
                    }
                }
                catch (Throwable e) {
                    error("Unexpected error [evt=" + evt + ", err=" + e + ']', e);

                    err = true;
                }

                return true;
            }
        }, new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT});

        cfg.setLocalEventListeners(lsnrs);

        if (persistence) {
            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).
                    setPersistenceEnabled(true))
                .setPageSize(1024)
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(memCfg);
        }

        if (testCommSpi)
            cfg.setCommunicationSpi(new ZkTestCommunicationSpi());

        if (commFailureRslvr != null)
            cfg.setCommunicationFailureResolver(commFailureRslvr.apply());

        return cfg;
    }

    /**
     * @param clientMode Client mode flag for started nodes.
     */
    private void clientMode(boolean clientMode) {
        client = clientMode;
    }

    /**
     * @param clientMode Client mode flag for nodes started from current thread.
     */
    private void clientModeThreadLocal(boolean clientMode) {
        clientThreadLoc.set(clientMode);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT, "1000");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        stopZkCluster();

        System.clearProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT);

        super.afterTestsStopped();
    }

    /**
     *
     */
    private void stopZkCluster() {
        if (zkCluster != null) {
            try {
                zkCluster.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to stop Zookeeper client: " + e, e);
            }

            zkCluster = null;
        }
    }

    /**
     *
     */
    private static void ackEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /**
     *
     */
    private void clearAckEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (USE_TEST_CLUSTER && zkCluster == null) {
            zkCluster = ZookeeperDiscoverySpiTestSuite2.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }

        reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        clearAckEveryEventSystemProperty();

        try {
            assertFalse("Unexpected error, see log for details", err);

//            checkEventsConsistency();

            checkInternalStructuresCleanup();

//             checkZkNodesCleanup();
        }
        finally {
            reset();

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkInternalStructuresCleanup() throws Exception {
        for (Ignite node : G.allGrids()) {
            final AtomicReference<?> res = GridTestUtils.getFieldValue(spi(node), "impl", "commErrProcFut");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res.get() == null;
                }
            }, 30_000);

            assertNull(res.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
   /* public void test50_50() throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;

        commFailureRslvr = () -> new CustomCommunicationFailureResolver(
            new CustomCommunicationFailureResolver.Configuration()
                .minNodesCount(5)
        );

        startGridsMultiThreaded(10);

        List<Ignite> nodes = G.allGrids();

        assertEquals(10, nodes.size());

        for (int i = 0; i < 10; i++) {
            IgniteKernal node = (IgniteKernal)nodes.get(i);

            if (node.cluster().localNode().order() % 2 == 0)
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(10, 1, 3, 5, 7, 9);
            else
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(10, 0, 2, 4, 6, 8);
        }

        failOnMsg(2, 6);

        waitForTopology(5);

        assertTopEqualsByOrders(1, 3, 5, 7, 9);
    }*/

    /**
     * @throws Exception If failed.
     */
    /*public void test50_50_2() throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;

        commFailureRslvr = () -> new CustomCommunicationFailureResolver(
            new CommunicationResolverConfiguration().checkConsistency(false)
        );


        startGridsMultiThreaded(10);


        ZkTestCommunicationSpi.testSpi(ignite(2)).initCheckResult(20, 0, 1, 2, 4, 5);
        ZkTestCommunicationSpi.testSpi(ignite(3)).initCheckResult(20, 0, 1, 3, 4, 5);

        IgniteSpiException e = failOnMsg(2, 3);

        waitForTopology(5);

        assertTopEqualsByIdxs(0, 1, 3, 4, 5);
    }*/

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation5_5() throws Exception {
        int[][] orders = doTestSegmentation(5, 5);

        assertTopEqualsByOrders(getSetWithCoordinator(orders));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation6_4() throws Exception {
        int[] order = doTestSegmentation(6, 4)[0];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void ignoreTestSegmentationWithInner6_4() throws Exception {
        //todo
        int[] order = doTestSegmentation(new T3<>(0, 1, 2), 6, 4)[0];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation7_3() throws Exception {
        checkConsistency = true;

        int[] order = doTestSegmentation(7, 3)[0];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation8_2() throws Exception {
        checkConsistency = true;

        int[] order = doTestSegmentation(8, 2)[0];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation9_1() throws Exception {
        checkConsistency = true;

        int[] order = doTestSegmentation(9, 1)[0];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation3_3_3() throws Exception {
        int[][] orders = doTestSegmentation(3, 3, 3);

        assertTopEqualsByOrders(getSetWithCoordinator(orders));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation3_4_3() throws Exception {
        int[] order = doTestSegmentation(3, 4, 3)[1];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation1_2_4_2_3() throws Exception {
        int[] order = doTestSegmentation(1, 2, 4, 2, 3)[2];

        assertTopEqualsByOrders(
            Arrays.stream(order).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new))
        );
    }

    /**
     * @param orders Orders.
     */
    @NotNull private TreeSet<Integer> getSetWithCoordinator(int[][] orders) {
        int i = 0;

        TreeSet<Integer> set =
            Arrays.stream(orders[i]).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new));

        while (!set.contains(1))
            set = Arrays.stream(orders[++i]).map(v -> v + 1).boxed().collect(Collectors.toCollection(TreeSet::new));
        return set;
    }

    /**
     * @param block Block communication in t1 segment between t2 and t3 nodes.
     * @param clustersNodesNum Clusters nodes number.
     */
    @SuppressWarnings("ConstantConditions")
    private int[][] doTestSegmentation(T3<Integer, Integer, Integer> block, int... clustersNodesNum) throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;

        commFailureRslvr = () -> new CustomCommunicationFailureResolver(
            new CommunicationResolverConfiguration()
                .setCheckConsistency(checkConsistency)
        );

        int nodesCnt = Arrays.stream(clustersNodesNum).sum();

        startGridsMultiThreaded(nodesCnt);

        List<Integer> list = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++)
            list.add(i);

        Collections.shuffle(list);

        int[][] clusterOrders = new int[clustersNodesNum.length][];

        int idx = 0;
        for (int i = 0; i < clustersNodesNum.length; i++) {
            clusterOrders[i] = new int[clustersNodesNum[i]];
            for (int j = 0; j < clustersNodesNum[i]; j++)
                clusterOrders[i][j] = list.get(idx++);
        }

        List<Ignite> nodes = getAllGridsInOrder();

        idx = 0;
        for (int i = 0; i < clustersNodesNum.length; i++) {
            for (int j = 0; j < clustersNodesNum[i]; j++)
                ZkTestCommunicationSpi.testSpi(nodes.get(list.get(idx++))).initCheckResult(nodesCnt, clusterOrders[i]);
        }

        if (block != null) {
            int[] order = clusterOrders[block.get1()];

            int idx1 = order[block.get2()];
            int idx2 = order[block.get3()];

            ZkTestCommunicationSpi.testSpi(nodes.get(idx1)).checkRes.set(idx2, false);
            ZkTestCommunicationSpi.testSpi(nodes.get(idx2)).checkRes.set(idx1, false);

            int[] ints = new int[order.length - 1];

            int pos = idx1 < idx2 ? block.get2() : block.get3();

            System.arraycopy(order, 0, ints, 0, pos);
            System.arraycopy(order, pos + 1, ints, pos, ints.length - pos);

            clusterOrders[block.get1()] = ints;
        }

        failOnMsg(1, nodesCnt - 1);

        waitForTopology(Arrays.stream(clustersNodesNum).max().getAsInt() - (block != null ? 1 : 0));

        return clusterOrders;
    }

    /**
     *
     */
    private int[][] doTestSegmentation(int... clusters) throws Exception {
        return doTestSegmentation(null, clusters);
    }

    /**
     *
     */
    private List<Ignite> getAllGridsInOrder() {
        List<Ignite> ignites = G.allGrids();

        ignites.sort((o1, o2) -> {
            long order1 = ((IgniteKernal)o1).cluster().localNode().order();
            long order2 = ((IgniteKernal)o2).cluster().localNode().order();

            return Long.compare(order1, order2);
        });

        return ignites;
    }

    /**
     * @param fromIdx From index.
     * @param toIdx To index.
     */
    private IgniteSpiException failOnMsg(int fromIdx, int toIdx) {
        ZookeeperDiscoverySpi spi = spi(ignite(fromIdx));

        UUID crdId = ignite(toIdx).cluster().localNode().id();

        try {
            spi.resolveCommunicationFailure(spi.getNode(crdId), new Exception("test"));

//            fail("Exception is not thrown");
        }
        catch (IgniteSpiException e) {
            return e;
        }

        return null;
    }

    /**
     *
     */
    private void assertTopEqualsByIdxs(Integer... idxs) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(idxs.length, nodes.size());

        Set<Integer> expIdxs = new TreeSet<>(Arrays.asList(idxs));
        Set<Integer> actualIdxs = new TreeSet<>();

        String instanceName = getTestIgniteInstanceName();

        for (Ignite node : nodes) {
            String idxStr = node.configuration().getIgniteInstanceName().replace(instanceName, "");

            actualIdxs.add(Integer.valueOf(idxStr));
        }

        assertEquals(expIdxs, actualIdxs);
    }

    /**
     * @param expIdxs Expected idxs.
     */
    private void assertTopEqualsByOrders(TreeSet expIdxs) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expIdxs.size(), nodes.size());

        Set<Integer> actualIdxs = new TreeSet<>();

        for (Ignite node : nodes)
            actualIdxs.add((int)((IgniteKernal)node).cluster().localNode().order());

        assertEquals(expIdxs, actualIdxs);
    }

    /**
     *
     */
    private void assertTopEqualsByOrders(int... order) {
        assertTopEqualsByOrders(Arrays.stream(order).boxed().collect(Collectors.toCollection(TreeSet::new)));
    }

    /**
     *
     */
    private void reset() {
        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        ZkTestClientCnxnSocketNIO.reset();

        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        err = false;

        evts.clear();

        try {
            cleanPersistenceDir();
        }
        catch (Exception e) {
            error("Failed to delete DB files: " + e, e);
        }

        clientThreadLoc.set(null);
    }

    /**
     *
     */
    protected void cleanPersistenceDir() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /**
     * @param node Node.
     * @return Node's discovery SPI.
     */
    private static ZookeeperDiscoverySpi spi(Ignite node) {
        return (ZookeeperDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /** {@inheritDoc} */
    @Override protected void waitForTopology(int expSize) throws Exception {
        super.waitForTopology(expSize);

        // checkZkNodesCleanup();
    }

    /**
     *
     */
    static class ZkTestCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        private volatile CountDownLatch pingStartLatch;

        /** */
        private volatile CountDownLatch pingLatch;

        /** */
        private volatile BitSet checkRes;

        /**
         * @param ignite Node.
         * @return Node's communication SPI.
         */
        static ZkTestCommunicationSpi testSpi(Ignite ignite) {
            return (ZkTestCommunicationSpi)ignite.configuration().getCommunicationSpi();
        }

        /**
         * @param nodes Number of nodes.
         * @param setBitIdxs Bits indexes to set in check result.
         */
        void initCheckResult(int nodes, int... setBitIdxs) {
            checkRes = new BitSet(nodes);

            for (int bitIdx : setBitIdxs)
                checkRes.set(bitIdx);
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
            CountDownLatch pingStartLatch = this.pingStartLatch;

            if (pingStartLatch != null)
                pingStartLatch.countDown();

            CountDownLatch pingLatch = this.pingLatch;

            try {
                if (pingLatch != null)
                    pingLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            BitSet checkRes = this.checkRes;

            if (checkRes != null) {
                this.checkRes = null;

                return new IgniteFinishedFutureImpl<>(checkRes);
            }

            return super.checkConnection(nodes);
        }
    }
}

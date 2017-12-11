package org.apache.ignite.internal.processors.cache.transactions;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 */
public class TransactionBehindStopNodeTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Tx size. */
    private static final int TX_SIZE = 100;

    /** Logger communication. */
    private static final boolean LOG_COMMUNICATION = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration("cache")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
            .setBackups(1);

        TcpCommunicationSpi spi = new TestCommunicationSpi();

        TestDiscoverySpi discoverySpi = new TestDiscoverySpi();

        return cfg
            .setCommunicationSpi(spi)
            .setCacheConfiguration(ccfg)
            .setDiscoverySpi(discoverySpi);
    }

    /**
     *
     */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            System.out.println(String.format("TcpDiscoverySpi.sendCustomEvent: %s", msg));

            super.sendCustomEvent(msg);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            System.out.println(String.format("TcpDiscoverySpi.writeToSocket: %s", msg));
            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            System.out.println(String.format("TcpDiscoverySpi.startMessageProcess: %s", msg));
            super.startMessageProcess(msg);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < NODES_CNT; i++) {
            IgniteEx ignite = startGrid(i);

            TcpCommunicationSpi spi = (TcpCommunicationSpi)ignite.configuration().getCommunicationSpi();

            CommunicationListener curLsnr = spi.getListener();

            spi.setListener(new CommunicationListener<Message>() {
                @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
                    if (LOG_COMMUNICATION) {
                        if (msg instanceof GridIoMessage) {
                            Message msg0 = ((GridIoMessage)msg).message();

                            String id = nodeId.toString();
                            String locNodeId = ignite.context().localNodeId().toString();

                            System.out.println(
                                String.format("Input msg[type=%s, fromNode= %s, locNode=%s]",
                                    msg0.getClass().getSimpleName(),
                                    id.charAt(id.length() - 1),
                                    locNodeId.charAt(locNodeId.length() - 1)
                                )
                            );
                        }
                        else
                            System.out.println(msg);
                    }

                    curLsnr.onMessage(nodeId, msg, msgC);
                }

                @Override public void onDisconnected(UUID nodeId) {
                    curLsnr.onDisconnected(nodeId);
                }
            });

            TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)ignite.configuration().getDiscoverySpi();

            DiscoverySpiListener lsnr =
                GridTestUtils.getFieldValue(discoverySpi, TcpDiscoverySpi.class, "lsnr");

            discoverySpi.setListener(new DiscoverySpiListener() {
                @Override public void onLocalNodeInitialized(ClusterNode locNode) {
                    System.out.println("onLocalNodeInitialized: " + locNode);

                    lsnr.onLocalNodeInitialized(locNode);
                }

                @Override public void onDiscovery(
                    int type,
                    long topVer,
                    ClusterNode node,
                    Collection<ClusterNode> topSnapshot,
                    @Nullable Map<Long, Collection<ClusterNode>> topHist,
                    @Nullable DiscoverySpiCustomMessage data) {

                    System.out.println("onDiscovery : " + data);

                    lsnr.onDiscovery(type, topVer, node, topSnapshot, topHist, data);
                }
            });


        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    public void testQWe() {

    }

    /**
     *
     */
    public void testName() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            awaitPartitionMapExchange();

            IgniteCache cache = ignite.cache("cache");

            G.stop(getTestIgniteInstanceName(0), false);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < 100; i++) {
                    /*if (i == 50) {
                        latch.countDown();

                        stopGridLatch.await();
                    }*/

                    cache.put(i, String.valueOf(i));
                }

                tx.commit();
            }

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     * @throws Exception If fail.
     */
    public void testOneNode() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch stopGridLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override public void run() {

                    try {
                        U.sleep(500);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }
                    log.info("G.stop: start");

                    G.stop(getTestIgniteInstanceName(0), false);

                    log.info("G.stop: stop");

//                    stopGridLatch.countDown();
                }
            }).start();

            IgniteCache cache = ignite.cache("cache");

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < 100; i++) {
                    if (i == 50) {
//                        latch.countDown();

                        U.sleep(1000);
//                        stopGridLatch.await();
                    }

                    log.info("put: " + i);

                    cache.put(i, String.valueOf(i));
                }

                tx.commit();
            }

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     * @throws Exception If fail.
     */
    public void testOneNode1() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch stopGridLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    log.info("G.stop: start");

                    G.stop(getTestIgniteInstanceName(0), false);

                    log.info("G.stop: stop");

                    stopGridLatch.countDown();
                }
            }).start();

            IgniteCache cache = ignite.cache("cache");

            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < 100; i++) {
                    if (i == 50) {
                        latch.countDown();

                        while (ignite.context().gateway().getState() == GridKernalState.STARTED) {
                            System.out.println("while (ignite.context().gateway().getState() == GridKernalState.STARTED)");
                            U.sleep(50);
                        }

//                        stopGridLatch.await();
                    }

                    log.info("put: " + i);

                    cache.put(i, String.valueOf(i));
                }

                tx.commitAsync();
            }

            /*try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                cache.put(42, "42");

                tx.commit();
            }*/

            U.sleep(Long.MAX_VALUE);

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     * @throws Exception If fail.
     */
    public void testOneNode123() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            IgniteEx ignite1 = startGrid(1);
//            IgniteEx ignite2 = startGrid(2);
//            IgniteEx ignite3 = startGrid(3);
//            IgniteEx ignite4 = startGrid(4);
//            IgniteEx ignite5 = startGrid(5);
//            IgniteEx ignite6 = startGrid(6);

            awaitPartitionMapExchange();

            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch stopGridLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    log.info("G.stop: start");

                    G.stop(getTestIgniteInstanceName(0), false);

                    log.info("G.stop: stop");

                    stopGridLatch.countDown();
                }
            }).start();


            /*new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    IgniteCache<Object, Object> cache = ignite1.cache("cache");

                    try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        for (int i = 10000; i < 10100; i++) {


                            cache.put(i, String.valueOf(i));
                        }

                        tx.commitAsync();
                    }

                }
            }).start();

            new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    IgniteCache<Object, Object> cache = ignite2.cache("cache");

                    try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        for (int i = 1000; i < 1100; i++) {


                            cache.put(i, String.valueOf(i));
                        }

                        tx.commitAsync();
                    }

                }
            }).start();*/

            IgniteCache cache = ignite.cache("cache");

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < 100; i++) {
                    if (i == 50) {
                        latch.countDown();

                        while (ignite.context().gateway().getState() == GridKernalState.STARTED) {
                            System.out.println("while (ignite.context().gateway().getState() == GridKernalState.STARTED)");
                            U.sleep(50);
                        }

                        assertEquals(GridKernalState.STOPPING, ignite.context().gateway().getState());
                    }

//                    log.info("put: " + i);

                    cache.put(i, String.valueOf(i));
                }

                tx.commit();
            }
            catch (Exception e) {
                e.printStackTrace(System.err);
            }

           /* try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                cache.put(42, "42");

                tx.commit();
            }*/

            stopGridLatch.await();

//            IgniteCache<Object, Object> cache1 = ignite1.cache("cache");

           /* try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = 0; i < 100; i++) {
                assertEquals(String.valueOf(i), cache1.get(i));
//                    System.out.println(i + " : "+cache1.get(i));
                }
                tx.commit();
            }*/

//           U.sleep(Long.MAX_VALUE);

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     *
     */
    public void testTransactionsEndBeforeStop() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch stopGridLatch = new CountDownLatch(1);

        IgniteEx ignite = startGrid(666);// To be stopped.

        awaitPartitionMapExchange();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                latch.await();

                stopGrid(666, false);

                stopGridLatch.countDown();

                return null;
            }
        });

        IgniteCache<Integer, String> cache = ignite.cache("cache");

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 1_000_000, TX_SIZE)) {
            for (int i = 0; i < TX_SIZE; i++) {
                if (i == 50) {
                    latch.countDown();

                    while (ignite.context().gateway().getState() == GridKernalState.STARTED)
                        U.sleep(50);

                    assertEquals(GridKernalState.STOPPING, ignite.context().gateway().getState());
                }

                cache.put(i, String.valueOf(i));
            }

            tx.commit();
        }

        stopGridLatch.await();

        IgniteCache<Object, Object> cache0 = ignite(0).cache("cache");

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 100; i++)
                assertEquals(String.valueOf(i), cache0.get(i));

            tx.commit();
        }

        fut.get();
    }

    /**
     *
     */
    public void testTxOnStopping() throws Exception {
        final IgniteEx ignite = startGrid(666);// To be stopped.

        awaitPartitionMapExchange();

        System.out.println("awaitPartitionMapExchange();awaitPartitionMapExchange();awaitPartitionMapExchange();awaitPartitionMapExchange();awaitPartitionMapExchange();awaitPartitionMapExchange();");

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch txStart = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Object, Object> cache = ignite.cache("cache");

                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

                    cache.put(1, 1);

                    txStart.countDown();

                    latch.await();

                    tx.commit();
                }

                return null;
            }
        });

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                txStart.await();

                stopGrid(666, false);

                return null;
            }
        });

        while (ignite.context().gateway().getState() == GridKernalState.STARTED)
            U.sleep(50);

        IgniteCache<Object, Object> cache = ignite(0).cache("cache");

        System.out.println("txtxtxtxtxtx");

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 1_000_000, TX_SIZE)) {
            for (int i = 0; i < TX_SIZE; i++) {
                System.out.println("put : " + i);
                cache.put(i, String.valueOf(i));
            }

            tx.commit();
        }

        latch.countDown();

        System.out.println("end");

        fut.get();
        stopFut.get();
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (LOG_COMMUNICATION) {
                if (msg instanceof GridIoMessage) {
                    Message msg0 = ((GridIoMessage)msg).message();

                    String id = node.id().toString();
                    String locNodeId = ((IgniteEx)ignite).context().localNodeId().toString();

                    System.out.println(
                        String.format("Output msg[type=%s, fromNode= %s, toNode=%s]",
                            msg0.getClass().getSimpleName(),
                            locNodeId.charAt(locNodeId.length() - 1),
                            id.charAt(id.length() - 1)
                        )
                    );

                }
                else
                    System.out.println(msg);
            }
            super.sendMessage(node, msg, ackC);
        }

    }
}

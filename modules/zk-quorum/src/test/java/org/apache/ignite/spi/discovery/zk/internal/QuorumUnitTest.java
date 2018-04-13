package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.quorum.CustomCommunicationFailureResolver;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;

import static junit.framework.TestCase.assertEquals;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class QuorumUnitTest {
    @Test
    public void name1() {

        int nodesCnt = 2000;
        List<ClusterNode> nodes = generateNodes(nodesCnt);

        CustomCommunicationFailureResolver r = new CustomCommunicationFailureResolver(/*new CommunicationResolverConfiguration().checkConsistency(false)*/);

        long t = System.currentTimeMillis();
        System.out.println("ignite generates");
        GridTestUtils.setFieldValue(r, "ignite", generateIgnite(nodes, 50, 32_864, 4));
        System.out.println("ignite generated: " + (System.currentTimeMillis() - t));
        setLogger(r);

        /*int[] ints = new int[nodesCnt];

        for (int i = 0; i < nodesCnt; i++)
            ints[i] = 1;*/

        Map<UUID, BitSet> state = getGraph(nodes, 1999, 1);

//        ZkCommunicationFailureContext ctx = new ZkCommunicationFailureContext(null, nodes, nodes, state);

        for (int i = 0; i < 100; i++) {

            ZkCommunicationFailureContext ctx = new ZkCommunicationFailureContext(null, nodes, nodes, state);

//            long t1 = System.currentTimeMillis();
            r.resolve(ctx);
//            System.out.println(System.currentTimeMillis() - t1);
        }

        System.out.println();

    }



    /*@Test
    public void name343() {
        GridPartitionStateMap map = new GridPartitionStateMap();
        System.out.println(GraphLayout.parseInstance(map).toFootprint());

//        GridDhtPartitionState[] states = GridDhtPartitionState.values();
        for (int i = 1000; i < CacheConfiguration.MAX_PARTITIONS_COUNT; i *= 2) {
            map = new GridPartitionStateMap();
            map.put(i - 1, OWNING);
            System.out.println(i + ": " + GraphLayout.parseInstance(map).toFootprint());
        }
    }

    @Test
    public void name666() {
        GridPartitionStateMap map = new GridPartitionStateMap();
        System.out.println(GraphLayout.parseInstance(map).toFootprint());

        map.put(63999, OWNING);

        System.out.println(GraphLayout.parseInstance(map).toFootprint());

    }*/

    /**
     * @param nodes Nodes.
     * @param cacheGroupsCnt Cache groups count.
     * @param partCnt Partition count.
     * @param copesCnt Copes count.
     */
    @SuppressWarnings("unchecked")
    private Ignite generateIgnite(List<ClusterNode> nodes, int cacheGroupsCnt, int partCnt, int copesCnt,
        int grpLimit, boolean rnd) {
        ThreadLocalRandom rndm = ThreadLocalRandom.current();
        RendezvousAffinityFunction f = new RendezvousAffinityFunction(false, partCnt);

        IgniteKernal ignite = mock(IgniteKernal.class);
        GridKernalContext ctx = mock(GridKernalContext.class);
        GridCacheProcessor cacheProcessor = mock(GridCacheProcessor.class);
        GridCacheSharedContext cacheCtx = mock(GridCacheSharedContext.class);
        AffinityFunction affinityFunction = mock(AffinityFunction.class);
        CacheConfiguration cacheConfiguration = mock(CacheConfiguration.class);

        when(cacheCtx.logger(GridDhtPartitionTopologyImpl.class)).thenReturn(log);
        when(cacheCtx.logger(GridDhtPartitionsExchangeFuture.EXCHANGE_LOG)).thenReturn(log);
        when(affinityFunction.partitions()).thenReturn(partCnt);
        when(cacheConfiguration.getCacheMode()).thenReturn(PARTITIONED);

        List<CacheGroupContext> grpContexts = new ArrayList<>(cacheGroupsCnt);

        for (int i = 0; i < (grpLimit < cacheGroupsCnt ? grpLimit : cacheGroupsCnt); i++) {
            System.out.println("pm" + i);
            CacheGroupContext grpCtx = mock(CacheGroupContext.class);
            when(grpCtx.affinityFunction()).thenReturn(affinityFunction);
            when(grpCtx.config()).thenReturn(cacheConfiguration);

            GridDhtPartitionTopology top = new GridDhtPartitionTopologyImpl(cacheCtx, grpCtx);

            GridDhtPartitionFullMap map = new GridDhtPartitionFullMap(nodes.get(0).id(), 2, 2);

//            ArrayList<GridPartitionStateMap> maps = new ArrayList<>();

            for (int j = 0; j < partCnt; j++) {
                int finalJ = j;
                IntStream stream;
                if (rnd) {
                    stream = rndm.ints(0, nodes.size())
                        .distinct()
                        .limit(copesCnt);
                }
                else {
                    stream = f.assignPartition(j, nodes, copesCnt -1, null)
                        .stream()
                        .mapToInt(node -> (int)node.order() - 1);
                }

                stream.forEach(idx -> {
                    GridDhtPartitionMap partMap = map.computeIfAbsent(nodes.get(idx).id(), uuid -> {
                        GridDhtPartitionMap m = new GridDhtPartitionMap();
                        GridTestUtils.setFieldValue(m, "map", new GridPartitionStateMap(partCnt * 3)/*new HashMap<>()*/);

                        return m;
                    });

                    partMap.put(finalJ, OWNING);
                });
            }

//            System.out.println(GraphLayout.parseInstance(map).toFootprint());
//            System.out.println(GraphLayout.parseInstance(maps.get(0)).toFootprint());
//            System.out.println(GraphLayout.parseInstance(top).toFootprint());

            GridTestUtils.setFieldValue(top, "node2part", map);

            when(grpCtx.topology()).thenReturn(top);

            grpContexts.add(grpCtx);
        }

        if (grpLimit < cacheGroupsCnt) {
            for (int i = grpContexts.size(); i < cacheGroupsCnt; i++)
                grpContexts.add(grpContexts.get(rndm.nextInt(grpLimit)));
        }

        when(cacheProcessor.cacheGroups()).thenReturn(grpContexts);
        when(ctx.cache()).thenReturn(cacheProcessor);
        when(ignite.context()).thenReturn(ctx);

//        System.out.println(GraphLayout.parseInstance(ignite).toFootprint());

        return ignite;
    }

    /**
     * @param nodes Nodes.
     * @param cacheGroupsCnt Cache groups count.
     * @param partCnt Partition count.
     * @param copesCnt Copes count.
     */
    private Ignite generateIgnite(List<ClusterNode> nodes, int cacheGroupsCnt, int partCnt, int copesCnt) {
        return generateIgnite(nodes, cacheGroupsCnt, partCnt, copesCnt, partCnt, true);
    }

    /**
     * @param nodes Nodes.
     * @param clustersNodesNum Clusters nodes number.
     */
    @NotNull private Map<UUID, BitSet> getGraph(List<ClusterNode> nodes, int... clustersNodesNum) {
        Map<UUID, BitSet> state = new HashMap<>();
        int nodesCnt = Arrays.stream(clustersNodesNum).sum();

        assertEquals(nodesCnt, nodes.size());

        List<Integer> list = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++)
            list.add(i);

        Collections.shuffle(list);

//        int[][] clusterOrders = new int[clustersNodesNum.length][];
        BitSet[] clusterOrders = new BitSet[clustersNodesNum.length];

        int idx = 0;
        for (int i = 0; i < clustersNodesNum.length; i++) {
            clusterOrders[i] = new BitSet(nodesCnt);
            for (int j = 0; j < clustersNodesNum[i]; j++)
                clusterOrders[i].set(list.get(idx++));
        }

        idx = 0;
        for (int i = 0; i < clustersNodesNum.length; i++) {
            for (int j = 0; j < clustersNodesNum[i]; j++)
                state.put(nodes.get(list.get(idx++)).id(), clusterOrders[i]);
        }

        return state;
    }

    /**
     * @param cnt Count.
     */
    private List<ClusterNode> generateNodes(int cnt) {
        ArrayList<ClusterNode> list = new ArrayList<>(cnt);

        HashMap<String, Object> attrs = new HashMap<>();
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long l1 = r.nextLong();
        long l2 = (r.nextLong(Long.MAX_VALUE / 0x100_000)) * 0x100_000;

        for (int i = 0; i < cnt; ) {
            UUID uuid = new UUID(l1, l2 + Integer.parseInt(String.valueOf(i), 16));

            ZookeeperClusterNode node = new ZookeeperClusterNode(uuid,
                null,
                null,
                null,
                attrs,
                new Serializable() {
                },
                0,
                false,
                null);

            node.order(++i);

            list.add(node);
        }

        return list;
    }

    /**
     * @param r R.
     */
    private void setLogger(CustomCommunicationFailureResolver r) {
        GridTestUtils.setFieldValue(r, "log", log);
    }

    /** Logger. */
    private static IgniteLogger log = new IgniteLogger() {
        @Override public IgniteLogger getLogger(Object ctgr) {
            return this;
        }

        @Override public void trace(String msg) {
            System.out.println(msg);
        }

        @Override public void debug(String msg) {
            System.out.println(msg);
        }

        @Override public void info(String msg) {
            System.out.println(msg);
        }

        @Override public void warning(String msg) {
            System.err.println(msg);
        }

        @Override public void warning(String msg, @Nullable Throwable e) {
            System.err.println(msg);
        }

        @Override public void error(String msg) {
            System.err.println(msg);
        }

        @Override public void error(String msg, @Nullable Throwable e) {
            System.err.println(msg);
        }

        @Override public boolean isTraceEnabled() {
            return true;
        }

        @Override public boolean isDebugEnabled() {
            return true;
        }

        @Override public boolean isInfoEnabled() {
            return true;
        }

        @Override public boolean isQuiet() {
            return false;
        }

        @Override public String fileName() {
            return null;
        }
    };

}

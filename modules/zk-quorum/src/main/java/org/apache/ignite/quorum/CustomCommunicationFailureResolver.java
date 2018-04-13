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

package org.apache.ignite.quorum;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationFailureContext;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Communication Failure Resolver.
 */
public class CustomCommunicationFailureResolver implements CommunicationFailureResolver {
    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** Failure context. */
    private final CommunicationResolverConfiguration cfg;

    /** Executor service. */
    private final ThreadPoolExecutor executorSvc;

    /**
     * @param cfg Config.
     */
    public CustomCommunicationFailureResolver(CommunicationResolverConfiguration cfg) {
        this.cfg = cfg;

        int threadCnt = cfg.getThreadCnt();

        if (threadCnt > 1) {
            executorSvc = new ThreadPoolExecutor(threadCnt, threadCnt, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        }
        else
            executorSvc = null;
    }

    /**
     * Default constructor.
     */
    public CustomCommunicationFailureResolver() {
        this.cfg = new CommunicationResolverConfiguration();

        this.executorSvc = null;
    }

    /** {@inheritDoc} */
    @Override public void resolve(CommunicationFailureContext ctx) {
        ClusterGraph graph = new ClusterGraph(log, ctx, cfg);

        ClusterSearch cluster = graph.findLargestIndependentCluster();

        // TODO sort by DC.
        List<ClusterNode> nodes = ctx.topologySnapshot();

        assert nodes.size() > 0;

        if (cluster != null && containsAllPartitions(cluster.nodesBitSet, nodes)) {
            assert cluster.nodeCnt <= nodes.size();

            if (cluster.nodeCnt < nodes.size()) {
                if (log.isInfoEnabled()) {
                    log.info("Communication problem resolver found independent cluster [" +
                        "clusterSrvCnt=" + cluster.srvCnt +
                        ", clusterTotalNodes=" + cluster.nodeCnt +
                        ", totalAliveNodes=" + nodes.size() + "]");
                }

                for (int i = 0; i < nodes.size(); i++) {
                    if (!cluster.nodesBitSet.get(i))
                        ctx.killNode(nodes.get(i));
                }
            }
            else
                U.warn(log, "All alive nodes are fully connected, this should be resolved automatically.");
        }
        else {
            if (log.isInfoEnabled())
                log.info("Communication problem resolver failed to find fully connected independent cluster.");

            for (ClusterNode node : nodes)
                ctx.killNode(node);
        }
    }

    /**
     * @param nodesBitSet Nodes BitSet.
     * @param nodes Nodes.
     */
    // TODO Perhaps this method should be moved to GridDhtPartitionTopologyImpl to be called under readlock.
    private boolean containsAllPartitions(BitSet nodesBitSet, List<ClusterNode> nodes) {
        long t1 = System.currentTimeMillis();

        if (!cfg.isCheckConsistency())
            return true;

        boolean containsAll = true;

        if (executorSvc == null) {
            for (CacheGroupContext ctx : ((IgniteKernal)ignite).context().cache().cacheGroups()) {
                if (skipVerification(ctx))
                    continue;

                if (!checkPartitionsConsistency(ctx, nodesBitSet, nodes)) {
                    if (cfg.isLogAllLostPartitions())
                        containsAll = false;
                    else
                        return false;
                }
            }
        }
        else
            return containsAllPartitionsParallel(nodesBitSet, nodes);

        System.out.println("containsAllPartitions time:" + (System.currentTimeMillis() - t1));
        return containsAll;
    }

    /**
     * @param nodesBitSet BitSet.
     * @param nodes Nodes.
     */
    private boolean containsAllPartitionsParallel(BitSet nodesBitSet, List<ClusterNode> nodes) {
        Collection<CacheGroupContext> contexts = ((IgniteKernal)ignite).context().cache().cacheGroups();
        ArrayList<Future<Boolean>> list = new ArrayList<>();

        for (CacheGroupContext ctx : contexts) {
            if (skipVerification(ctx))
                continue;

            list.add(executorSvc.submit(() -> checkPartitionsConsistency(ctx, nodesBitSet, nodes)));
        }

        boolean containsAll = true;

        for (Future<Boolean> fut : list) {
            try {
                if (!fut.get()) {
                    if (cfg.isLogAllLostPartitions())
                        containsAll = false;
                    else {
                        executorSvc.getQueue().clear();

                        return false;
                    }

                }
            }
            catch (InterruptedException | ExecutionException e) {
                log.error("Failed to check lose partitions", e);

                containsAll = false;
            }
        }

        return containsAll;
    }

    /**
     * @param ctx Context.
     * @param nodesBitSet Nodes bit set.
     * @param nodes Nodes.
     */
    private boolean checkPartitionsConsistency(CacheGroupContext ctx, BitSet nodesBitSet, List<ClusterNode> nodes) {
        GridDhtPartitionTopology top = ctx.topology();
        int partCnt = top.partitions();

        Set<Integer> ownedParts = new HashSet<>();
        for (int i = nodesBitSet.nextSetBit(0); i >= 0; i = nodesBitSet.nextSetBit(i + 1)) {
            GridDhtPartitionMap partMap = top.partitions(nodes.get(i).id());

            if (partMap == null)
                continue;

            for (Map.Entry<Integer, GridDhtPartitionState> e : partMap.entrySet()) {
                if (e.getValue() == OWNING)
                    ownedParts.add(e.getKey());
            }

            if (ownedParts.size() >= partCnt)
                return  true;
        }

        SB sb = new SB("Communication problem resolver found lose partitions in cache group: '");
        sb.a(ctx.cacheOrGroupName()).a("' in largest independent cluster: [");

        for (int i = 0; i < partCnt; i++) {
            if (!ownedParts.contains(i))
                sb.a(i + ", ");
        }

        sb.d(sb.length() - 2, sb.length()).a(']');

        log.warning(sb.toString());

        return false;
    }

    /**
     * @param ctx Context.
     */
    private boolean skipVerification(CacheGroupContext ctx) {
        return ctx.isLocal() || ctx.config().getCacheMode() == REPLICATED
            || cfg.getCheckConsistencyExcludes().contains(ctx.cacheOrGroupName());
    }

    /**
     * @param cluster Cluster nodes mask.
     * @param nodes Nodes.
     * @param limit IDs limit.
     * @return Cluster node IDs string.
     */
    private static String clusterNodeIds(BitSet cluster, List<ClusterNode> nodes, int limit) {
        int startIdx = 0;

        StringBuilder builder = new StringBuilder();

        int cnt = 0;

        for (; ; ) {
            int idx = cluster.nextSetBit(startIdx);

            if (idx == -1)
                break;

            startIdx = idx + 1;

            if (builder.length() == 0)
                builder.append('[');
            else
                builder.append(", ");

            builder.append(nodes.get(idx).id());

            if (cnt++ > limit)
                builder.append(", ...");
        }

        builder.append(']');

        return builder.toString();
    }

    /**
     *
     */
    private static class ClusterSearch {
        /** */
        int srvCnt;

        /** */
        int nodeCnt;

        /** */
        final BitSet nodesBitSet;

        /** Servers to kill count. */
        int srvToKillCnt;

        /** Clients to kill count. */
        int clientsToKillCnt;

        /**
         * @param nodes Total nodes.
         */
        ClusterSearch(int nodes) {
            nodesBitSet = new BitSet(nodes);
        }

        /**
         *
         */
        private int goodServersCnt() {
            return srvCnt - srvToKillCnt;
        }
    }

    /**
     *
     */
    private static class ClusterGraph {
        /** */
        private final static int WORD_IDX_SHIFT = 6;

        /** */
        private final IgniteLogger log;

        /** */
        private final int nodeCnt;

        /** */
        private final long[] visitBitSet;

        /** */
        private final CommunicationFailureContext ctx;

        /** */
        private final List<ClusterNode> nodes;

        /** Failure config. */
        private final CommunicationResolverConfiguration cfg;

        /**
         * @param log Logger.
         * @param ctx Context.
         */
        ClusterGraph(IgniteLogger log, CommunicationFailureContext ctx,
            CommunicationResolverConfiguration cfg) {
            this.log = log;
            this.ctx = ctx;
            this.cfg = cfg;

            nodes = ctx.topologySnapshot();

            nodeCnt = nodes.size();

            assert nodeCnt > 0;

            visitBitSet = initBitSet(nodeCnt);

        }

        /**
         * @param bitIdx Bit index.
         * @return Word index containing bit with given index.
         */
        private static int wordIndex(int bitIdx) {
            return bitIdx >> WORD_IDX_SHIFT;
        }

        /**
         * @param bitCnt Number of bits.
         * @return Bit set words.
         */
        static long[] initBitSet(int bitCnt) {
            return new long[wordIndex(bitCnt - 1) + 1];
        }

        /**
         * @return Cluster nodes bit set.
         */
        @Nullable ClusterSearch findLargestIndependentCluster() {
            ClusterSearch maxCluster = null;

            for (int i = 0; i < nodeCnt; i++) {
                if (getBit(visitBitSet, i))
                    continue;

                ClusterSearch cluster = new ClusterSearch(nodeCnt);

                search(cluster, i);

                if (log.isInfoEnabled()) {
                    log.info("Communication problem resolver found cluster [srvCnt=" + cluster.srvCnt +
                        ", totalNodeCnt=" + cluster.nodeCnt +
                        ", nodeIds=" + clusterNodeIds(cluster.nodesBitSet, nodes, 2000) + "]");
                }

                if (cluster.srvCnt >= cfg.getMinNodesCnt()) {
                    if (maxCluster == null || cluster.srvCnt > maxCluster.srvCnt)
                        maxCluster = cluster;
                }
            }

            if (maxCluster != null) {
                if (deleteAllNodesWithConnectionProblems(maxCluster).goodServersCnt() < cfg.getMinNodesCnt())
                    return null;
            }

            return maxCluster;
        }

        /**
         * @param cluster Cluster nodes bit set.
         * @return {@code True} if all bitSet nodes are able to connect to each other.
         */
        ClusterSearch deleteAllNodesWithConnectionProblems(ClusterSearch cluster) {
            int startIdx = 0;

            BitSet bitSet = cluster.nodesBitSet;

            for (; ; ) {
                int idx = bitSet.nextSetBit(startIdx);

                if (idx == -1)
                    break;

                ClusterNode node1 = nodes.get(idx);

                boolean client = node1.isClient();

                for (int i = 0/*TODO not idx + 1 cos of search method*/; i < nodes.size(); i++) {
                    if (!bitSet.get(i) || i == idx)
                        continue;

                    ClusterNode node2 = nodes.get(i);

                    if (!ctx.connectionAvailable(node1, node2)
                        // TODO handle one-sided disconnection (if possible).
                        && !ctx.connectionAvailable(node2, node1)) {

                        if (!node2.isClient()) {
                            if (client)
                                cluster.clientsToKillCnt++;
                            else
                                cluster.srvToKillCnt++;

                            if (firstIdxToThrow(idx, i)) {
                                cluster.nodesBitSet.set(idx, false);
                                break;
                            }
                            else
                                cluster.nodesBitSet.set(i, false);
                        }
                        else if (!client) {
                            cluster.nodesBitSet.set(i, false);

                            cluster.clientsToKillCnt++;
                        }
                    }
                }

                startIdx = idx + 1;
            }

            return cluster;
        }

        /**
         * @param idx1 Index 1.
         * @param idx2 Index 2.
         */
        private boolean firstIdxToThrow(int idx1, int idx2) {
            if (cfg.isPreferOldNodes())
                return idx1 > idx2;
            else
                return idx1 < idx2;
        }

        /**
         * @param cluster Current cluster bit set.
         * @param idx Node index.
         */
        void search(ClusterSearch cluster, int idx) {
            assert !getBit(visitBitSet, idx);

            setBit(visitBitSet, idx);

            cluster.nodesBitSet.set(idx);
            cluster.nodeCnt++;

            ClusterNode node1 = nodes.get(idx);

            if (!CU.clientNode(node1))
                cluster.srvCnt++;

            for (int i = 0; i < nodeCnt; i++) {
                if (i == idx || getBit(visitBitSet, i))
                    continue;

                ClusterNode node2 = nodes.get(i);

                boolean connected = ctx.connectionAvailable(node1, node2) ||
                    ctx.connectionAvailable(node2, node1);

                if (connected)
                    search(cluster, i);
            }
        }

        /**
         * @param words Bit set words.
         * @param bitIdx Bit index.
         */
        static void setBit(long words[], int bitIdx) {
            int wordIdx = wordIndex(bitIdx);

            words[wordIdx] |= (1L << bitIdx);
        }

        /**
         * @param words Bit set words.
         * @param bitIdx Bit index.
         * @return Bit value.
         */
        static boolean getBit(long[] words, int bitIdx) {
            int wordIdx = wordIndex(bitIdx);

            return (words[wordIdx] & (1L << bitIdx)) != 0;
        }
    }

    /**
     * @return Failure context.
     */
    public CommunicationResolverConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CustomCommunicationFailureResolver.class, this);
    }
}

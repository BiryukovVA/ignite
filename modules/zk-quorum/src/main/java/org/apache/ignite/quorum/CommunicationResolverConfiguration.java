package org.apache.ignite.quorum;

import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class CommunicationResolverConfiguration {
    /** */
    private String activatorAttr;

    /** Main dc attr. */
    private String mainDcAttr;

    /** Min nodes count. */
    private int minNodesCnt = 1;

    /** Check consistency. */
    private boolean checkConsistency = true; // FIXME for tests.

    /** Prefer old nodes. */
    private boolean preferOldNodes;

    /** Check consistency excludes. */
    private Set<String> checkConsistencyExcludes;

    /** Thread count. */
    private int threadCnt = 1;

    /** Logger all lost partitions. */
    private boolean logAllLostPartitions;

    /** Stop unresolved cluster. */
    private boolean stopUnresolvedCluster;

    /** Delete nodes with connection problems. */
    private boolean delNodesWithConnProblems;

    /**
     *
     */
    public String getActivatorAttr() {
        return activatorAttr;
    }

    /**
     * @param activatorAttr Activator attr.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setActivatorAttr(String activatorAttr) {
        this.activatorAttr = activatorAttr;

        return this;
    }

    /**
     *
     */
    public String getMainDcAttr() {
        return mainDcAttr;
    }

    /**
     * @param mainDcAttr Main dc attr.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setMainDcAttr(String mainDcAttr) {
        this.mainDcAttr = mainDcAttr;

        return this;
    }

    /**
     *
     */
    public int getMinNodesCnt() {
        return minNodesCnt;
    }

    /**
     * @param minNodesCnt Min nodes count.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setMinNodesCnt(int minNodesCnt) {
        this.minNodesCnt = minNodesCnt;

        return this;
    }

    /**
     *
     */
    public boolean isCheckConsistency() {
        return checkConsistency;
    }

    /**
     * @param checkConsistency Check consistency.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setCheckConsistency(boolean checkConsistency) {
        this.checkConsistency = checkConsistency;

        return this;
    }

    /**
     *
     */
    public boolean isPreferOldNodes() {
        return preferOldNodes;
    }

    /**
     * @param preferOldNodes Prefer old nodes.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setPreferOldNodes(boolean preferOldNodes) {
        this.preferOldNodes = preferOldNodes;

        return this;
    }

    /**
     *
     */
    public Set<String> getCheckConsistencyExcludes() {
        if (checkConsistencyExcludes == null)
            return Collections.emptySet();

        return checkConsistencyExcludes;
    }

    /**
     * @param checkConsistencyExcludes Check consistency excludes.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setCheckConsistencyExcludes(Set<String> checkConsistencyExcludes) {
        this.checkConsistencyExcludes = checkConsistencyExcludes;

        return this;
    }

    /**
     * @return Number of threads to be used.
     */
    public int getThreadCnt() {
        return threadCnt;
    }

    /**
     * If {@code threadCnt} > 1, a thread pool will be created to increasing processing speed.
     *
     * @param threadCnt Thread count.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setThreadCnt(int threadCnt) {
        this.threadCnt = threadCnt;

        return this;
    }

    /**
     *
     */
    public boolean isLogAllLostPartitions() {
        return logAllLostPartitions;
    }

    /**
     * @param logAllLostPartitions {@code False} if stop processing after finding first cache group
     * with lost partitions.
     * @return {@code this} for chaining.
     */
    public CommunicationResolverConfiguration setLogAllLostPartitions(boolean logAllLostPartitions) {
        this.logAllLostPartitions = logAllLostPartitions;

        return this;
    }

    /**
     * If {@code true} stop full cluster in case of impossibility to solve communication problem.
     *
     * @return {@code true} if stop full cluster on unsolved communication problem.
     */
    public boolean isStopUnresolvedCluster() {
        return stopUnresolvedCluster;
    }

    /**
     * If {@code true} stop full cluster in case of impossibility to solve communication problem.
     *
     * @param stopUnresolvedCluster Stop unresolved cluster.
     */
    public CommunicationResolverConfiguration setStopUnresolvedCluster(boolean stopUnresolvedCluster) {
        this.stopUnresolvedCluster = stopUnresolvedCluster;

        return this;
    }

    /**
     * If {@code true} delete nodes with connection problems not related to split brain.
     * The final cluster may not be 100% optimal, this problem is np hard.
     *
     * @return {@code True} if delete nodes with connection problems.
     */
    public boolean isDelNodesWithConnProblems() {
        return delNodesWithConnProblems;
    }

    /**
     * @param delNodesWithConnProblems Delete nodes with connection problems.
     */
    public void setDelNodesWithConnProblems(boolean delNodesWithConnProblems) {
        this.delNodesWithConnProblems = delNodesWithConnProblems;
    }
}

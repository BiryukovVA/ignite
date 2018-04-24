package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Sent by node that is stopping to coordinator across the ring,
 * then sent by coordinator across the ring.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoverySafeNodeLeftMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the node that is about to leave the topology.
     */
    public TcpDiscoverySafeNodeLeftMessage(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoverySafeNodeLeftMessage.class, this, "super", super.toString());
    }
}

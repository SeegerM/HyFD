package de.metanome.algorithms.hyfd.gpdep.util;

public enum PartitionCacheMode {
    /**
     * Do not cache partitions.
     */
    NONE,

    /**
     * Use a fresh HashMap for each RHS discovery.
     * This is especially useful for top-down traversal.
     */
    PER_RHS,

    /**
     * Use one ConcurrentHashMap shared across all RHS runs.
     * This is useful for parallel discovery and for reusing the same LHS partition
     * across different RHS attributes.
     */
    GLOBAL_CONCURRENT
}

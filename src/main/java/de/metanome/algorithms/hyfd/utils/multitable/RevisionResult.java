package de.metanome.algorithms.hyfd.utils.multitable;

import java.time.Instant;
import java.util.Set;

public class RevisionResult {
    public final String revisionId;    // e.g., from JSON or uniqueTableName suffix
    public final Instant timestamp;    // nullable if not available
    public final double weight;        // default 1.0 if you don’t use weights
    public final Set<FDKey> holds;     // FDs that HELD in this revision

    public RevisionResult(String revisionId, Instant timestamp, double weight, Set<FDKey> holds) {
        this.revisionId = revisionId;
        this.timestamp = timestamp;
        this.weight = weight;
        this.holds = holds;
    }
}


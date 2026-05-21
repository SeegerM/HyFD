package de.metanome.algorithms.hyfd.gpdep.util;

import de.metanome.algorithms.hyfd.gpdep.AbstractGpdepDiscovery;

import java.util.BitSet;

public class GpdepExactFD {
    public final BitSet lhs;
    public final int rhs;

    public GpdepExactFD(BitSet lhs, int rhs) {
        this.lhs = AbstractGpdepDiscovery.cloneBitSet(lhs);
        this.rhs = rhs;
    }
}

package de.metanome.algorithms.hyfd.structures;

import java.util.Arrays;
import java.util.BitSet;

public class BitSetKey {
    private final byte[] bytes;

    public BitSetKey(BitSet bitSet) {
        this.bytes = bitSet.toByteArray();
    }

    public BitSet getBitSet(){
        return BitSet.valueOf(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BitSetKey)) return false;
        return Arrays.equals(this.bytes, ((BitSetKey) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}

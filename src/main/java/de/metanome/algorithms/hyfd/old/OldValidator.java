package de.metanome.algorithms.hyfd.old;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.MemoryGuardian;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;
import de.metanome.algorithms.hyfd.structures.FDTreeElement;
import de.metanome.algorithms.hyfd.structures.FDTreeElementLhsPair;
import de.metanome.algorithms.hyfd.structures.IntegerPair;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import de.metanome.algorithms.hyfd.utils.Logger;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class OldValidator {

    private FDSet negCover;
    private FDTree posCover;
    private int numRecords;
    private List<PositionListIndex> plis;
    private int[][] compressedRecords;
    private float efficiencyThreshold;
    private MemoryGuardian memoryGuardian;
    private ExecutorService executor;
    private final double threshold = 0.9d;
    private int maxViolations;
    ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private int level = 0;

    public OldValidator(FDSet negCover, FDTree posCover, int maxViolations, int numRecords, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian, ObjectArrayList<ColumnIdentifier> columnIdentifiers) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.numRecords = numRecords;
        this.plis = plis;
        this.compressedRecords = compressedRecords;
        this.efficiencyThreshold = efficiencyThreshold;
        this.memoryGuardian = memoryGuardian;
        this.maxViolations = maxViolations;
        this.columnIdentifiers = columnIdentifiers;

        if (parallel) {
            int numThreads = Runtime.getRuntime().availableProcessors();
            this.executor = Executors.newFixedThreadPool(numThreads);
        }
    }

    private class FD {
        public BitSet lhs;
        public int rhs;
        public FD(BitSet lhs, int rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }
    }

    private class ValidationResult {
        public int validations = 0;
        public int intersections = 0;
        public List<de.metanome.algorithms.hyfd.old.OldValidator.FD> invalidFDs = new ArrayList<>();
        public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        public void add(de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult other) {
            this.validations += other.validations;
            this.intersections += other.intersections;
            this.invalidFDs.addAll(other.invalidFDs);
            this.comparisonSuggestions.addAll(other.comparisonSuggestions);
        }
    }

    private class ValidationTask implements Callable<de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult> {
        private FDTreeElementLhsPair elementLhsPair;
        public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }
        public ValidationTask(FDTreeElementLhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }

        public de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult call2() throws Exception {
            de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult result = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult();

            FDTreeElement element = this.elementLhsPair.getElement();
            BitSet lhs = this.elementLhsPair.getLhs();
            BitSet rhs = element.getFds();

            int rhsSize = rhs.cardinality();
            if (rhsSize == 0)
                return result;
            result.validations = result.validations + rhsSize;

            if (de.metanome.algorithms.hyfd.old.OldValidator.this.level == 0) {
                // Check if rhs is unique
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    //if (!OldValidator.this.plis.get(rhsAttr).isConstant(OldValidator.this.numRecords)) {
                    if (!de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(rhsAttr).isConstant(de.metanome.algorithms.hyfd.old.OldValidator.this.numRecords)) {
                        element.removeFd(rhsAttr);
                        result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
                    }
                    result.intersections++;
                }
            }
            else if (de.metanome.algorithms.hyfd.old.OldValidator.this.level == 1) {
                // Check if lhs from plis refines rhs
                int lhsAttribute = lhs.nextSetBit(0);
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    //if (!OldValidator.this.plis.get(lhsAttribute).refines(OldValidator.this.compressedRecords, rhsAttr)) {
                    if (!de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(lhsAttribute).refines(de.metanome.algorithms.hyfd.old.OldValidator.this.compressedRecords, rhsAttr)) {
                        element.removeFd(rhsAttr);
                        result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
                    }
                    result.intersections++;
                }
            }
            else {
                // Check if lhs from plis plus remaining inverted plis refines rhs
                int firstLhsAttr = lhs.nextSetBit(0);

                lhs.clear(firstLhsAttr);
                BitSet validRhs = de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(firstLhsAttr).refinesOld(de.metanome.algorithms.hyfd.old.OldValidator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
                lhs.set(firstLhsAttr);

                result.intersections++;

                rhs.andNot(validRhs); // Now contains all invalid FDs
                element.setFds(validRhs); // Sets the valid FDs in the FD tree

                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
                    result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
            }
            return result;
        }

        // Partial Validation Call
        public de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult call() throws Exception {
            de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult result = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult();

            FDTreeElement element = this.elementLhsPair.getElement();
            BitSet lhs = this.elementLhsPair.getLhs();
            BitSet rhs = element.getFds();

            int rhsSize = rhs.cardinality();
            if (rhsSize == 0)
                return result;
            result.validations = result.validations + rhsSize;

            if (de.metanome.algorithms.hyfd.old.OldValidator.this.level == 0) {
                // Check if rhs is unique
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    //if (!OldValidator.this.plis.get(rhsAttr).isConstant(OldValidator.this.numRecords)) {
                    AtomicInteger violations = new AtomicInteger(0);
                    if (!de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(rhsAttr).isApproximatelyConstant(de.metanome.algorithms.hyfd.old.OldValidator.this.numRecords, de.metanome.algorithms.hyfd.old.OldValidator.this.maxViolations, violations, columnIdentifiers.get(plis.get(rhsAttr).getAttribute()).toString())) {
                        element.removeFd(rhsAttr);
                        result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
                    } else {
                        element.addScore(rhsAttr, violations.get() == 0 ? 1f : 1f - ((float) violations.get() / (float) de.metanome.algorithms.hyfd.old.OldValidator.this.numRecords));
                    }
                    result.intersections++;
                }
            }
            else if (de.metanome.algorithms.hyfd.old.OldValidator.this.level == 1) {
                // Check if lhs from plis refines rhs
                int lhsAttribute = lhs.nextSetBit(0);
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    //if (!OldValidator.this.plis.get(lhsAttribute).refinesApproximately(OldValidator.this.compressedRecords, rhsAttr, 0.95d, plis.get(lhsAttribute).attribute, OldValidator.this.numRecords)) {
                    AtomicInteger violations = new AtomicInteger(0);
                    if (!de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(lhsAttribute).refinesApproximately(de.metanome.algorithms.hyfd.old.OldValidator.this.compressedRecords, rhsAttr, violations, de.metanome.algorithms.hyfd.old.OldValidator.this.maxViolations, plis.get(rhsAttr).getAttribute(), plis.get(lhsAttribute).getAttribute(), columnIdentifiers.get(plis.get(lhsAttribute).getAttribute()).toString(), columnIdentifiers.get(plis.get(rhsAttr).attribute).toString())) {
                        element.removeFd(rhsAttr);
                        result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
                    } else {
                        element.addScore(rhsAttr, violations.get() == 0 ? 1f : 1f - ((float) violations.get() / (float) de.metanome.algorithms.hyfd.old.OldValidator.this.numRecords));
                    }
                    result.intersections++;
                }
            }
            else {
                // Check if lhs from plis plus remaining inverted plis refines rhs
                int firstLhsAttr = lhs.nextSetBit(0);
                BitSet clone = (BitSet) lhs.clone();
                //lhs.clear(firstLhsAttr);
                Float[] scoreList = new Float[rhs.size()];
                BitSet validRhs = de.metanome.algorithms.hyfd.old.OldValidator.this.plis.get(firstLhsAttr).refinesApproximately(de.metanome.algorithms.hyfd.old.OldValidator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions, de.metanome.algorithms.hyfd.old.OldValidator.this.maxViolations, scoreList, de.metanome.algorithms.hyfd.old.OldValidator.this.numRecords, plis ,columnIdentifiers, clone);
                //lhs.set(firstLhsAttr);

                result.intersections++;

                rhs.andNot(validRhs); // Now contains all invalid FDs
                element.setFds(validRhs); // Sets the valid FDs in the FD tree
                element.addScores(validRhs, scoreList);
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
                    result.invalidFDs.add(new de.metanome.algorithms.hyfd.old.OldValidator.FD(lhs, rhsAttr));
            }
            return result;
        }
    }

    private de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult validationResult = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult();

        de.metanome.algorithms.hyfd.old.OldValidator.ValidationTask task = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationTask(null);
        for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
            task.setElementLhsPair(elementLhsPair);
            try {
                validationResult.add(task.call());
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    private de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult validateParallel(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult validationResult = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult();

        List<Future<de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult>> futures = new ArrayList<>();
        for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
            de.metanome.algorithms.hyfd.old.OldValidator.ValidationTask task = new de.metanome.algorithms.hyfd.old.OldValidator.ValidationTask(elementLhsPair);
            futures.add(this.executor.submit(task));
        }

        for (Future<de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult> future : futures) {
            try {
                validationResult.add(future.get());
            }
            catch (ExecutionException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
            catch (InterruptedException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    public List<IntegerPair> validatePositiveCover() throws AlgorithmExecutionException {
        int numAttributes = this.plis.size();

        Logger.getInstance().writeln("Validating FDs using plis ...");

        List<FDTreeElementLhsPair> currentLevel = null;
        if (this.level == 0) {
            currentLevel = new ArrayList<>();
            currentLevel.add(new FDTreeElementLhsPair(this.posCover, new BitSet(numAttributes)));
        }
        else {
            currentLevel = this.posCover.getLevel(this.level);
        }

        // Start the level-wise validation/discovery
        int previousNumInvalidFds = 0;
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        while (!currentLevel.isEmpty()) {
            Logger.getInstance().write("\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");

            // Validate current level
            Logger.getInstance().write("(V)");

            de.metanome.algorithms.hyfd.old.OldValidator.ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
            comparisonSuggestions.addAll(validationResult.comparisonSuggestions);

            // If the next level exceeds the predefined maximum lhs size, then we can stop here
            if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth())) {
                int numInvalidFds = validationResult.invalidFDs.size();
                int numValidFds = validationResult.validations - numInvalidFds;
                Logger.getInstance().writeln("(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
                break;
            }

            // Add all children to the next level
            Logger.getInstance().write("(C)");

            List<FDTreeElementLhsPair> nextLevel = new ArrayList<>();
            for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
                FDTreeElement element = elementLhsPair.getElement();
                BitSet lhs = elementLhsPair.getLhs();

                if (element.getChildren() == null)
                    continue;

                for (int childAttr = 0; childAttr < numAttributes; childAttr++) {
                    FDTreeElement child = element.getChildren()[childAttr];

                    if (child != null) {
                        BitSet childLhs = (BitSet) lhs.clone();
                        childLhs.set(childAttr);
                        nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
                    }
                }
            }

            // Generate new FDs from the invalid FDs and add them to the next level as well
            Logger.getInstance().write("(G); ");

            int candidates = 0;
            for (de.metanome.algorithms.hyfd.old.OldValidator.FD invalidFD : validationResult.invalidFDs) {
                for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
                    BitSet childLhs = this.extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
                    if (childLhs != null) {
                        FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
                        if (child != null) {
                            nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
                            candidates++;

                            this.memoryGuardian.memoryChanged(1);
                            this.memoryGuardian.match(this.negCover, this.posCover, null);
                        }
                    }
                }

                if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth()))
                    break;
            }

            currentLevel = nextLevel;
            this.level++;
            int numInvalidFds = validationResult.invalidFDs.size();
            int numValidFds = validationResult.validations - numInvalidFds;
            Logger.getInstance().writeln(validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");

            // Decide if we continue validating the next level or if we go back into the sampling phase
            if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
                return comparisonSuggestions;
            //	return new ArrayList<>();
            previousNumInvalidFds = numInvalidFds;
        }

        if (this.executor != null) {
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(365, TimeUnit.DAYS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    private BitSet extendWith(BitSet lhs, int rhs, int extensionAttr) {
        if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
                (rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
                this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
                ((this.posCover.getChildren() != null) && (this.posCover.getChildren()[extensionAttr] != null) && this.posCover.getChildren()[extensionAttr].isFd(rhs)))
            // Pruning: If B->C, then AB->C cannot be minimal
            return null;

        BitSet childLhs = (BitSet) lhs.clone(); // TODO: This clone() could be avoided when done externally
        childLhs.set(extensionAttr);

        // TODO: Add more pruning here

        // if contains FD: element was a child before and has already been added to the next level
        // if contains Generalization: element cannot be minimal, because generalizations have already been validated
        if (this.posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
            return null;

        return childLhs;
    }

}

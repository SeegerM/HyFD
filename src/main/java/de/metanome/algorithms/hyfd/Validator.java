package de.metanome.algorithms.hyfd;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;
import de.metanome.algorithms.hyfd.structures.FDTreeElement;
import de.metanome.algorithms.hyfd.structures.FDTreeElementLhsPair;
import de.metanome.algorithms.hyfd.structures.IntegerPair;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import de.metanome.algorithms.hyfd.utils.Logger;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Validator {

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
	private boolean writeViolations;
	private boolean useGpu;
	private GPUValidator gpuValidator;
	private Map<BitSet, PositionListIndex> lhsPliCache;

	public Validator(FDSet negCover, FDTree posCover, int maxViolations, int numRecords, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian, ObjectArrayList<ColumnIdentifier> columnIdentifiers, boolean writeViolations, boolean gpu) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.numRecords = numRecords;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
		this.efficiencyThreshold = efficiencyThreshold;
		this.memoryGuardian = memoryGuardian;
		this.maxViolations = maxViolations;
		this.columnIdentifiers = columnIdentifiers;
		this.writeViolations = writeViolations;

		this.useGpu = gpu;
		if (this.useGpu) {
			this.gpuValidator = new GPUValidator();
			this.lhsPliCache = new HashMap<>(); // Initialize cache for GPU use
		}

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
		public List<FD> invalidFDs = new ArrayList<>();
		public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		public void add(ValidationResult other) {
			this.validations += other.validations;
			this.intersections += other.intersections;
			this.invalidFDs.addAll(other.invalidFDs);
			this.comparisonSuggestions.addAll(other.comparisonSuggestions);
		}
	}
	
	private class ValidationTask implements Callable<ValidationResult> {
		private FDTreeElementLhsPair elementLhsPair;
		public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
			this.elementLhsPair = elementLhsPair;
		}
		public ValidationTask(FDTreeElementLhsPair elementLhsPair) {
			this.elementLhsPair = elementLhsPair;
		}

		// Partial Validation Call
		public ValidationResult call() throws Exception {
			ValidationResult result = new ValidationResult();
			
			FDTreeElement element = this.elementLhsPair.getElement();
			BitSet lhs = this.elementLhsPair.getLhs();
			BitSet rhs = element.getFds();
			
			int rhsSize = rhs.cardinality();
			if (rhsSize == 0)
				return result;
			result.validations = result.validations + rhsSize;
			
			if (Validator.this.level == 0) {
				// Check if rhs is unique
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					//if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
					AtomicInteger violations = new AtomicInteger(0);
					if (!Validator.this.plis.get(rhsAttr).isApproximatelyConstant(Validator.this.numRecords, Validator.this.maxViolations, violations, columnIdentifiers.get(plis.get(rhsAttr).getAttribute()).toString(), writeViolations)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					} else {
						element.addScore(rhsAttr, violations.get() == 0 ? 1f : 1f - ((float) violations.get() / (float) Validator.this.numRecords));
					}
					result.intersections++;
				}
			}
			else if (Validator.this.level == 1) {
				// Check if lhs from plis refines rhs
				int lhsAttribute = lhs.nextSetBit(0);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					//if (!Validator.this.plis.get(lhsAttribute).refinesApproximately(Validator.this.compressedRecords, rhsAttr, 0.95d, plis.get(lhsAttribute).attribute, Validator.this.numRecords)) {
					AtomicInteger violations = new AtomicInteger(0);
					if (!Validator.this.plis.get(lhsAttribute).refinesApproximately(Validator.this.compressedRecords, rhsAttr, violations, Validator.this.maxViolations, plis.get(rhsAttr).getAttribute(), plis.get(lhsAttribute).getAttribute(), columnIdentifiers.get(plis.get(lhsAttribute).getAttribute()).toString(), columnIdentifiers.get(plis.get(rhsAttr).attribute).toString(),writeViolations)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					} else {
						element.addScore(rhsAttr, violations.get() == 0 ? 1f : 1f - ((float) violations.get() / (float) Validator.this.numRecords));
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
				BitSet validRhs = Validator.this.plis.get(firstLhsAttr).refinesApproximately(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions, Validator.this.maxViolations, scoreList, Validator.this.numRecords, plis ,columnIdentifiers, clone, writeViolations);
				//lhs.set(firstLhsAttr);
				
				result.intersections++;
				
				rhs.andNot(validRhs); // Now contains all invalid FDs
				element.setFds(validRhs); // Sets the valid FDs in the FD tree
				element.addScores(validRhs, scoreList);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
					result.invalidFDs.add(new FD(lhs, rhsAttr));
			}
			return result;
		}
	}

	private ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
		ValidationResult validationResult = new ValidationResult();
		
		ValidationTask task = new ValidationTask(null);
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
	
	private ValidationResult validateParallel(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
		ValidationResult validationResult = new ValidationResult();
		
		List<Future<ValidationResult>> futures = new ArrayList<>();
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			ValidationTask task = new ValidationTask(elementLhsPair);
			futures.add(this.executor.submit(task));
		}
		
		for (Future<ValidationResult> future : futures) {
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
			ValidationResult validationResult;
			if (this.useGpu && this.level > 0) { // GPU path is most effective for level > 0
				Logger.getInstance().write("(V-GPU)");
				validationResult = this.validateWithGpu(currentLevel);
			} else {
				Logger.getInstance().write("(V-CPU)");
				validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
			}
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
			for (FD invalidFD : validationResult.invalidFDs) {
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

	private ValidationResult validateWithGpu(List<FDTreeElementLhsPair> currentLevel) {
		ValidationResult validationResult = new ValidationResult();
		List<GPUValidator.ValidationTask> gpuTasks = new ArrayList<>();

		// We need to map GPU tasks back to the original HyFD objects
		List<Object[]> taskMetadata = new ArrayList<>();

		// 1. Create GPU validation tasks for all FDs in the current level
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			FDTreeElement element = elementLhsPair.getElement();
			BitSet lhs = elementLhsPair.getLhs();
			BitSet rhsSet = element.getFds();

			if (rhsSet.isEmpty()) {
				continue;
			}

			// Get or compute the PositionListIndex for the LHS
			//int firstLhsAttr = lhs.nextSetBit(0);
			//PositionListIndex lhsPli = plis.get(firstLhsAttr);
			PositionListIndex lhsPli = getLhsPli(lhs);
			lhsPli.lhsAttributes = lhs;
			if (lhsPli == null || lhsPli.getClusters().isEmpty()) {
				continue;
			}

			// Create a separate GPU task for each RHS attribute
			for (int rhsAttr = rhsSet.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhsSet.nextSetBit(rhsAttr + 1)) {
				gpuTasks.add(new GPUValidator.ValidationTask(lhsPli, rhsAttr));
				// Store metadata to process results later
				taskMetadata.add(new Object[]{elementLhsPair, rhsAttr});
				validationResult.validations++;
			}
		}

		if (gpuTasks.isEmpty()) {
			return validationResult;
		}

		// 2. Execute the mass validation on the GPU
		gpuValidator.performMassValidationOld2(gpuTasks, this.compressedRecords, this.maxViolations);

		// 3. Process the results from the GPU
		for (int i = 0; i < gpuTasks.size(); i++) {
			GPUValidator.ValidationTask finishedTask = gpuTasks.get(i);
			Object[] meta = taskMetadata.get(i);
			FDTreeElementLhsPair elementLhsPair = (FDTreeElementLhsPair) meta[0];
			int rhsAttr = (int) meta[1];

			if (!finishedTask.isValid()) {
				elementLhsPair.getElement().removeFd(rhsAttr);
				validationResult.invalidFDs.add(new FD(elementLhsPair.getLhs(), rhsAttr));
			} else {
				// Calculate and add score
				float score = 1f - ((float) finishedTask.getViolations() / (float) this.numRecords);
				elementLhsPair.getElement().addScore(rhsAttr, score);
			}
		}

		// In this simplified integration, comparisonSuggestions are not generated by the GPU path.
		// This could be added by modifying the GpuValidator kernel to output violating record pairs.
		validationResult.intersections = this.lhsPliCache.size(); // Approximate metric
		this.lhsPliCache.clear(); // Clear cache for the next level
		return validationResult;
	}

	private PositionListIndex getLhsPli(BitSet lhs) {
		if (lhs.cardinality() == 0) return null;
		if (lhsPliCache.containsKey(lhs)) return lhsPliCache.get(lhs);

		if (lhs.cardinality() == 1) {
			PositionListIndex pli = this.plis.get(lhs.nextSetBit(0));
			lhsPliCache.put(lhs, pli);
			return pli;
		}

		// Multi-attribute LHS: compute intersection
		List<int[]> invertedPlis = new ArrayList<>();
		for (int attr = lhs.nextSetBit(0); attr >= 0; attr = lhs.nextSetBit(attr + 1)) {
			if (attr != lhs.nextSetBit(0)) { // Skip first attribute (base PLI)
				invertedPlis.add(this.plis.get(attr).asInvertedIndex(numRecords));
			}
		}

		PositionListIndex basePli = this.plis.get(lhs.nextSetBit(0));
		PositionListIndex intersectedPli = basePli.intersect(
				invertedPlis.toArray(new int[0][])
		);

		lhsPliCache.put(lhs, intersectedPli);
		return intersectedPli;
	}

	private PositionListIndex getLhsPliOld(BitSet lhs) {
		if (lhs.cardinality() == 0) {
			return null; // Should be handled by level 0 validation
		}

		if (lhsPliCache.containsKey(lhs)) {
			return lhsPliCache.get(lhs);
		}

		if (lhs.cardinality() == 1) {
			PositionListIndex pli = this.plis.get(lhs.nextSetBit(0));
			lhsPliCache.put(lhs, pli);
			return pli;
		}

		// Compute the intersection for multi-attribute LHS
		int firstLhsAttr = lhs.nextSetBit(0);
		PositionListIndex intersectionPli = this.plis.get(firstLhsAttr);

		int[] otherPlis = new int[lhs.cardinality() - 1];
		int count = 0;
		for (int i = lhs.nextSetBit(firstLhsAttr + 1); i >= 0; i = lhs.nextSetBit(i + 1)) {
			otherPlis[count++] = this.plis.get(i).getAttribute();
		}

		// This is a placeholder for the actual intersection logic. You need a method
		// on PositionListIndex that can intersect with multiple other PLIs.
		// Let's assume an intersect method exists that takes other PLIs.
		// PositionListIndex resultPli = intersectionPli.intersect(...)
		// For now, we return the first PLI as a simplification.
		// A full implementation requires completing this intersection logic.

		// A proper implementation would look something like this:
		// BitSet remainingLhs = (BitSet) lhs.clone();
		// remainingLhs.clear(firstLhsAttr);
		// PositionListIndex resultPli = plis.get(firstLhsAttr).intersect(plis, remainingLhs);
		// lhsPliCache.put(lhs, resultPli);
		// return resultPli;

		// Simplified for this example:
		lhsPliCache.put(lhs, intersectionPli);
		return intersectionPli;
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

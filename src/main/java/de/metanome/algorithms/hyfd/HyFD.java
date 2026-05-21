package de.metanome.algorithms.hyfd;

import java.util.*;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.RelaxedFunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.RelaxedFunctionalDependency;
import de.metanome.algorithms.hyfd.gpdep.GpdepDiscovery;
import de.metanome.algorithms.hyfd.gpdep.strategies.GpdepDiscoveryTopDown;
import de.metanome.algorithms.hyfd.gpdep.strategies.GpdepDiscoveryBottomUp;
import de.metanome.algorithms.hyfd.gpdep.util.*;
import de.metanome.algorithms.hyfd.structures.FDList;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;
import de.metanome.algorithms.hyfd.structures.IntegerPair;
import de.metanome.algorithms.hyfd.structures.PLIBuilder;
import de.metanome.algorithms.hyfd.structures.PositionListIndex;
import de.metanome.algorithms.hyfd.utils.Logger;
import de.metanome.algorithms.hyfd.utils.ValueComparator;
import de.uni_potsdam.hpi.utils.CollectionUtils;
import de.uni_potsdam.hpi.utils.FileUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class HyFD implements RelaxedFunctionalDependencyAlgorithm, BooleanParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm, RelationalInputParameterAlgorithm {

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public enum Identifier {
		INPUT_GENERATOR, THRESHOLD, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE, INPUT_ROW_LIMIT
	};

	public enum Mode {
		STANDARD,
		PARTIAL,
		GPDEP
	}

	public enum GpdepTraversal {
		BOTTOM_UP,
		TOP_DOWN
	}

	public enum GpdepRestriction {
		NONE,
		BEST_PER_RHS
	}

	private boolean useExactFdBounds = true;
	private Mode mode = Mode.STANDARD;

	private GpdepTraversal gpdepTraversal = GpdepTraversal.BOTTOM_UP;
	private GpdepRestriction gpdepRestriction = GpdepRestriction.BEST_PER_RHS;
	private PartitionCacheMode gpdepCacheMode = PartitionCacheMode.GLOBAL_CONCURRENT;

	private double minGpdep = 0.0d;


	private RelationalInputGenerator inputGenerator = null;
	private RelaxedFunctionalDependencyResultReceiver resultReceiver = null;

	private ValueComparator valueComparator;
	private final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	
	private boolean validateParallel = true;	// The validation is the most costly part in HyFD and it can easily be parallelized
	private int maxLhsSize = -1;				// The lhss can become numAttributes - 1 large, but usually we are only interested in FDs with lhs < some threshold (otherwise they would not be useful for normalization, key discovery etc.)
	private int inputRowLimit = -1;				// Maximum number of rows to be read from for analysis; values smaller or equal 0 will cause the algorithm to read all rows
	
	private float efficiencyThreshold = 0.01f;

	private String tableName;
	private List<String> attributeNames;
	private int numAttributes;

	private float threshold = 1.0f;

	@Override
	public String getAuthors() {
		return "Thorsten Papenbrock";
	}

	@Override
	public String getDescription() {
		return "Hybrid Sampling- and Lattice-Traversal-based FD discovery";
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
		configs.add(new ConfigurationRequirementRelationalInput(HyFD.Identifier.INPUT_GENERATOR.name()));
		
		ConfigurationRequirementBoolean nullEqualsNull = new ConfigurationRequirementBoolean(HyFD.Identifier.NULL_EQUALS_NULL.name());
		Boolean[] defaultNullEqualsNull = new Boolean[1];
		defaultNullEqualsNull[0] = new Boolean(true);
		nullEqualsNull.setDefaultValues(defaultNullEqualsNull);
		nullEqualsNull.setRequired(true);
		configs.add(nullEqualsNull);

		ConfigurationRequirementBoolean validateParallel = new ConfigurationRequirementBoolean(HyFD.Identifier.VALIDATE_PARALLEL.name());
		Boolean[] defaultValidateParallel = new Boolean[1];
		defaultValidateParallel[0] = new Boolean(this.validateParallel);
		validateParallel.setDefaultValues(defaultValidateParallel);
		validateParallel.setRequired(true);
		configs.add(validateParallel);

		ConfigurationRequirementBoolean enableMemoryGuardian = new ConfigurationRequirementBoolean(HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name());
		Boolean[] defaultEnableMemoryGuardian = new Boolean[1];
		defaultEnableMemoryGuardian[0] = new Boolean(this.memoryGuardian.isActive());
		enableMemoryGuardian.setDefaultValues(defaultEnableMemoryGuardian);
		enableMemoryGuardian.setRequired(true);
		configs.add(enableMemoryGuardian);
		
		ConfigurationRequirementInteger maxLhsSize = new ConfigurationRequirementInteger(HyFD.Identifier.MAX_DETERMINANT_SIZE.name());
		Integer[] defaultMaxLhsSize = new Integer[1];
		defaultMaxLhsSize[0] = new Integer(this.maxLhsSize);
		maxLhsSize.setDefaultValues(defaultMaxLhsSize);
		maxLhsSize.setRequired(false);
		configs.add(maxLhsSize);

		ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(HyFD.Identifier.INPUT_ROW_LIMIT.name());
		Integer[] defaultInputRowLimit = { Integer.valueOf(this.inputRowLimit) };
		inputRowLimit.setDefaultValues(defaultInputRowLimit);
		inputRowLimit.setRequired(false);
		configs.add(inputRowLimit);

		ConfigurationRequirementString threshold = new ConfigurationRequirementString(HyFD.Identifier.THRESHOLD.name());
		String[] defaultThreshold = new String[1];
		threshold.setDefaultValues(defaultThreshold);
		threshold.setRequired(false);
		configs.add(threshold);

		return configs;
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (Identifier.THRESHOLD.name().equals(identifier))
			this.threshold = Float.parseFloat(values[0]);
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setResultReceiver(RelaxedFunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.NULL_EQUALS_NULL.name().equals(identifier))
			this.valueComparator = new ValueComparator(values[0].booleanValue());
		else if (HyFD.Identifier.VALIDATE_PARALLEL.name().equals(identifier))
			this.validateParallel = values[0].booleanValue();
		else if (HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name().equals(identifier))
			this.memoryGuardian.setActive(values[0].booleanValue());
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.MAX_DETERMINANT_SIZE.name().equals(identifier))
			this.maxLhsSize = values[0].intValue();
		else if (HyFD.Identifier.INPUT_ROW_LIMIT.name().equals(identifier))
			if (values.length > 0)
				this.inputRowLimit = values[0].intValue();
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (HyFD.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.inputGenerator = values[0];
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	private void handleUnknownConfiguration(String identifier, String value) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
	}

	@Override
	public String toString() {
		return "HyFD:\r\n\t" + 
				"inputGenerator: " + ((this.inputGenerator != null) ? this.inputGenerator.toString() : "-") + "\r\n\t" +
				"tableName: " + this.tableName + " (" + CollectionUtils.concat(this.attributeNames, ", ") + ")\r\n\t" +
				"numAttributes: " + this.numAttributes + "\r\n\t" +
				"isNullEqualNull: " + ((this.valueComparator != null) ? String.valueOf(this.valueComparator.isNullEqualNull()) : "-") + ")\r\n\t" +
				"maxLhsSize: " + this.maxLhsSize + "\r\n" +
				"inputRowLimit: " + this.inputRowLimit + "\r\n" +
				"\r\n" +
				"Progress log: \r\n" + Logger.getInstance().read();
	}
	
	private void initialize(RelationalInput relationalInput) throws AlgorithmExecutionException {
		this.tableName = relationalInput.relationName();
		this.attributeNames = relationalInput.columnNames();
		this.numAttributes = this.attributeNames.size();
		if (this.valueComparator == null)
			this.valueComparator = new ValueComparator(true);
	}
	
	@Override
	public void execute() throws AlgorithmExecutionException {
		long startTime = System.currentTimeMillis();
		if (this.inputGenerator == null)
			throw new AlgorithmConfigurationException("No input generator set!");
		if (this.resultReceiver == null)
			throw new AlgorithmConfigurationException("No result receiver set!");

		//this.executeHyFDOld();


		switch (mode) {
			case GPDEP:
				this.executeGpdep();
				break;

			case PARTIAL:
				this.executeHyFDOld();
				break;

			case STANDARD:
			default:
				this.executeHyFDOld();
				break;
		}

		Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private void executeGpdep() throws AlgorithmExecutionException {
		Logger.getInstance().writeln("Initializing ...");

		PreparedInput prepared = prepareInputForDiscovery();

		if (prepared.numRecords == 0) {
			return;
		}

		List<GpdepExactFD> exactFds = Collections.emptyList();

		if (this.useExactFdBounds && this.gpdepTraversal == GpdepTraversal.TOP_DOWN) {
			Logger.getInstance().writeln("Discovering exact FDs as GPDEP bounds ...");
			exactFds = collectExactFdsForGpdep(
					prepared.numRecords,
					prepared.compressedRecords,
					prepared.plis,
					prepared.columnIdentifiers
			);
			Logger.getInstance().writeln("Exact FD bounds: " + exactFds.size());
		}

		GpdepDiscovery discovery = getGpdepDiscovery(prepared, exactFds);

		Logger.getInstance().writeln(
				"Discovering GPDEP FDs: traversal=" + gpdepTraversal
						+ ", restriction=" + gpdepRestriction
						+ ", cache=" + gpdepCacheMode
						+ ", minGpdep=" + minGpdep
						+ ", minPartial=" + threshold
		);

		List<ScoredFD> gpdepResults = discovery.discoverParallel();

		for (ScoredFD scoredFD : gpdepResults) {
			Logger.getInstance().writeln(scoredFD.toString());
			RelaxedFunctionalDependency result =
					scoredFD.toRelaxedFunctionalDependency(
							prepared.plis,
							prepared.columnIdentifiers
					);

			this.resultReceiver.receiveResult(result);
		}

		Logger.getInstance().writeln(
				"... done! (" + gpdepResults.size() + " GPDEP FDs)"
		);
	}

	private GpdepDiscovery getGpdepDiscovery(PreparedInput prepared, List<GpdepExactFD> exactFds) {
		ResultMode resultMode = toResultMode(this.gpdepRestriction);

		GpdepDiscovery discovery;

		if (this.gpdepTraversal == GpdepTraversal.TOP_DOWN) {
			discovery = new GpdepDiscoveryTopDown(
					this.numAttributes,
					prepared.numRecords,
					prepared.compressedRecords,
					prepared.plis,
					prepared.columnIdentifiers,
					prepared.effectiveMaxLhsSize,
					this.minGpdep,
					this.threshold,
					exactFds,
					resultMode,
					this.gpdepCacheMode
			);
		} else {
			discovery = new GpdepDiscoveryBottomUp(
					this.numAttributes,
					prepared.numRecords,
					prepared.compressedRecords,
					prepared.plis,
					prepared.columnIdentifiers,
					prepared.effectiveMaxLhsSize,
					this.minGpdep,
					this.threshold,
					exactFds,
					resultMode,
					this.gpdepCacheMode
			);
		}
		return discovery;
	}

	private ResultMode toResultMode(GpdepRestriction restriction) {
		switch (restriction) {
			case BEST_PER_RHS:
				return ResultMode.BEST_PER_RHS;

			case NONE:
			default:
				return ResultMode.ALL_NON_DOMINATED;
		}
	}

	private PreparedInput prepareInputForDiscovery()
			throws AlgorithmExecutionException {

		RelationalInput relationalInput = this.getInput();
		this.initialize(relationalInput);

		Logger.getInstance().writeln("Reading data and calculating plis ...");

		PLIBuilder pliBuilder = new PLIBuilder(this.inputRowLimit);
		List<PositionListIndex> plis = pliBuilder.getPLIs(
				relationalInput,
				this.numAttributes,
				this.valueComparator.isNullEqualNull()
		);

		this.closeInput(relationalInput);

		final int numRecords = pliBuilder.getNumLastRecords();
		pliBuilder = null;

		ObjectArrayList<ColumnIdentifier> columnIdentifiers =
				this.buildColumnIdentifiers();

		if (numRecords == 0) {
			return new PreparedInput(
					plis,
					numRecords,
					new int[0][],
					columnIdentifiers,
					0
			);
		}

		Logger.getInstance().writeln("Sorting plis by number of clusters ...");

		Collections.sort(plis, new Comparator<PositionListIndex>() {
			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {
				int numClustersInO1 =
						numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 =
						numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});

		Logger.getInstance().writeln("Inverting plis ...");
		int[][] invertedPlis = this.invertPlis(plis, numRecords);

		Logger.getInstance().writeln(
				"Extracting integer representations for the records ..."
		);

		int[][] compressedRecords = new int[numRecords][];

		for (int recordId = 0; recordId < numRecords; recordId++) {
			compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
		}

		int effectiveMaxLhsSize = this.maxLhsSize < 0
				? this.numAttributes - 1
				: Math.min(this.maxLhsSize, this.numAttributes - 1);

		return new PreparedInput(
				plis,
				numRecords,
				compressedRecords,
				columnIdentifiers,
				effectiveMaxLhsSize
		);
	}

	private List<GpdepExactFD> collectExactFdsForGpdep(
			int numRecords,
			int[][] compressedRecords,
			List<PositionListIndex> plis,
			ObjectArrayList<ColumnIdentifier> columnIdentifiers
	) throws AlgorithmExecutionException {

		int effectiveMaxLhsSize = this.maxLhsSize < 0
				? this.numAttributes - 1
				: Math.min(this.maxLhsSize, this.numAttributes - 1);

		FDSet negCover = new FDSet(this.numAttributes, effectiveMaxLhsSize);

		FDTree posCover = new FDTree(this.numAttributes, effectiveMaxLhsSize);
		posCover.addMostGeneralDependencies();

		int maxViolations = 0;

		Validator validator = new Validator(
				negCover,
				posCover,
				maxViolations,
				numRecords,
				compressedRecords,
				plis,
				this.efficiencyThreshold,
				this.validateParallel,
				this.memoryGuardian,
				columnIdentifiers
		);

		List<IntegerPair> comparisonSuggestions;

		do {
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);

		final List<GpdepExactFD> exactFds = new ArrayList<>();

		RelaxedFunctionalDependencyResultReceiver collector =
				new RelaxedFunctionalDependencyResultReceiver() {

					@Override
					public void receiveResult(RelaxedFunctionalDependency fd)
							throws CouldNotReceiveResultException,
							ColumnNameMismatchException {

						int rhs = sortedAttributeIndexOf(
								fd.getDependant(),
								plis,
								columnIdentifiers
						);

						BitSet lhs = new BitSet(numAttributes);

						for (Object obj : fd.getDeterminant().getColumnIdentifiers()) {
							ColumnIdentifier columnIdentifier = (ColumnIdentifier) obj;

							int lhsAttr = sortedAttributeIndexOf(
									columnIdentifier,
									plis,
									columnIdentifiers
							);

							lhs.set(lhsAttr);
						}

						exactFds.add(new GpdepExactFD(lhs, rhs));
					}

					@Override
					public Boolean acceptedResult(RelaxedFunctionalDependency result) {
						return true;
					}
				};

		posCover.addFunctionalDependenciesInto(
				collector,
				columnIdentifiers,
				plis
		);

		return exactFds;
	}


	private int sortedAttributeIndexOf(
			ColumnIdentifier columnIdentifier,
			List<PositionListIndex> plis,
			ObjectArrayList<ColumnIdentifier> columnIdentifiers
	) {
		for (int sortedAttr = 0; sortedAttr < plis.size(); sortedAttr++) {
			int originalAttr = plis.get(sortedAttr).getAttribute();

			if (columnIdentifiers.get(originalAttr).equals(columnIdentifier)) {
				return sortedAttr;
			}
		}

		throw new IllegalArgumentException(
				"Unknown column identifier: " + columnIdentifier
		);
	}

	private void executeHyFDOld() throws AlgorithmExecutionException {
		// Initialize
		Logger.getInstance().writeln("Initializing ...");
		RelationalInput relationalInput = this.getInput();
		this.initialize(relationalInput);
		
		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////
		
		// Calculate plis
		Logger.getInstance().writeln("Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder(this.inputRowLimit);
		List<PositionListIndex> plis = pliBuilder.getPLIs(relationalInput, this.numAttributes, this.valueComparator.isNullEqualNull());
		this.closeInput(relationalInput);

		final int numRecords = pliBuilder.getNumLastRecords();
		pliBuilder = null;
		
		if (numRecords == 0) {
			ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
			for (int attr = 0; attr < this.numAttributes; attr++)
				this.resultReceiver.receiveResult(new RelaxedFunctionalDependency(new ColumnCombination(), columnIdentifiers.get(attr), 1d));
			return;
		}
		
		// Sort plis by number of clusters: For searching in the covers and for validation, it is good to have attributes with few non-unique values and many clusters left in the prefix tree
		Logger.getInstance().writeln("Sorting plis by number of clusters ...");
		Collections.sort(plis, new Comparator<PositionListIndex>() {
			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {		
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});
		
		// Calculate inverted plis
		Logger.getInstance().writeln("Inverting plis ...");
		int[][] invertedPlis = this.invertPlis(plis, numRecords);

		// Extract the integer representations of all records from the inverted plis
		Logger.getInstance().writeln("Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
		invertedPlis = null;
		
		// Initialize the negative cover
		FDSet negCover = new FDSet(this.numAttributes, this.maxLhsSize);
		
		// Initialize the positive cover
		FDTree posCover = new FDTree(this.numAttributes, this.maxLhsSize);
		posCover.addMostGeneralDependencies();
		
		//////////////////////////
		// Build the components //
		//////////////////////////

		// TODO: implement parallel sampling
		int maxViolations = (int) (numRecords - (numRecords * threshold));
		Logger.getInstance().writeln("Max Violations: " + maxViolations);

		Sampler sampler = new Sampler(negCover, posCover, maxViolations, compressedRecords, plis, this.efficiencyThreshold, this.valueComparator, this.memoryGuardian);
		Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
		Validator validator = new Validator(negCover, posCover, maxViolations, numRecords, compressedRecords, plis, this.efficiencyThreshold, this.validateParallel, this.memoryGuardian, this.buildColumnIdentifiers());

		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
			FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
			inductor.updatePositiveCover(newNonFds);
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating FD-tree into result format ...");
		
	//	int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" + this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis, false);
		int numFDs = posCover.addFunctionalDependenciesInto(this.resultReceiver, this.buildColumnIdentifiers(), plis);
		
		Logger.getInstance().writeln("... done! (" + numFDs + " FDs)");
	}

	private RelationalInput getInput() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput relationalInput = this.inputGenerator.generateNewCopy();
		if (relationalInput == null)
			throw new InputGenerationException("Input generation failed!");
		return relationalInput;
	}
	
	private void closeInput(RelationalInput relationalInput) {
		FileUtils.close(relationalInput);
	}

	private ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.attributeNames.size());
		for (String attributeName : this.attributeNames)
			columnIdentifiers.add(new ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
	}

	private ObjectArrayList<List<String>> loadData(RelationalInput relationalInput) throws InputIterationException {
		ObjectArrayList<List<String>> records = new ObjectArrayList<List<String>>();
		while (relationalInput.hasNext())
			records.add(relationalInput.next());
		return records;
	}

	private int[][] invertPlis(List<PositionListIndex> plis, int numRecords) {
		int[][] invertedPlis = new int[plis.size()][];
		for (int attr = 0; attr < plis.size(); attr++) {
			int[] invertedPli = new int[numRecords];
			Arrays.fill(invertedPli, -1);
			
			for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
				for (int recordId : plis.get(attr).getClusters().get(clusterId))
					invertedPli[recordId] = clusterId;
			}
			invertedPlis[attr] = invertedPli;
		}
		return invertedPlis;
	}
	
	private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[this.numAttributes];
		for (int i = 0; i < this.numAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}

}

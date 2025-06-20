/*
 * Copyright 2014 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.metanome.algorithms.hyfd.structures;

import de.metanome.algorithm_integration.ColumnIdentifier;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import de.uni_potsdam.hpi.utils.CollectionUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * Position list indices (or stripped partitions) are an index structure that
 * stores the positions of equal values in a nested list. A column with the
 * values a, a, b, c, b, c transfers to the position list index ((0, 1), (2, 4),
 * (3, 5)). Clusters of size 1 are discarded. A position list index should be
 * created using the {@link PLIBuilder}.
 */
public class PositionListIndex {

	public final int attribute;
	protected final List<IntArrayList> clusters;
	protected final int numNonUniqueValues;
	
	public int getAttribute() {
		return this.attribute;
	}
	
	public List<IntArrayList> getClusters() {
		return this.clusters;
	}

	public int getNumNonUniqueValues() {
		return this.numNonUniqueValues;
	}
	
	public PositionListIndex(int attribute, List<IntArrayList> clusters) {
		this.attribute = attribute;
		this.clusters = clusters;
		this.numNonUniqueValues = this.countNonUniqueValuesIn(clusters);
	}
	
	protected int countNonUniqueValuesIn(List<IntArrayList> clusters) {
		int numNonUniqueValues = 0;
		for (IntArrayList cluster : clusters)
			numNonUniqueValues += cluster.size();
		return numNonUniqueValues;
	}

	/**
	 * Returns the number of non unary clusters.
	 *
	 * @return the number of clusters in the {@link PositionListIndex}
	 */
	public long size() {
		return this.clusters.size();
	}

	/**
	 * @return the column represented by the {@link PositionListIndex} is unique.
	 */
	public boolean isUnique() {
		return this.size() == 0;
	}
	
	public boolean isConstant(int numRecords) {
		if (numRecords <= 1)
			return true;
		if ((this.clusters.size() == 1) && (this.clusters.get(0).size() == numRecords))
			return true;
		return false;
	}

	public boolean isApproximatelyConstant(int numRecords, int maxViolations, AtomicInteger violations, String rhsName, boolean writeViolations) {
		if (numRecords <= 1) {
			return true;
		}
		int maxFrequency = 0;
		int numOverall = 0;
		IntArrayList dominantCluster = null;
		for (IntArrayList cluster : this.clusters) {
			if (cluster.size() > maxFrequency) {
				maxFrequency = cluster.size();
				dominantCluster = cluster;
				numOverall += maxFrequency;
			}
			if (numOverall-maxFrequency > maxViolations)
				return false;
		}

		int violationCount = numRecords - maxFrequency;

		if(writeViolations){
			List<String> violationDetails = new ArrayList<>();
			for (IntArrayList cluster : this.clusters) {
				if (cluster != dominantCluster) {
					for (int i = 0; i < cluster.size(); i++) {
						violationDetails.add(String.valueOf(cluster.getInt(i)));
					}
				}
			}
			storeViolationDetails(violationDetails, -1, attribute, "[]", rhsName);
		}
		violations.set(violationCount);

		return violationCount <= maxViolations;
	}

	public boolean isApproximatelyConstant(int numRecords, double threshold) {
		// Count how many records share the most common value
		// For a simple implementation, assume that each cluster represents a distinct value.
		// Sum the sizes of the clusters.
		int maxFrequency = 0;
		for (IntArrayList cluster : this.clusters) {
			if (cluster.size() > maxFrequency) {
				maxFrequency = cluster.size();
			}
		}
		return ((double) maxFrequency / numRecords) >= threshold;
	}

/*	public PositionListIndex intersect(PositionListIndex otherPLI) {
		Int2IntOpenHashMap hashedPLI = otherPLI.asHashMap();		
		Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = this.buildIntersectMap(this, hashedPLI);
		
		List<IntArrayList> clusters = new ArrayList<>();
		for (Int2ObjectMap<IntArrayList> cluster1 : intersectMap.values())
			for (IntArrayList cluster2 : cluster1.values())
				if (cluster2.size() > 1)
					clusters.add(cluster2);
		
		return new PositionListIndex(clusters);
	}
*/
	
/*	public Int2IntOpenHashMap asHashMap() {
		Int2IntOpenHashMap hashedPLI = new Int2IntOpenHashMap(this.clusters.size());		
		int clusterId = 0;
		for (IntArrayList cluster : this.clusters) {
			for (int recordId : cluster)
				hashedPLI.put(recordId, clusterId);
			
			clusterId++;
		}
		return hashedPLI;
	}
	
	protected Int2ObjectMap<Int2ObjectMap<IntArrayList>> buildIntersectMap(PositionListIndex testPLI, Int2IntOpenHashMap hashedPLI) {
		Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = new Int2ObjectOpenHashMap<>();
		for (int cluster1Id = 0; cluster1Id < testPLI.clusters.size(); cluster1Id++) {
			IntArrayList cluster = testPLI.clusters.get(cluster1Id);
			for (int recordId : cluster) {
				if (hashedPLI.containsKey(recordId)) {
					int cluster2Id = hashedPLI.get(recordId);
					
					Int2ObjectMap<IntArrayList> cluster1 = intersectMap.get(cluster1Id);
					if (cluster1 == null) {
						cluster1 = new Int2ObjectOpenHashMap<IntArrayList>();
						intersectMap.put(cluster1Id, cluster1);
					}
						
					IntArrayList cluster2 = cluster1.get(cluster2Id);
					if (cluster2 == null) {
						cluster2 = new IntArrayList();
						cluster1.put(cluster2Id, cluster2);
					}
					
					cluster2.add(recordId);
				}
			}
		}
		return intersectMap;
	}
*/	
	
	public PositionListIndex intersect(int[]... plis) {
		List<IntArrayList> clusters = new ArrayList<>();
		for (IntArrayList pivotCluster : this.clusters) {
			HashMap<IntArrayList, IntArrayList> clustersMap = new HashMap<IntArrayList, IntArrayList>(pivotCluster.size());
			
			for (int recordId : pivotCluster) {
				IntArrayList subClusters = new IntArrayList(plis.length);
				
				boolean isUnique = false;
				for (int i = 0; i < plis.length; i++) {
					if (plis[i][recordId] == -1) {
						isUnique = true;
						break;
					}	
					subClusters.add(plis[i][recordId]);
				}
				if (isUnique)
					continue;
				
				if (!clustersMap.containsKey(subClusters))
					clustersMap.put(subClusters, new IntArrayList());
				
				clustersMap.get(subClusters).add(recordId);
			}
			
			for (IntArrayList cluster : clustersMap.values())
				if (cluster.size() > 1)
					clusters.add(cluster);
		}
		return new PositionListIndex(-1, clusters);
	}
	
	public PositionListIndex intersect(int[] otherPLI) {
		Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = this.buildIntersectMap(otherPLI);
		
		List<IntArrayList> clusters = new ArrayList<>();
		for (Int2ObjectMap<IntArrayList> cluster1 : intersectMap.values())
			for (IntArrayList cluster2 : cluster1.values())
				if (cluster2.size() > 1)
					clusters.add(cluster2);
		
		return new PositionListIndex(-1, clusters);
	}

	protected Int2ObjectMap<Int2ObjectMap<IntArrayList>> buildIntersectMap(int[] hashedPLI) {
		Int2ObjectMap<Int2ObjectMap<IntArrayList>> intersectMap = new Int2ObjectOpenHashMap<>();
		for (int cluster1Id = 0; cluster1Id < this.clusters.size(); cluster1Id++) {
			IntArrayList cluster = this.clusters.get(cluster1Id);
			for (int recordId : cluster) {
				if (hashedPLI[recordId] >= 0) {
					int cluster2Id = hashedPLI[recordId];
					
					Int2ObjectMap<IntArrayList> cluster1 = intersectMap.get(cluster1Id);
					if (cluster1 == null) {
						cluster1 = new Int2ObjectOpenHashMap<IntArrayList>();
						intersectMap.put(cluster1Id, cluster1);
					}
						
					IntArrayList cluster2 = cluster1.get(cluster2Id);
					if (cluster2 == null) {
						cluster2 = new IntArrayList();
						cluster1.put(cluster2Id, cluster2);
					}
					
					cluster2.add(recordId);
				}
			}
		}
		return intersectMap;
	}

/*	public long getRawKeyError() {
		if (this.rawKeyError == -1) {
			this.rawKeyError = this.calculateRawKeyError();
		}

		return this.rawKeyError;
	}

	protected long calculateRawKeyError() {
		long sumClusterSize = 0;

		for (LongArrayList cluster : this.clusters) {
			sumClusterSize += cluster.size();
		}

		return sumClusterSize - this.clusters.size();
	}
*/
/*	public boolean refines(PositionListIndex otherPLI) {
		Int2IntOpenHashMap hashedPLI = otherPLI.asHashMap();
		
		for (IntArrayList cluster : this.clusters) {
			int otherClusterId = hashedPLI.get(cluster.getInt(0));
			
			for (int recordId : cluster)
				if ((!hashedPLI.containsKey(recordId)) || (hashedPLI.get(recordId) != otherClusterId))
					return false;
		}
		
		return true;
	}
*/

	public boolean refinesApproximately(int[][] compressedRecords, int rhsAttr,
										AtomicInteger violations, int maxViolations, int rhsAttr2, int lhsAttr, String lhs, String rhs, boolean writeViolations) {
		int totalViolations = 0;
		// List to store details about each violation.
		List<String> violationDetails = new ArrayList<>();

		if (this.clusters.isEmpty())
			return true;

		// Process each cluster to count violations and record details
		for (IntArrayList cluster : this.clusters) {
			totalViolations += countClusterInValidity2(compressedRecords, rhsAttr, cluster, lhsAttr, violationDetails,writeViolations);
			if (totalViolations > maxViolations)
				return false;
		}
		// Update the AtomicInteger with the total violation count.
		violations.set(totalViolations);

		if (writeViolations)
			storeViolationDetails(violationDetails, lhsAttr, rhsAttr2, lhs, rhs);

		return true;
	}

	/**
	 * Counts the violations in a given cluster and records the details.
	 * For each violation, a string is added to 'violationDetails' containing:
	 * - the record id,
	 * - the left-hand side (lhs) attribute value, and
	 * - the right-hand side (rhs) attribute value.
	 */
	protected int countClusterInValidity(int[][] compressedRecords, int rhsAttr,
										 IntArrayList cluster, int lhsAttr, List<String> violationDetails) {
		// Determine the reference value from the first record in the cluster.
		int reference = compressedRecords[cluster.getInt(0)][rhsAttr];
		if (reference == -1)
			return 0;  // Alternatively, this could be treated as a violation.

		int invalid = 0;
		// Iterate over each record in the cluster.
		for (int recordId : cluster) {
			if (compressedRecords[recordId][rhsAttr] != reference) {
				invalid++;
				// Format: "RecordID: {recordId}, LHS: {lhsValue}, RHS: {rhsValue}"
				String detail = "" + recordId;
				violationDetails.add(detail);
			}
		}
		return invalid;
	}

	public int countClusterInValidity2(
			int[][] compressedRecords,
			int rhsAttr,
			IntArrayList cluster,
			int lhsAttr,
			List<String> violationDetails,
			boolean writeViolations) {

			// 1) Primitive map, no boxing, default 0
		Int2IntOpenHashMap freq = new Int2IntOpenHashMap();
		freq.defaultReturnValue(0);

		int reference = -1;
		int maxCount  = 0;

		// Cache for faster loops
		final int[] clusterArr = cluster.elements();
		final int   size       = cluster.size();
		final int[][] cr       = compressedRecords;

		// --- Pass 1: count and track top value in one go ---
		for (int i = 0; i < size; i++) {
			int recId = clusterArr[i];
			int v     = cr[recId][rhsAttr];
			if (v != -1) {
				// fastutil’s addTo returns previous value, so +1 gives new count
				int cnt = freq.addTo(v, 1) + 1;
				if (cnt > maxCount) {
					maxCount  = cnt;
					reference = v;
				}
			}
		}

		// If we never saw a valid value, reference stays -1
		if (reference == -1) {
			if (writeViolations) {
				// everything except the first one is “invalid” by your old logic
				for (int i = 1; i < size; i++) {
					violationDetails.add(String.valueOf(clusterArr[i]));
				}
			}
			return size - 1;
		}

		// --- Pass 2: count all != reference and collect details if needed ---
		int invalid = 0;
		if (writeViolations) {
			for (int i = 0; i < size; i++) {
				int recId = clusterArr[i];
				if (cr[recId][rhsAttr] != reference) {
					invalid++;
					violationDetails.add(String.valueOf(recId));
				}
			}
		} else {
			for (int i = 0; i < size; i++) {
				if (cr[clusterArr[i]][rhsAttr] != reference) {
					invalid++;
				}
			}
		}

		return invalid;
	}


	public int countClusterInValidity3(int[][] compressedRecords, int rhsAttr,
										 IntArrayList cluster, int lhsAttr, List<String> violationDetails, boolean writeViolations) {
		Int2IntArrayMap frequencyMap = new Int2IntArrayMap();
		// Step 1: Count frequency of each value in the RHS attribute within the cluster
		for (int recordId : cluster) {
			int value = compressedRecords[recordId][rhsAttr];
			if (value != -1) {
				frequencyMap.put(value, frequencyMap.getOrDefault(value, 0) + 1);
			}
		}

		if (frequencyMap.isEmpty()) { //Special Case
			if (writeViolations)
				for (int i = 1; i < cluster.size(); i++)
					violationDetails.add(String.valueOf(cluster.getInt(i)));
			return cluster.size()-1; // No valid reference values
		}

		// Step 2: Find the most frequent value
		int reference = -1;
		int maxFreq = 0;
		for (Map.Entry<Integer, Integer> entry : frequencyMap.entrySet()) {
			if (entry.getValue() > maxFreq) {
				maxFreq = entry.getValue();
				reference = entry.getKey();
			}
		}

		// Step 3: Count violations
		int invalid = 0;
		for (int recordId : cluster) {
			int value = compressedRecords[recordId][rhsAttr];
			if (value != reference) {
				invalid++;
				if (writeViolations)
					violationDetails.add(String.valueOf(recordId));
			}
		}

		return invalid;
	}

	private void storeViolationDetails(List<String> violationDetails, List<Integer> lhsAttr, int rhsAttr, List<String> lhs, String rhs) {
		// Create a file in the system's temp directory.
		File tempFile = new File(System.getProperty("java.io.tmpdir"), "violations_temp.txt");
		try (FileWriter writer = new FileWriter(tempFile, true)) {  // Open file in append mode.
			String lhsAttrString = lhsAttr.stream().map(Object::toString).collect(Collectors.joining(","));
			String lhsString = String.join(",", lhs);
			writer.write("FD(" + lhsAttrString + "," + lhsString + "->" + rhsAttr + "," + rhs + "): "
					+ String.join(",", violationDetails) + System.lineSeparator());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Saves the list of violation details to a temporary file.
	 * Each detail is written on a new line.
	 */
	private void storeViolationDetails(List<String> violationDetails, int lhsAttr, int rhsAttr, String lhs, String rhs) {
		// Create a file in the system's temp directory.
		File tempFile = new File(System.getProperty("java.io.tmpdir"), "violations_temp.txt");
		try (FileWriter writer = new FileWriter(tempFile, true)) {  // Open file in append mode.
			writer.write("FD(" + lhsAttr+ "," + lhs + "->" + rhsAttr + "," + rhs + "): " + String.join(",", violationDetails) + System.lineSeparator());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean refinesApproximately(int[][] compressedRecords, int rhsAttr, AtomicInteger violations, int maxViolations) {
		int totalViolations = 0;
		if (this.clusters.isEmpty())
			return true;
		for (IntArrayList cluster : this.clusters) {
			totalViolations += countClusterInValidity(compressedRecords, rhsAttr, cluster);
			if (totalViolations > maxViolations)
				return false;
		}
		violations.set(totalViolations);  // Update the AtomicInteger
		return true;
	}


	public BitSet refinesApproximately(int[][] compressedRecords, BitSet lhs, BitSet rhs,
									   List<IntegerPair> comparisonSuggestions, int maxViolations,
									   Float[] scoreList, int numRecords, List<PositionListIndex> plis, ObjectArrayList<ColumnIdentifier> columnIdentifiers, BitSet clone, boolean writeViolations) {

		List<Integer> lhsIndex = new ArrayList<>();
		List<String> lhsIdentifiers = new ArrayList<>();
		for (int rhsAttr = clone.nextSetBit(0); rhsAttr >= 0;
			 rhsAttr = clone.nextSetBit(rhsAttr + 1)){
			lhsIndex.add(plis.get(rhsAttr).getAttribute());
			lhsIdentifiers.add(columnIdentifiers.get(plis.get(rhsAttr).getAttribute()).toString());
		}

		//if (lhsIdentifiers.contains("hospital_dirty.csv.state") && lhsIdentifiers.contains("hospital_dirty.csv.measure_code") && lhsIndex.size() == 2)
		//	System.out.println();


		int rhsSize = rhs.cardinality();
		BitSet refinedRhs = (BitSet) rhs.clone();

		// Build mappings for the rhs attributes.
		int[] rhsAttrId2Index = new int[compressedRecords[0].length];
		int[] rhsAttrIndex2Id = new int[rhsSize];
		int index = 0;
		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0;
			 rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			rhsAttrId2Index[rhsAttr] = index;
			rhsAttrIndex2Id[index] = rhsAttr;
			index++;
		}

		// Counter for violations per rhs attribute.
		int[] violationCounts = new int[compressedRecords[0].length];
		// Map to store violation details (e.g., record ids as strings) per each candidate rhs attribute.
		Map<Integer, List<String>> violationDetailsMap = new HashMap<>();

		// Iterate over clusters.
		for (IntArrayList cluster : this.clusters) {
			// Group records by their lhs “subcluster” based on the candidate lhs attributes.
			Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters =
					new Object2ObjectOpenHashMap<>(cluster.size());
			for (int recordId : cluster) {
				ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhs.cardinality(), compressedRecords[recordId]);
				if (subClusterIdentifier == null)
					continue;
				if (subClusters.containsKey(subClusterIdentifier)) {
					ClusterIdentifierWithRecord representative = subClusters.get(subClusterIdentifier);
					// For every rhs attribute in the candidate, update violation counts
					// and store the corresponding recordId as a violation detail.
					for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0;
						 rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
						int currentRhsValue = compressedRecords[recordId][rhsAttr];
						int repRhsValue = representative.get(rhsAttrId2Index[rhsAttr]);
						if (currentRhsValue != repRhsValue) { //currentRhsValue != -1 &&
							violationCounts[rhsAttr]++;
							if (writeViolations)
								violationDetailsMap
										.computeIfAbsent(rhsAttr, k -> new ArrayList<>())
										.add(String.valueOf(recordId));
							// Optionally, record a suggestion.
							comparisonSuggestions.add(new IntegerPair(recordId, representative.getRecord()));
						}
					}
				} else {
					// First record in the subcluster becomes the representative.
					int[] rhsValues = new int[rhsSize];
					for (int i = 0; i < rhsSize; i++) {
						rhsValues[i] = compressedRecords[recordId][rhsAttrIndex2Id[i]];
					}
					subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsValues, recordId));
				}
			}
		}

		// Process each candidate rhs attribute:
		// - Remove those that exceed the allowed maxViolations.
		// - Update the scoreList for the rest.
		// - Write the violation details for each remaining candidate FD.
		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0;
			 rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			if (violationCounts[rhsAttr] > maxViolations) {
				refinedRhs.clear(rhsAttr);
			} else {
				scoreList[rhsAttr] = 1f - ((float) violationCounts[rhsAttr] / (float) numRecords);
				if (writeViolations) {
					List<String> violationDetails = violationDetailsMap.get(rhsAttr);
					if (violationDetails != null) {
						// For logging, we use the composite LHS representation (via lhs.toString())
						// and the candidate rhs attribute id as its string value.
						storeViolationDetails(violationDetails, lhsIndex, plis.get(rhsAttr).getAttribute(), lhsIdentifiers, columnIdentifiers.get(plis.get(rhsAttr).getAttribute()).toString());
					}
				}
			}
		}
		return refinedRhs;
	}


	public BitSet refinesApproximately2(int[][] compressedRecords, BitSet lhs, BitSet rhs,
									   List<IntegerPair> comparisonSuggestions, int maxViolations, Float[] scoreList, int numRecords) {
		int rhsSize = rhs.cardinality();
		BitSet refinedRhs = (BitSet) rhs.clone();

		// Build mappings for the rhs attributes.
		int[] rhsAttrId2Index = new int[compressedRecords[0].length];
		int[] rhsAttrIndex2Id = new int[rhsSize];
		int index = 0;
		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			rhsAttrId2Index[rhsAttr] = index;
			rhsAttrIndex2Id[index] = rhsAttr;
			index++;
		}

		// Counter for violations per rhs attribute.
		int[] violationCounts = new int[compressedRecords[0].length];

		// Iterate over clusters.
		for (IntArrayList cluster : this.clusters) {
			// Group records by their lhs “subcluster” based on the candidate lhs attributes.
			Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters =
					new Object2ObjectOpenHashMap<>(cluster.size());
			for (int recordId : cluster) {
				ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhs.cardinality(), compressedRecords[recordId]);
				if (subClusterIdentifier == null)
					continue;
				if (subClusters.containsKey(subClusterIdentifier)) {
					ClusterIdentifierWithRecord representative = subClusters.get(subClusterIdentifier);
					// For every rhs attribute in the candidate, update violation counts.
					for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
						int currentRhsValue = compressedRecords[recordId][rhsAttr];
						int repRhsValue = representative.get(rhsAttrId2Index[rhsAttr]);
						if (currentRhsValue != repRhsValue) { //currentRhsValue != -1 &&
							violationCounts[rhsAttr]++;
							// Optionally, record a suggestion.
							comparisonSuggestions.add(new IntegerPair(recordId, representative.getRecord()));
						}
					}
				} else {
					// First record in the subcluster becomes the representative.
					int[] rhsValues = new int[rhsSize];
					for (int i = 0; i < rhsSize; i++) {
						rhsValues[i] = compressedRecords[recordId][rhsAttrIndex2Id[i]];
					}
					subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsValues, recordId));
				}
			}
		}

		// Remove any rhs attribute that exceeds the allowed maxViolations.
		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			if (violationCounts[rhsAttr] > maxViolations) {
				refinedRhs.clear(rhsAttr);
			} else {
				scoreList[rhsAttr] = 1f - ((float) violationCounts[rhsAttr] / (float) numRecords);
			}
		}
		return refinedRhs;
	}

	public boolean refinesApproximately(int[][] compressedRecords, int rhsAttr, double threshold, int attribute, int totalRecords) {
		int invalidRecords = 0;
		if (this.clusters.isEmpty())
			return true;
		for (IntArrayList cluster : this.clusters) {
			invalidRecords += countClusterInValidity(compressedRecords, rhsAttr, cluster);
		}
		int validRecords = totalRecords - invalidRecords;
		return ((double) validRecords / totalRecords) >= threshold;
	}

	protected int countClusterInValidity(int[][] compressedRecords, int rhsAttr, IntArrayList cluster) {
		// Get the reference value from the first record.
		int reference = compressedRecords[cluster.getInt(0)][rhsAttr];
		if (reference == -1)
			return 0;  // or decide to treat this as a complete violation
		int invalid = 0;
		for (int recordId : cluster) {
			if (compressedRecords[recordId][rhsAttr] != reference)
				invalid++;
		}
		return invalid;
	}

	public boolean refines(int[][] compressedRecords, int rhsAttr) {
		for (IntArrayList cluster : this.clusters)
			if (!this.probe(compressedRecords, rhsAttr, cluster))
				return false;
		return true;
	}
	
	protected boolean probe(int[][] compressedRecords, int rhsAttr, IntArrayList cluster) {
		int rhsClusterId = compressedRecords[cluster.getInt(0)][rhsAttr];
		
		// If otherClusterId < 0, then this cluster must point into more than one other clusters
		if (rhsClusterId == -1)
			return false;
		
		// Check if all records of this cluster point into the same other cluster
		for (int recordId : cluster)
			if (compressedRecords[recordId][rhsAttr] != rhsClusterId)
				return false;
		
		return true;
	}
	
	public boolean refines(int[] rhsInvertedPli) {
		for (IntArrayList cluster : this.clusters)
			if (!this.probe(rhsInvertedPli, cluster))
				return false;
		return true;
	}

	protected boolean probe(int[] rhsInvertedPli, IntArrayList cluster) {
		int rhsClusterId = rhsInvertedPli[cluster.getInt(0)];
		
		// If otherClusterId < 0, then this cluster must point into more than one other clusters
		if (rhsClusterId == -1)
			return false;
		
		// Check if all records of this cluster point into the same other cluster
		for (int recordId : cluster)
			if (rhsInvertedPli[recordId] != rhsClusterId)
				return false;
		
		return true;
	}
	
/*	public BitSet refines(int[][] invertedPlis, BitSet lhs, BitSet rhs) {
		// Returns the rhs attributes that are refined by the lhs
		BitSet refinedRhs = rhs.clone();
		
		// TODO: Check if it is technically possible that this fd holds, i.e., if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10, then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)
		
		BitSet invalidRhs = new BitSet(rhs.cardinality());
		for (IntArrayList cluster : this.clusters) {
			// Build the intersection of lhs attribute clusters
			Object2ObjectOpenHashMap<IntArrayList, IntArrayList> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
			for (int recordId : cluster) {
				IntArrayList subClusterIdentifier = this.buildClusterIdentifier(recordId, invertedPlis, lhs);
				if (subClusterIdentifier == null)
					continue;
				
				if (!subClusters.containsKey(subClusterIdentifier))
					subClusters.put(subClusterIdentifier, new IntArrayList());
				subClusters.get(subClusterIdentifier).add(recordId);
			}
			
			// Probe the rhs attributes against the lhs intersection
			for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {	// Put the rhs loop on the top level, because we usually have only one rhs attribute
				for (IntArrayList subCluster : subClusters.values()) {
					if (subCluster.size() == 1) // TODO: remove the clusters of size 1 before these loops?
						continue;
					
					if (!this.probe(invertedPlis[rhsAttr], subCluster)) {
						invalidRhs.set(rhsAttr);
						break;
					}
				}
			}
			refinedRhs.andNot(invalidRhs);
			if (refinedRhs.isEmpty())
				break;
		}
		return refinedRhs;
	}
*/
public BitSet refines(int[][] compressedRecords, BitSet lhs, BitSet rhs, List<IntegerPair> comparisonSuggestions, double threshold) {
	int rhsSize = rhs.cardinality();
	int lhsSize = lhs.cardinality();

	// Create a clone of rhs to mark which attributes are (approximately) refined.
	BitSet refinedRhs = (BitSet) rhs.clone();

	// Build mappings for rhs attributes:
	//   - rhsAttrId2Index: maps the actual attribute ID to a 0-based index.
	//   - rhsAttrIndex2Id: inverse mapping to recover the actual attribute ID.
	int[] rhsAttrId2Index = new int[compressedRecords[0].length];
	int[] rhsAttrIndex2Id = new int[rhsSize];
	int index = 0;
	for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
		rhsAttrId2Index[rhsAttr] = index;
		rhsAttrIndex2Id[index] = rhsAttr;
		index++;
	}

	// Initialize counters for each rhs attribute.
	// For each attribute (by its ID), we count the number of comparisons made and those that agree.
	int[] validCounts = new int[compressedRecords[0].length];
	int[] totalCounts = new int[compressedRecords[0].length];

	// Iterate over each cluster in this.clusters.
	for (IntArrayList cluster : this.clusters) {
		// Group records by their lhs cluster identifier.
		Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters
				= new Object2ObjectOpenHashMap<>(cluster.size());

		for (int recordId : cluster) {
			ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords[recordId]);
			if (subClusterIdentifier == null)
				continue;

			if (subClusters.containsKey(subClusterIdentifier)) {
				// For duplicate records in the same sub-cluster, compare the rhs values.
				ClusterIdentifierWithRecord representative = subClusters.get(subClusterIdentifier);

				// For each attribute in refinedRhs, update counters based on the comparison.
				for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
					totalCounts[rhsAttr]++;  // One more comparison for this attribute.

					int currentRhsValue = compressedRecords[recordId][rhsAttr];
					int repRhsValue = representative.get(rhsAttrId2Index[rhsAttr]);

					// Check if the current record’s value agrees with the representative.
					if (currentRhsValue != -1 && currentRhsValue == repRhsValue) {
						validCounts[rhsAttr]++;
					} else {
						// Record a suggestion for a potential discrepancy.
						comparisonSuggestions.add(new IntegerPair(recordId, representative.getRecord()));
					}
				}
			} else {
				// First record for this sub-cluster: store its rhs values as the representative.
				int[] rhsValues = new int[rhsSize];
				for (int i = 0; i < rhsSize; i++) {
					rhsValues[i] = compressedRecords[recordId][rhsAttrIndex2Id[i]];
				}
				subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsValues, recordId));
			}
		}
	}

	for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
		int total = totalCounts[rhsAttr];
		// If there were comparisons for this attribute, check if the valid ratio meets the threshold.
		if (total > 0 && ((double) validCounts[rhsAttr] / total) < threshold) {
			refinedRhs.clear(rhsAttr);
		}
	}

	return refinedRhs;
}


	public BitSet refinesOld(int[][] compressedRecords, BitSet lhs, BitSet rhs, List<IntegerPair> comparisonSuggestions) {
		int rhsSize = rhs.cardinality();
		int lhsSize = lhs.cardinality();
		
		// Returns the rhs attributes that are refined by the lhs
		BitSet refinedRhs = (BitSet) rhs.clone();
		
		// TODO: Check if it is technically possible that this fd holds, i.e., if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10, then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)
		
		int[] rhsAttrId2Index = new int[compressedRecords[0].length];
		int[] rhsAttrIndex2Id = new int[rhsSize];
		int index = 0;

		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			rhsAttrId2Index[rhsAttr] = index;
			rhsAttrIndex2Id[index] = rhsAttr;
			index++;
		}
		
		for (IntArrayList cluster : this.clusters) {
			Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
			for (int recordId : cluster) {
				ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords[recordId]);
				if (subClusterIdentifier == null)
					continue;
				
				if (subClusters.containsKey(subClusterIdentifier)) {
					ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);

					for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
						int rhsCluster = compressedRecords[recordId][rhsAttr];
						if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
							comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));
							
							refinedRhs.clear(rhsAttr);
							if (refinedRhs.isEmpty())
								return refinedRhs;
						}
					}
				}
				else {
					int[] rhsClusters = new int[rhsSize];
					for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++)
						rhsClusters[rhsAttr] = compressedRecords[recordId][rhsAttrIndex2Id[rhsAttr]];
					subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
				}
			}
		}
		return refinedRhs;
	}
	
	public BitSet refines(int[][] invertedPlis, BitSet lhs, BitSet rhs, int numAttributes, ArrayList<IntegerPair> comparisonSuggestions) {
		int rhsSize = rhs.cardinality();
		int lhsSize = lhs.cardinality();
		
		// Returns the rhs attributes that are refined by the lhs
		BitSet refinedRhs = (BitSet) rhs.clone();
		
		// TODO: Check if it is technically possible that this fd holds, i.e., if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10, then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)
		
		int[] rhsAttrId2Index = new int[numAttributes];
		int[] rhsAttrIndex2Id = new int[rhsSize];
		int index = 0;
		for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
			rhsAttrId2Index[rhsAttr] = index;
			rhsAttrIndex2Id[index] = rhsAttr;
			index++;
		}
		
		for (IntArrayList cluster : this.clusters) {
			Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
			for (int recordId : cluster) {
				ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(recordId, invertedPlis, lhs, lhsSize);
				if (subClusterIdentifier == null)
					continue;
				
				if (subClusters.containsKey(subClusterIdentifier)) {
					ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);
					
					for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
						int rhsCluster = invertedPlis[rhsAttr][recordId];
						if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
							comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));
							
							refinedRhs.clear(rhsAttr);
							if (refinedRhs.isEmpty())
								return refinedRhs;
						}
					}
				}
				else {
					int[] rhsClusters = new int[rhsSize];
					for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++)
						rhsClusters[rhsAttr] = invertedPlis[rhsAttrIndex2Id[rhsAttr]][recordId];
					subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
				}
			}
		}
		return refinedRhs;
	}

	public boolean refines(int[][] compressedRecords, BitSet lhs, int[] rhs) {
		for (IntArrayList cluster : this.clusters) {
			ClusterTree clusterTree = new ClusterTree();
			
			// Check if all subclusters of this cluster point into the same other clusters
			for (int recordId : cluster)
				if (!clusterTree.add(compressedRecords, lhs, recordId, rhs[recordId]))
					return false;
		}
		return true;
	}

	public boolean refines(int[][] lhsInvertedPlis, int[] rhs) {
		for (IntArrayList cluster : this.clusters) {
			Object2IntOpenHashMap<IntArrayList> clustersMap = new Object2IntOpenHashMap<>(cluster.size());
			
			// Check if all subclusters of this cluster point into the same other clusters
			for (int recordId : cluster) {
				IntArrayList additionalLhsCluster = this.buildClusterIdentifier(recordId, lhsInvertedPlis);
				if (additionalLhsCluster == null)
					continue;
				
				if (clustersMap.containsKey(additionalLhsCluster)) {
					if ((rhs[recordId] == -1) || (clustersMap.getInt(additionalLhsCluster) != rhs[recordId]))
						return false;
				}
				else {
					clustersMap.put(additionalLhsCluster, rhs[recordId]);
				}
			}
		}
		return true;
	}

	protected ClusterIdentifier buildClusterIdentifier(BitSet lhs, int lhsSize, int[] record) { 
		int[] cluster = new int[lhsSize];
		
		int index = 0;
		for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
			int clusterId = record[lhsAttr];
			
			if (clusterId < 0)
				return null;
			
			cluster[index] = clusterId;
			index++;
		}
		
		return new ClusterIdentifier(cluster);
	}

	protected ClusterIdentifier buildClusterIdentifier(int recordId, int[][] invertedPlis, BitSet lhs, int lhsSize) { 
		int[] cluster = new int[lhsSize];
		
		int index = 0;
		for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
			int clusterId = invertedPlis[lhsAttr][recordId];
			
			if (clusterId < 0)
				return null;
			
			cluster[index] = clusterId;
			index++;
		}
		
		return new ClusterIdentifier(cluster);
	}

	protected IntArrayList buildClusterIdentifier(int recordId, int[][] lhsInvertedPlis) { 
		IntArrayList clusterIdentifier = new IntArrayList(lhsInvertedPlis.length);
		
		for (int attributeIndex = 0; attributeIndex < lhsInvertedPlis.length; attributeIndex++) {
			int clusterId = lhsInvertedPlis[attributeIndex][recordId];
			
			if (clusterId < 0)
				return null;
			
			clusterIdentifier.add(clusterId);
		}
		return clusterIdentifier;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);

		Collections.sort(setCluster, new Comparator<IntSet>() {
			@Override
			public int compare(IntSet o1, IntSet o2) {
				return o1.hashCode() - o2.hashCode();
			}
		});
		result = prime * result + (setCluster.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		PositionListIndex other = (PositionListIndex) obj;
		if (this.clusters == null) {
			if (other.clusters != null) {
				return false;
			}
		} else {
			List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);
			List<IntOpenHashSet> otherSetCluster = this.convertClustersToSets(other.clusters);

			for (IntOpenHashSet cluster : setCluster) {
				if (!otherSetCluster.contains(cluster)) {
					return false;
				}
			}
			for (IntOpenHashSet cluster : otherSetCluster) {
				if (!setCluster.contains(cluster)) {
					return false;
				}
			}
		}

		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("{ ");
		for (IntArrayList cluster : this.clusters) {
			builder.append("{");
			builder.append(CollectionUtils.concat(cluster, ","));
			builder.append("} ");
		}
		builder.append("}");
		return builder.toString();
	}

	protected List<IntOpenHashSet> convertClustersToSets(List<IntArrayList> listCluster) {
		List<IntOpenHashSet> setClusters = new LinkedList<>();
		for (IntArrayList cluster : listCluster) {
			setClusters.add(new IntOpenHashSet(cluster));
		}

		return setClusters;
	}
}

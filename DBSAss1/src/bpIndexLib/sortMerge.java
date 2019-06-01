package bpIndexLib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class sortMerge {
	//	Only works for Strings! Passing in int as Strings will result in lexicographical ordering
	public static void mergeSort(String[] keys, int[] keyPageID, int[] keySlotID, int start, int length) {
		// Base Case
		if(length <= 1) {
			return;
		}
		
		int half1index = start;
		int half2index = start + length / 2;
		
		// Split array in half and recursively perform mergeSort
		mergeSort(keys, keyPageID, keySlotID, half1index, length / 2);
		mergeSort(keys, keyPageID, keySlotID, half2index, length - (length/2));
		
		// Merge two halves
		String[] mergedKeys = new String[length];
		int[] mergedKeyPageID = new int[length];
		int[] mergedKeySlotID = new int[length];
		
		for(int i = 0; i < length; i++) {
			if(keys[half1index].compareTo(keys[half2index]) <= 0) {
				// Half 1 comes first
				mergedKeys[i] = keys[half1index];
				mergedKeyPageID[i] = keyPageID[half1index];
				mergedKeySlotID[i] = keySlotID[half1index];
				half1index++;
			} else {
				// Half 2 comes first
				mergedKeys[i] = keys[half2index];
				mergedKeyPageID[i] = keyPageID[half2index];
				mergedKeySlotID[i] = keySlotID[half2index];
				half2index++;
			}
			
			// Merge rest of unfinished half
			if(half1index == start + length / 2) {
				i++;
				while(i < length) {
					mergedKeys[i] = keys[half2index];
					mergedKeyPageID[i] = keyPageID[half2index];
					mergedKeySlotID[i] = keySlotID[half2index];
					half2index++;
					i++;
				}
				break;
			} else if(half2index == start + length) {
				i++;
				while(i < length) {
					mergedKeys[i] = keys[half1index];
					mergedKeyPageID[i] = keyPageID[half1index];
					mergedKeySlotID[i] = keySlotID[half1index];
					half1index++;
					i++;
				}
				break;
			}
		}
		
		// Copy over merge results
		for(int i = 0; i < length; i++) {
			keys[start + i] = mergedKeys[i];
			keyPageID[start + i] = mergedKeyPageID[i];
			keySlotID[start + i] = mergedKeySlotID[i];
		}
	}
	
	public static void mergeFiles(int numSubFiles, int numMergeLimit) throws IOException {
		String fileNamePrefix = null;
		BufferedReader[] filesToMerge = new BufferedReader[numMergeLimit];
		String[] fileNames = new String[numMergeLimit];
		
		int totalFilesCompleted = 0;
		int numLayer = 0;
		int currentQueue = numSubFiles;
		int nextQueue = 0;
		int numMerging = -1;
		
		String[] newDataLines;
		String goal = null;
		int goalIndex = -1;
		String currentKey;
		int numValueExists;
		
		String prevFileName = null;
		String currentFileName = null;
		String openFileName = null;
		BufferedWriter mergedFile = null;
		
		// Loop through all files to be merged
		while(totalFilesCompleted != currentQueue + nextQueue) {
			while(currentQueue > 0) {
				numMerging = -1;
				for(int j = 0; j < numMergeLimit; j++) {
					StringBuilder prefixBuilder = new StringBuilder();
					for(int k = 0; k < numLayer; k++) {
						prefixBuilder.append('M');
					}
					fileNamePrefix = prefixBuilder.toString();
					try {
						openFileName = "subfile." + fileNamePrefix + (totalFilesCompleted + j);
						filesToMerge[j] = new BufferedReader(new FileReader(openFileName));
						fileNames[j] = openFileName;
					} catch (FileNotFoundException e) {
						numMerging = j;
						break;
					}
				}
				if(numMerging == -1) {
					numMerging = numMergeLimit;
				}
				
				// Merge read in files
				mergedFile = null;
				newDataLines = new String[numMerging];
				numValueExists = -1;
				while(numValueExists != 0) {
					numValueExists = 0;
					
					// Pull in new data if needed
					for(int k = 0; k < numMerging; k++) {
						if(newDataLines[k] == null) {
							newDataLines[k] = filesToMerge[k].readLine();
						}
					}
					
					// Find smallest data
					for(int k = 0; k < numMerging; k++) {
						// Check for any smallest data
						if(newDataLines[k] != null) {
							currentKey = newDataLines[k].split(",")[0];
							
							if(goal == null || currentKey.compareTo(goal) < 0) {
								goal = currentKey;
								goalIndex = k;
							}
							numValueExists++;
						}
					}
					
					if(goal != null) {
						// Write smallest data
						if(mergedFile == null) {
							currentFileName = "subfile." + fileNamePrefix + 'M' + nextQueue;
							mergedFile = new BufferedWriter(new FileWriter("subfile." + fileNamePrefix + 'M' + nextQueue));
						}
						mergedFile.write(newDataLines[goalIndex]);
						mergedFile.write("\n");
						newDataLines[goalIndex] = null;
						goal = null;
						goalIndex = -1;
					}
				}
				currentQueue -= numMerging;
				for(int k = 0; k < numMerging; k++) {
					filesToMerge[k].close();
					
					// Delete subfiles
					File temp = new File(fileNames[k]);
					temp.delete();
					fileNames[k] = null;
					totalFilesCompleted++;
				}
				
				// Add new merged file to next queue
				if(mergedFile != null) {
					prevFileName = currentFileName;
					mergedFile.close();
					nextQueue++;
				}
			}
			
			if(nextQueue > 1 && currentQueue <= 0) {
				// Finished merging current layer, reset for next layer
				currentQueue = nextQueue;
				nextQueue = 0;
				numLayer++;
				totalFilesCompleted = 0;
			} else {
				// No more files to merge
				if(mergedFile != null) {
					mergedFile.close();
				} else {
					currentFileName = prevFileName;
				}

				File oldFile = new File(currentFileName);
				File temp = new File("sortedData.txt");
				temp.delete();
				oldFile.renameTo(temp);
				break;
			}
		}
	}
}

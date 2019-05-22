import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Scanner;

import dbLoadLib.LineProcess;
import dbLoadLib.Page;
import dbLoadLib.Record;
import bpIndexLib.*;

public class bpIndex {
	private static final int EXPECTED_ARGS = 4;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	private static int subFileSize = 100; // Num data entries in subfiles in out of main memory merge sort
	private static int numMergeLimit = 100; // Num of subfiles to simultaneously merge
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		
		// Reading heap file variables
		byte[] data = null;
		RandomAccessFile heap;
		Page page;
		int numPages = 0;
		int recordsRead = 0;
		Record record;
		int pageID;
		int slotID;
		
		// B+ Index Variables
		int bpNodeSize = 0;
		int heapPageSize = 0;
		int attributeIndex = 0;
		String heapFileName = null;
		String key;
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS) {
			heapFileName = args[1];
			try {
				bpNodeSize = Integer.parseInt(args[0]);
				heapPageSize = Integer.parseInt(args[2]);
				attributeIndex = Integer.parseInt(args[3]);
			} catch(NumberFormatException e) {
				System.err.println("Invalid page size.");
				System.exit(1);
			}
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("java bpIndex <B+ Node Size> <Heap File Name> <Heap File Page Size> <Attribute No. to Index By>");
			System.err.println("B+ Node Size = Number of Keys in each node.");
			System.err.println("Attribute No. to Index By = Which attribute to create the index on. Starts at 0 for the first attribute.");
			System.exit(1);
		}
		
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("INPUTS\n");
		System.err.println("B+ Node Size: " + bpNodeSize);
		System.err.println("Heap File Name: " + heapFileName);
		System.err.println("Heap Page Size: " + heapPageSize);
		System.err.println("Attribute No. to Index By: " + attributeIndex);
		System.err.println("--------------------------------------------------------------------------------");
		
		int numSubFiles = 0;
		int recordsSubFile = 0;
		String[] subFileKeys = new String[subFileSize];
		int[] subFileKeyPageID = new int[subFileSize];
		int[] subFileKeySlotID = new int[subFileSize];
		BufferedWriter subFile = null;
		
//		beginTime = System.nanoTime();
		try {
			System.err.println("READING DB...");
			heap = new RandomAccessFile(heapFileName, "rw");
			
			subFile = new BufferedWriter(new FileWriter("subfile." + numSubFiles));
			while(true) {
				//Load in a page
				data = new byte[heapPageSize];
				heap.readFully(data);
				page = new Page(data, heapDataTypes);
				recordsRead += page.getNumRecords();
				numPages++;
				pageID = page.getPageID();
				
				// Read in data from heap file
				for(int i = 0; i < page.getNumRecords(); i++) {
					record = page.getSlotByIndex(i);
					slotID = page.getSlotID(i);
					if(heapDataTypes[attributeIndex] == LineProcess.INT_TYPE) {
						// Convert binary int to String
						key = "" + ByteBuffer.wrap(record.getData().get(attributeIndex)).getInt();
					} else {
						key = new String(record.getData().get(attributeIndex));
					}
					
					// Add file to current subfile
					subFileKeys[recordsSubFile] = key;
					subFileKeyPageID[recordsSubFile] = pageID;
					subFileKeySlotID[recordsSubFile] = slotID;
					recordsSubFile++;
					 
					// Start new subfile if needed
					if(recordsSubFile == subFileSize) {
						// Sort subfile here
						sortMerge.mergeSort(subFileKeys, subFileKeyPageID, subFileKeySlotID, 0, subFileSize);
						
						// Write out sorted subfile
						for(int j = 0; j < subFileSize; j++) {
							StringBuilder csvData = new StringBuilder();
							csvData.append(subFileKeys[j]);
							csvData.append(",");
							csvData.append(subFileKeyPageID[j]);
							csvData.append(",");
							csvData.append(subFileKeySlotID[j]);
							csvData.append("\n");
							subFile.write(csvData.toString());
						}
						
						// Reset
						subFileKeys = new String[subFileSize];
						subFileKeyPageID = new int[subFileSize];
						subFileKeySlotID = new int[subFileSize];
						recordsSubFile = 0;
						numSubFiles++;
						subFile.close();
						subFile = new BufferedWriter(new FileWriter("subfile." + numSubFiles));
					}
				}
			}
		} catch (EOFException e) {
			System.err.println("\nEnd of file.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("MERGING\n");
		System.err.println("--------------------------------------------------------------------------------");
		// Write out any remaining data
		try { 
			if(subFile != null) {
				numSubFiles++;
				subFile.close();
			}
			
			// Merge sort the files written
			String prefix = null;
			
			//--------------------------------------------------------------------------------------------------------
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
						prefix = prefixBuilder.toString();
						try {
							openFileName = "subfile." + prefix + (totalFilesCompleted + j);
							filesToMerge[j] = new BufferedReader(new FileReader(openFileName));
							fileNames[j] = openFileName;
							System.out.println("Found file " + "subfile." + prefix + (totalFilesCompleted + j));
						} catch (FileNotFoundException e) {
							numMerging = j;
							System.out.println("Could not find file " + "subfile." + prefix + (totalFilesCompleted + j));
							break;
						}
					}
					if(numMerging == -1) {
						numMerging = numMergeLimit;
					}
					
					System.out.println("Found " + numMerging + " files to merge");
					
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
								// Set initial smallest
								if(goal == null) {
									goal = currentKey;
									goalIndex = k;
								} else if(currentKey.compareTo(goal) < 0) {
									goal = currentKey;
									goalIndex = k;
								}
								numValueExists++;
							}
						}
						
						if(goal != null) {
							// Write smallest data
							if(mergedFile == null) {
								currentFileName = "subfile." + prefix + 'M' + nextQueue;
								mergedFile = new BufferedWriter(new FileWriter("subfile." + prefix + 'M' + nextQueue));
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
						System.out.println("Finished merging into: " + currentFileName);
						System.out.println("Remaining Queue: " + currentQueue);
					}
				}
				System.out.println("NextQueue = " + nextQueue + ", currentQueue = " + currentQueue);
				if(nextQueue > 1 && currentQueue <= 0) {
					System.out.println("Finished layer");
					currentQueue = nextQueue;
					nextQueue = 0;
					numLayer++;
					totalFilesCompleted = 0;
				} else {
					System.out.println("Breaking point");
					System.out.println(currentFileName);
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
			//--------------------------------------------------------------------------------------------------------
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Create the nodes of the B+ tree
		BufferedReader sortedFile = null;
		try {
			sortedFile = new BufferedReader(new FileReader("sortedData.txt"));
		} catch (Exception e) {
			
		}
		bpTree tree = new bpTree(sortedFile, bpNodeSize);
		
		// Queries
//		tree.equalityQuery("16148-01/01/2017 03:41:25 PM", true);
//		tree.equalityQuery("17854-07/11/2017 06:20:35 PM", true);
//		tree.equalityQuery("23073-12/21/2017 04:05:57 PM", true);
//		tree.rangeQuery("16148-01/01/2017 03:41:25 PM", "23073-12/21/2017 04:05:57 PM");
		beginTime = System.nanoTime();
		tree.equalityQuery("RUSSELL STREET", true);
		tree.equalityQuery("WILLIAM STREET", true);
		tree.equalityQuery("KING STREET", true);
		tree.equalityQuery("BOURKE STREET", true);
		tree.equalityQuery("ALBERT STREET", true);
		tree.equalityQuery("A'BECKETT STREET", true);
		tree.rangeQuery("ALBERT STREET", "KING STREET");
//		tree.printLeaves();
		
		endTime = System.nanoTime();
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("SUMMARY STATS\n");
		System.err.printf("Total Time Taken: %d ms\n", (endTime - beginTime) / 1000000);
		System.err.println("Total Records Read: " + recordsRead);
		System.err.println("Total Pages Read: " + numPages);
		System.err.println("--------------------------------------------------------------------------------");
	}
}

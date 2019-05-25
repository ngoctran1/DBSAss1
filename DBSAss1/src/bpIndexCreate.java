import java.io.*;
import java.nio.ByteBuffer;

import dbLoadLib.LineProcess;
import dbLoadLib.Page;
import dbLoadLib.Record;
import bpIndexLib.*;

public class bpIndexCreate {
	private static final int EXPECTED_ARGS = 4;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	private static int subFileSize = 10000; // Num data entries in subfiles in out of main memory merge sort
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
		
		beginTime = System.nanoTime();
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
						// Sort subfile
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
		endTime = System.nanoTime();
		System.err.printf("Time to Read DB and Sort Subfiles: %d ms\n", (endTime - beginTime) / 1000000);
		System.err.println("Num Pages Read: " + numPages);
		System.err.println("Num Records Read: " + recordsRead);
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("MERGING\n");
		
		beginTime = System.nanoTime();
		
		// Write out any remaining data
		try { 
			if(subFile != null) {
				numSubFiles++;
				subFile.close();
			}
			sortMerge.mergeFiles(numSubFiles, numMergeLimit);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		endTime = System.nanoTime();

		System.err.printf("Time to Merge Subfiles: %d ms\n", (endTime - beginTime) / 1000000);
		System.err.println("--------------------------------------------------------------------------------");
		
		beginTime = System.nanoTime();
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("CREATING B+ TREE\n");

		// Create the nodes of the B+ tree
		BufferedReader sortedFile = null;
		try {
			sortedFile = new BufferedReader(new FileReader("sortedData.txt"));
		} catch (Exception e) {
			
		}
		
		bpTree tree = new bpTree(sortedFile, bpNodeSize);
		tree.close();
		
		endTime = System.nanoTime();
		System.err.printf("\nTime to Create B+ Tree: %d ms\n", (endTime - beginTime) / 1000000);
		
		System.err.println("--------------------------------------------------------------------------------");
	}
}

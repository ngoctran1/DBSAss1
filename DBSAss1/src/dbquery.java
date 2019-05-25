import java.io.*;

import dbLoadLib.Page;
import dbLoadLib.Record;
import dbLoadLib.Heap;

public class dbquery {
	private static final int EXPECTED_ARGS = 5;
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		long readHeapStartTime;
		long readHeapFileTime = 0;
		
		Heap heap;
		int heapPageSize = -1;
		String heapFileName = null;
		Page page;
		
		BufferedWriter resultOutput = null;
		String resultFile = "dbquery-Result.txt";
		
		int numPages = 0;
		int recordsRead = 0;
		
		Record record;
		String attribute;

		String query = null;
		String query2 = null;
		int attributeIndex = -1;
		boolean equalitySearch = true;
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS || args.length == EXPECTED_ARGS - 1) {
			heapFileName = args[0];
			query = args[3];
			try {
				heapPageSize = Integer.parseInt(args[1]);
				attributeIndex = Integer.parseInt(args[2]);
			} catch(NumberFormatException e) {
				System.err.println("Invalid page size.");
				System.exit(1);
			}
			
			if(args.length == EXPECTED_ARGS) {
				equalitySearch = false;
				query2 = args[4];
			}
		} else {
			printCommandError();
			System.exit(1);
		}
		
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("INPUTS\n");
		System.err.println("Heap File to Open: " + heapFileName);
		System.err.println("Heap Page Size: " + heapPageSize);
		System.err.println("Attribute Index: " + attributeIndex);
		System.err.println("Search query: " + query);
		System.err.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			System.err.println("SEARCHING...\n");
			heap = new Heap(heapFileName, heapPageSize);
			resultOutput = new BufferedWriter(new FileWriter(resultFile));
			
			while(true) {
				//Load in a page
				readHeapStartTime = System.nanoTime();
				page = heap.getPage(-1);
				readHeapFileTime += System.nanoTime() - readHeapStartTime;
				
				if(page == null) {
					break;
				}
				recordsRead += page.getNumRecords();
				numPages++;
				
				// Search for query
				for(int i = 0; i < page.getNumRecords(); i++) {
					record = page.getSlotByIndex(i);
					attribute = new String(record.getData().get(attributeIndex));
					if(equalitySearch == true) {
						if(query.compareTo(attribute) == 0) {
							resultOutput.write(record.getReadableData());
							resultOutput.write("\n");
						}
					} else {
						if(attribute.compareTo(query) >= 0 && attribute.compareTo(query2) < 0) {
							resultOutput.write(record.getReadableData());
							resultOutput.write("\n");
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			resultOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		System.err.println("Results printed to file: " + resultFile);
		endTime = System.nanoTime();
		
		long totalTime = endTime - beginTime;
		System.err.println("--------------------------------------------------------------------------------");
		System.err.println("SUMMARY STATS\n");
		System.err.println("Total Records Read: " + recordsRead);
		System.err.println("Total Pages Read: " + numPages);
		System.err.printf("\nHeap File Read Time: %d ms\n", readHeapFileTime / 1000000);
		System.err.printf("Total Time Taken: %d ms\n", (totalTime) / 1000000);
		System.err.println("--------------------------------------------------------------------------------");
	}
	
	private static void printCommandError() {
		System.err.println("Invalid parameters. Please use the following:");
		System.err.println("Equality Query: java dbquery <Heap File Name> <Heap Page Size> <Attribute Index> <Query>");
		System.err.println("Range Query: java dbquery <Heap File Name> <Heap Page Size> <Attribute Index> <Query1> <Query2>");
	}
}

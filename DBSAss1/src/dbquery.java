import java.io.*;

import dbLoadLib.LineProcess;
import dbLoadLib.Page;
import dbLoadLib.Record;
import dbLoadLib.Heap;

public class dbquery {
	private static final int EXPECTED_ARGS = 5;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		
		Heap heap;
		int heapPageSize = -1;
		String heapFileName = null;
		Page page;
		
		BufferedWriter resultOutput = null;
		String resultFile = "dbquery-Result.txt";
		
		int numPages = 0;
		int recordsRead = 0;
		
		Record record;
		String recordDAName;

		String query = null;
		String query2 = null;
		int attributeIndex = -1;
		boolean equalitySearch = true;
		
		// Parse arguments
		if(args.length <= EXPECTED_ARGS) {
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
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("Equality Query: java dbquery <Heap File Name> <Heap Page Size> <Attribute Index> <Query>");
			System.err.println("Range Query: java dbquery <Heap File Name> <Heap Page Size> <Attribute Index> <Query1> <Query2>");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("Heap File to Open: " + heapFileName);
		System.out.println("Heap Page Size: " + heapPageSize);
		System.out.println("Attribute Index: " + attributeIndex);
		System.out.println("Search query: " + query);
		System.out.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			System.out.println("SEARCHING...\n");
			heap = new Heap(heapFileName, heapPageSize);
			resultOutput = new BufferedWriter(new FileWriter(resultFile));
			
			while(true) {
				//Load in a page
				page = heap.getPage();
				if(page == null) {
					break;
				}
				
				recordsRead += page.getNumRecords();
				numPages++;
				
				// Search for DA_Name query
				for(int i = 0; i < page.getNumRecords(); i++) {
					record = page.getSlotByIndex(i);
					recordDAName = new String(record.getData().get(attributeIndex));
					if(equalitySearch == true) {
						if(query.compareTo(recordDAName) == 0) {
							resultOutput.write(record.getReadableData());
							resultOutput.write("\n");
						}
					} else {
						if(recordDAName.compareTo(query) >= 0 && recordDAName.compareTo(query2) < 0) {
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
			System.exit(0);
		}
		System.out.println("Results printed to file: " + resultFile);
		endTime = System.nanoTime();
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("SUMMARY STATS\n");
		System.out.printf("Total Time Taken: %d ms\n", (endTime - beginTime) / 1000000);
		System.out.println("Total Records Read: " + recordsRead);
		System.out.println("Total Pages Read: " + numPages);
		System.out.println("--------------------------------------------------------------------------------");
	}
}

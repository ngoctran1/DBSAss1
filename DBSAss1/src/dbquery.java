import java.io.*;

import dbLoadLib.LineProcess;
import dbLoadLib.Page;
import dbLoadLib.Record;

public class dbquery {
	private static final int EXPECTED_ARGS = 2;
	private static int[] heapDataTypes = new int[]{LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.INT_TYPE, LineProcess.STR_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.INT_TYPE, LineProcess.STR_TYPE,
			  LineProcess.STR_TYPE, LineProcess.STR_TYPE, LineProcess.INT_TYPE,
			  LineProcess.STR_TYPE};
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		String query = null;
		int pageSize = 0;
		byte[] data = null;
		RandomAccessFile heap;
		Page page;
		int numPages = 0;
		int recordsRead = 0;
		Record record;
		String recordDAName;
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS) {
			query = args[0];
			try {
				pageSize = Integer.parseInt(args[1]);
			} catch(NumberFormatException e) {
				System.err.println("Invalid page size.");
				System.exit(1);
			}
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("java dbquery text pagesize");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("Search query: " + query);
		System.out.println("Page Size: " + pageSize);
		System.out.println("Heap File to Open: heap." + pageSize);
		System.out.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			System.out.println("MATCHES");
			heap = new RandomAccessFile("heap." + pageSize, "rw");
			while(true) {
				//Load in a page
				data = new byte[pageSize];
				heap.readFully(data);
				page = new Page(data, heapDataTypes);
				recordsRead += page.getNumRecords();
				numPages++;
				
				// Search for DA_Name query
				for(int i = 0; i < page.getNumRecords(); i++) {
					record = page.getSlotByIndex(i);
					recordDAName = new String(record.getData().get(0));
					if(recordDAName.equals(query)) {
						System.out.println(String.format("\nPageID: %s, SlotID: %s:", page.getPageID(), page.getSlotID(i)));
						System.out.println(record.getReadableData());
					}
				}
			}
		} catch (EOFException e) {
			System.out.println("\nEnd of file.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		endTime = System.nanoTime();
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("SUMMARY STATS\n");
		System.out.printf("Total Time Taken: %d ms\n", (endTime - beginTime) / 1000000);
		System.out.println("Total Records Read: " + recordsRead);
		System.out.println("Total Pages Read: " + numPages);
		System.out.println("--------------------------------------------------------------------------------");
	}
}

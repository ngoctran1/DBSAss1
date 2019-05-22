import java.io.*;

import dbLoadLib.LineProcess;
import dbLoadLib.Page;
import dbLoadLib.Record;

public class bpIndexQuery {
	private static final int EXPECTED_ARGS = 3;
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		String query = null;
		int nodeSize = 0;
		byte[] data = null;
		RandomAccessFile bpFile = null;
		Page page;
		int numPages = 0;
		int recordsRead = 0;
		Record record;
		String recordDAName;
		String bpFileName = null;
		
		// Parse arguments
		if(args.length == EXPECTED_ARGS) {
			query = args[2];
			bpFileName = args[0];
			try {
				nodeSize = Integer.parseInt(args[1]);
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
		System.out.println("B+ File: " + bpFileName);
		System.out.println("Node Size: " + nodeSize);
		System.out.println("Query: " + query);
		System.out.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			System.out.println("MATCHES");
//			bpFile = new RandomAccessFile(bpFileName, "rw");
			
			// Read in first (root) node
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

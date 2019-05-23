import java.io.*;

import bpIndexLib.bpTree;
public class bpIndexQuery {
	private static final int EXPECTED_ARGS = 3;
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		String query = null;
		String query2 = null;
		RandomAccessFile bpFile = null;
		int numPages = 0;
		int recordsRead = 0;
		String bpFileName = null;
		bpTree tree = null;
		boolean equalitySearch = true;
		// Parse arguments
		if(args.length <= EXPECTED_ARGS && args.length > 1) {
			bpFileName = args[0];
			query = args[1];
			if(args.length == EXPECTED_ARGS) {
				equalitySearch = false;
				query2 = args[2];
			}
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("Equality Query: java dbquery <B+ Tree Index File> <Query>");
			System.err.println("Range Query: java dbquery <B+ Tree Index File> <Query1> <Query2>");
			System.err.println("Ensurre queries are enclosed in quotation marks if they include spaces.");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("B+ File: " + bpFileName);
		System.out.println("Query: " + query);
		System.out.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			bpFile = new RandomAccessFile(bpFileName, "rw");
			tree = new bpTree(bpFile);
			
			System.out.println("MATCHES");
			if(equalitySearch) {
				tree.equalityQuery(query, true);
			} else {
				System.out.println("Number of Matches: " + tree.rangeQuery(query, query2));
			}
//			tree.printLeaves();
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

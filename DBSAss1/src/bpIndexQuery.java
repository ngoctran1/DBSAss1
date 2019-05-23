import java.io.*;

import bpIndexLib.bpTree;
import dbLoadLib.Heap;
import dbLoadLib.Page;

public class bpIndexQuery {
	private static final int EXPECTED_ARGS = 5;
	
	public static void main(String[] args) {
		long beginTime;
		long endTime;
		
		String query = null;
		String query2 = null;
		
		RandomAccessFile bpFile = null;
		String bpFileName = null;
		
		RandomAccessFile heapFile = null;
		String heapFileName = null;
		int heapPageSize = -1;
		Heap heap;
		
		BufferedWriter resultOutput = null;
		String resultFile = "bpIndexQuery-Result.txt";
		
		bpTree tree = null;
		boolean equalitySearch = true;
		
		// Parse arguments
		if(args.length <= EXPECTED_ARGS && args.length > 3) {
			heapFileName = args[0];
			bpFileName = args[2];
			query = args[3];
			
			try {
				heapPageSize = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(0);
			}
			if(args.length == EXPECTED_ARGS) {
				equalitySearch = false;
				query2 = args[4];
			}
		} else {
			System.err.println("Invalid parameters. Please use the following:");
			System.err.println("Equality Query: java dbquery <Heap File> <Heap File Page Size> <B+ Tree Index File> <Query>");
			System.err.println("Range Query: java dbquery <Heap File> <Heap File Page Size> <B+ Tree Index File> <Query1> <Query2>");
			System.err.println("Ensure queries are enclosed in quotation marks if they include spaces.");
			System.exit(1);
		}
		
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("INPUTS\n");
		System.out.println("Heap File: " + heapFileName);
		System.out.println("Heap Page Size: " + heapPageSize);
		System.out.println("B+ File: " + bpFileName);

		if(equalitySearch) {
			System.out.println("Query (Equality): " + query);
		} else {
			System.out.println("Query (Range): \"" + query + "\" - \"" + query2 + "\"");
		}
		System.out.println("--------------------------------------------------------------------------------");
		beginTime = System.nanoTime();
		try {
			bpFile = new RandomAccessFile(bpFileName, "r");
			heap = new Heap(heapFileName, heapPageSize);
			resultOutput = new BufferedWriter(new FileWriter(resultFile));
			tree = new bpTree(bpFile);
			
			System.out.println("--------------------------------------------------------------------------------");
			System.out.println("SEARCHING...\n");
			if(equalitySearch) {
				tree.equalityQuery(query, true, heap, resultOutput);
			} else {
				tree.rangeQuery(query, query2, heap, resultOutput);
			}
		} catch (Exception e) {
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
	}
}
